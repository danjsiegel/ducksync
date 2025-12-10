-- DuckSync PostgreSQL Schema
-- This schema stores metadata for cache definitions and state tracking

-- Create the ducksync schema
CREATE SCHEMA IF NOT EXISTS ducksync;

-- Grant permissions (adjust user as needed)
-- GRANT ALL PRIVILEGES ON SCHEMA ducksync TO ducksync_user;

--============================================================================
-- ducksync.sources: Snowflake account connections
--============================================================================
CREATE TABLE IF NOT EXISTS ducksync.sources (
    source_name VARCHAR(255) PRIMARY KEY,
    driver_type VARCHAR(50) NOT NULL DEFAULT 'snowflake',
    secret_name VARCHAR(255) NOT NULL,  -- References DuckDB CREATE SECRET name
    passthrough_enabled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT chk_driver_type CHECK (driver_type IN ('snowflake'))
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_sources_driver_type ON ducksync.sources(driver_type);

COMMENT ON TABLE ducksync.sources IS 'Registered data sources (Snowflake accounts)';
COMMENT ON COLUMN ducksync.sources.source_name IS 'Unique identifier for the source';
COMMENT ON COLUMN ducksync.sources.driver_type IS 'Source type (currently only snowflake supported)';
COMMENT ON COLUMN ducksync.sources.secret_name IS 'Name of DuckDB secret containing credentials';
COMMENT ON COLUMN ducksync.sources.passthrough_enabled IS 'Allow passthrough queries for uncached tables';

--============================================================================
-- ducksync.caches: Query result cache definitions
--============================================================================
CREATE TABLE IF NOT EXISTS ducksync.caches (
    cache_name VARCHAR(255) PRIMARY KEY,
    source_name VARCHAR(255) NOT NULL REFERENCES ducksync.sources(source_name) ON DELETE CASCADE,
    source_query TEXT NOT NULL,
    monitor_tables TEXT[] NOT NULL,  -- Array of fully-qualified table names to monitor
    ttl_seconds INTEGER,  -- NULL = no expiration (reference data)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT chk_ttl_positive CHECK (ttl_seconds IS NULL OR ttl_seconds > 0),
    CONSTRAINT chk_monitor_tables_not_empty CHECK (array_length(monitor_tables, 1) > 0)
);

-- Index for source lookups
CREATE INDEX IF NOT EXISTS idx_caches_source_name ON ducksync.caches(source_name);

COMMENT ON TABLE ducksync.caches IS 'Cached query definitions';
COMMENT ON COLUMN ducksync.caches.cache_name IS 'Unique cache identifier (used in SQL queries)';
COMMENT ON COLUMN ducksync.caches.source_name IS 'Source to execute query against';
COMMENT ON COLUMN ducksync.caches.source_query IS 'SQL query to cache results from';
COMMENT ON COLUMN ducksync.caches.monitor_tables IS 'Tables to monitor for changes (last_altered)';
COMMENT ON COLUMN ducksync.caches.ttl_seconds IS 'Cache TTL in seconds (NULL = no expiration)';

--============================================================================
-- ducksync.state: Runtime cache state tracking
--============================================================================
CREATE TABLE IF NOT EXISTS ducksync.state (
    cache_name VARCHAR(255) PRIMARY KEY REFERENCES ducksync.caches(cache_name) ON DELETE CASCADE,
    last_refresh TIMESTAMP WITH TIME ZONE,
    source_state_hash TEXT,  -- JSON hash of {table_name: last_altered_timestamp}
    expires_at TIMESTAMP WITH TIME ZONE,  -- NULL if no TTL
    refresh_count INTEGER DEFAULT 0,
    last_row_count BIGINT,
    last_duration_ms DOUBLE PRECISION
);

-- Index for expiration checks
CREATE INDEX IF NOT EXISTS idx_state_expires_at ON ducksync.state(expires_at) WHERE expires_at IS NOT NULL;

COMMENT ON TABLE ducksync.state IS 'Runtime state tracking for caches';
COMMENT ON COLUMN ducksync.state.cache_name IS 'Reference to cache definition';
COMMENT ON COLUMN ducksync.state.last_refresh IS 'Timestamp of last successful refresh';
COMMENT ON COLUMN ducksync.state.source_state_hash IS 'Hash of source table metadata for change detection';
COMMENT ON COLUMN ducksync.state.expires_at IS 'When cache expires (calculated from TTL)';
COMMENT ON COLUMN ducksync.state.refresh_count IS 'Total number of refreshes performed';
COMMENT ON COLUMN ducksync.state.last_row_count IS 'Number of rows from last refresh';
COMMENT ON COLUMN ducksync.state.last_duration_ms IS 'Duration of last refresh in milliseconds';

--============================================================================
-- Helper Views
--============================================================================

-- View showing cache status
CREATE OR REPLACE VIEW ducksync.cache_status AS
SELECT 
    c.cache_name,
    c.source_name,
    c.ttl_seconds,
    s.last_refresh,
    s.expires_at,
    s.refresh_count,
    s.last_row_count,
    s.last_duration_ms,
    CASE 
        WHEN s.last_refresh IS NULL THEN 'NEVER_REFRESHED'
        WHEN s.expires_at IS NOT NULL AND s.expires_at < NOW() THEN 'EXPIRED'
        ELSE 'VALID'
    END as status
FROM ducksync.caches c
LEFT JOIN ducksync.state s ON c.cache_name = s.cache_name;

COMMENT ON VIEW ducksync.cache_status IS 'View showing current status of all caches';

--============================================================================
-- Utility Functions
--============================================================================

-- Function to check if a cache needs refresh
CREATE OR REPLACE FUNCTION ducksync.needs_refresh(p_cache_name VARCHAR)
RETURNS BOOLEAN AS $$
DECLARE
    v_expires_at TIMESTAMP WITH TIME ZONE;
    v_last_refresh TIMESTAMP WITH TIME ZONE;
BEGIN
    SELECT expires_at, last_refresh 
    INTO v_expires_at, v_last_refresh
    FROM ducksync.state 
    WHERE cache_name = p_cache_name;
    
    -- Never refreshed
    IF v_last_refresh IS NULL THEN
        RETURN TRUE;
    END IF;
    
    -- Has TTL and expired
    IF v_expires_at IS NOT NULL AND v_expires_at < NOW() THEN
        RETURN TRUE;
    END IF;
    
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION ducksync.needs_refresh(VARCHAR) IS 'Check if cache needs refresh based on TTL';

-- Function to update state after refresh
CREATE OR REPLACE FUNCTION ducksync.update_refresh_state(
    p_cache_name VARCHAR,
    p_state_hash TEXT,
    p_row_count BIGINT,
    p_duration_ms DOUBLE PRECISION
)
RETURNS VOID AS $$
DECLARE
    v_ttl_seconds INTEGER;
    v_expires_at TIMESTAMP WITH TIME ZONE;
BEGIN
    -- Get TTL from cache definition
    SELECT ttl_seconds INTO v_ttl_seconds
    FROM ducksync.caches
    WHERE cache_name = p_cache_name;
    
    -- Calculate new expiration time
    IF v_ttl_seconds IS NOT NULL THEN
        v_expires_at := NOW() + (v_ttl_seconds || ' seconds')::INTERVAL;
    ELSE
        v_expires_at := NULL;
    END IF;
    
    -- Update or insert state
    INSERT INTO ducksync.state (
        cache_name, last_refresh, source_state_hash, expires_at, 
        refresh_count, last_row_count, last_duration_ms
    )
    VALUES (
        p_cache_name, NOW(), p_state_hash, v_expires_at,
        1, p_row_count, p_duration_ms
    )
    ON CONFLICT (cache_name) DO UPDATE SET
        last_refresh = NOW(),
        source_state_hash = p_state_hash,
        expires_at = v_expires_at,
        refresh_count = ducksync.state.refresh_count + 1,
        last_row_count = p_row_count,
        last_duration_ms = p_duration_ms;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION ducksync.update_refresh_state(VARCHAR, TEXT, BIGINT, DOUBLE PRECISION) IS 
    'Update cache state after successful refresh';

--============================================================================
-- Permissions (uncomment and adjust as needed)
--============================================================================
-- GRANT USAGE ON SCHEMA ducksync TO ducksync_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ducksync TO ducksync_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ducksync TO ducksync_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ducksync TO ducksync_user;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA ducksync GRANT ALL ON TABLES TO ducksync_user;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA ducksync GRANT ALL ON SEQUENCES TO ducksync_user;

