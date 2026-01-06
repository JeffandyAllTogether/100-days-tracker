-- ============================================
-- HARVEST TIME TRACKING DATABASE SCHEMA
-- Updated for Cloud Deployment (Supabase)
-- ============================================

-- ============================================
-- MAIN TIME TRACKING TABLE
-- ============================================

DROP TABLE IF EXISTS harvest_time_tracking CASCADE;

CREATE TABLE harvest_time_tracking (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    week_start DATE NOT NULL,
    week_number BIGINT,              -- Changed from INTEGER to BIGINT
    year BIGINT,                     -- Changed from INTEGER to BIGINT
    month VARCHAR(7),
    day_of_week VARCHAR(10),
    task VARCHAR(255),
    notes TEXT,
    hours DECIMAL(5,2) NOT NULL CHECK (hours >= 0),
    time_type VARCHAR(20),           -- 'CT', 'VT', or 'Other'
    ct_category VARCHAR(50),         -- 'SQL', 'Python', 'Data_Engineering'
    vt_category VARCHAR(50),         -- 'Filming', 'Scripting', 'Editing'
    ct_type VARCHAR(50),             -- 'Deep_Dive', 'Shipping', 'Uncategorized', 'Pre_Classification'
    day_number BIGINT,               -- Changed from INTEGER to BIGINT
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Prevent duplicate entries
    CONSTRAINT unique_entry UNIQUE(date, task, notes)
);

-- Create indexes for faster queries
CREATE INDEX idx_date ON harvest_time_tracking(date);
CREATE INDEX idx_week_start ON harvest_time_tracking(week_start);
CREATE INDEX idx_time_type ON harvest_time_tracking(time_type);
CREATE INDEX idx_ct_category ON harvest_time_tracking(ct_category) WHERE ct_category IS NOT NULL;
CREATE INDEX idx_ct_type ON harvest_time_tracking(ct_type) WHERE ct_type IS NOT NULL;
CREATE INDEX idx_day_number ON harvest_time_tracking(day_number) WHERE day_number IS NOT NULL;
CREATE INDEX idx_year_week ON harvest_time_tracking(year, week_number);

-- Add comments for documentation
COMMENT ON TABLE harvest_time_tracking IS 'Main table for tracking time entries from Harvest app';
COMMENT ON COLUMN harvest_time_tracking.time_type IS 'CT = Coding Time, VT = Video Time, Other = Everything else';
COMMENT ON COLUMN harvest_time_tracking.ct_category IS 'Subject area for coding: SQL, Python, Data_Engineering';
COMMENT ON COLUMN harvest_time_tracking.ct_type IS 'Deep_Dive = Learning, Shipping = Building projects (tracked after 2025-12-31)';
COMMENT ON COLUMN harvest_time_tracking.day_number IS 'Day number for video content (not challenge day)';

-- ============================================
-- SUCCESS MESSAGE
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '✅ Database schema created successfully!';
    RAISE NOTICE 'Table created: harvest_time_tracking';
    RAISE NOTICE 'Data types: week_number, year, day_number = BIGINT';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Run your ETL script to load data';
    RAISE NOTICE '2. Verify data: SELECT COUNT(*) FROM harvest_time_tracking;';
    RAISE NOTICE '3. Check weeks: SELECT DISTINCT week_number, week_start FROM harvest_time_tracking ORDER BY week_number;';
END $$;
