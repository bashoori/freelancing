-- =========================================
-- Schema: Job Data Ingestion & Reporting
-- =========================================

-- Drop tables (for development only)
-- Comment these out in production
DROP TABLE IF EXISTS ingestion_runs;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS sources;


-- =========================================
-- Table: sources
-- =========================================
CREATE TABLE sources (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =========================================
-- Table: jobs
-- =========================================
CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,

    source_id INT NOT NULL,
    source_job_id TEXT NOT NULL,

    title TEXT NOT NULL,
    company TEXT,
    location TEXT,
    salary TEXT,
    description TEXT,
    url TEXT NOT NULL,

    posted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_source
        FOREIGN KEY (source_id)
        REFERENCES sources(id)
        ON DELETE CASCADE,

    CONSTRAINT unique_job
        UNIQUE (source_id, source_job_id)
);


-- =========================================
-- Table: ingestion_runs
-- =========================================
CREATE TABLE ingestion_runs (
    id SERIAL PRIMARY KEY,

    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT, -- success / failed
    records_loaded INT,
    errors TEXT
);


-- =========================================
-- Indexes (important for performance)
-- =========================================

-- Speed up filtering by date
CREATE INDEX idx_jobs_posted_at ON jobs(posted_at);

-- Speed up filtering by location
CREATE INDEX idx_jobs_location ON jobs(location);

-- Speed up filtering by company
CREATE INDEX idx_jobs_company ON jobs(company);


-- =========================================
-- Seed data (so pipeline works immediately)
-- =========================================

INSERT INTO sources (id, name, url)
VALUES (1, 'RemoteOK', 'https://remoteok.com')
ON CONFLICT (id) DO NOTHING;


-- =========================================
-- Optional: Reporting View
-- =========================================

CREATE OR REPLACE VIEW recent_jobs AS
SELECT
    j.id,
    j.title,
    j.company,
    j.location,
    j.salary,
    j.url,
    j.posted_at,
    s.name AS source
FROM jobs j
JOIN sources s ON j.source_id = s.id
WHERE j.posted_at >= NOW() - INTERVAL '7 days';