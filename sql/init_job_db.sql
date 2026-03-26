CREATE TABLE IF NOT EXISTS jobs_clean (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    title TEXT NOT NULL,
    company TEXT,
    location TEXT,
    remote_type VARCHAR(20),
    publication_date TIMESTAMP,
    job_url TEXT UNIQUE,
    description TEXT,
    skills TEXT[],
    category VARCHAR(50),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);