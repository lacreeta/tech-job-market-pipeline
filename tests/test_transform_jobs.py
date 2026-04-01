from src.transform.jobs_transformer import transform_jobs

def test_transform_jobs_returns_cleanjob_shape():
    raw_jobs = [
        {
            "title": "Data Engineer",
            "company_name": "Acme",
            "candidate_required_location": "Spain",
            "publication_date": "2026-03-31",
            "url": "https://example.com/job-1",
            "description": "Build pipelines",
            "tags": ["Python", "Airflow"],
            "category": "Software Development",
        }
    ]

    result = transform_jobs(raw_jobs)

    assert len(result) == 1

    job = result[0]

    expected_keys = {
        "source",
        "title",
        "company",
        "location",
        "remote_type",
        "publication_date",
        "job_url",
        "description",
        "skills",
        "category",
    }

    assert set(job.keys()) == expected_keys

def test_transform_jobs_returns_expected_types():
    raw_jobs = [
        {
            "title": "Data Engineer",
            "company_name": "Acme",
            "candidate_required_location": "Spain",
            "publication_date": "2026-03-31",
            "url": "https://example.com/job-1",
            "description": "Build pipelines",
            "tags": ["Python", "Airflow"],
            "category": "Software Development",
        }
    ]

    result = transform_jobs(raw_jobs)
    job = result[0]

    assert isinstance(job["source"], str)
    assert isinstance(job["title"], str)
    assert isinstance(job["company"], str)
    assert isinstance(job["location"], str)
    assert isinstance(job["remote_type"], str)
    assert isinstance(job["publication_date"], str)
    assert isinstance(job["job_url"], str)
    assert isinstance(job["description"], str)
    assert isinstance(job["skills"], list)
    assert isinstance(job["category"], str)

def test_transform_jobs_filters_jobs_without_required_fields():
    raw_jobs = [
        {
            "title": "Valid Job",
            "company_name": "Acme",
            "url": "https://example.com/job-1",
        },
        {
            "title": None,
            "company_name": "Beta",
            "url": "https://example.com/job-2",
        },
        {
            "title": "Missing Company",
            "company_name": None,
            "url": "https://example.com/job-3",
        },
        {
            "title": "Missing URL",
            "company_name": "Gamma",
            "url": None,
        },
    ]

    result = transform_jobs(raw_jobs)

    assert len(result) == 1
    assert result[0]["title"] == "Valid Job"
    assert result[0]["company"] == "Acme"
    assert result[0]["job_url"] == "https://example.com/job-1"

def test_transform_jobs_sets_empty_skills_when_tags_missing():
    raw_jobs = [
        {
            "title": "Data Engineer",
            "company_name": "Acme",
            "candidate_required_location": "Spain",
            "publication_date": "2026-03-31",
            "url": "https://example.com/job-1",
            "description": "Build pipelines",
            "category": "Software Development",
        }
    ]

    result = transform_jobs(raw_jobs)

    assert len(result) == 1
    assert result[0]["skills"] == []
    
def test_transform_jobs_copies_tags_into_skills():
    raw_jobs = [
        {
            "title": "Data Engineer",
            "company_name": "Acme",
            "candidate_required_location": "Spain",
            "publication_date": "2026-03-31",
            "url": "https://example.com/job-1",
            "description": "Build pipelines",
            "tags": ["Python", "SQL"],
            "category": "Software Development",
        }
    ]

    result = transform_jobs(raw_jobs)

    assert result[0]["skills"] == ["Python", "SQL"]