from typing import TypedDict

class CleanJob(TypedDict):
    source: str
    title: str
    company: str
    location: str
    remote_type: str
    publication_date: str
    job_url: str
    description: str
    skills: list[str]
    category: str