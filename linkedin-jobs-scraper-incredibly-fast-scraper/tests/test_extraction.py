thonimport json
import os
import sys
from typing import List, Dict, Any

import pytest

# Make src importable when running pytest from repo root
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(CURRENT_DIR, "..", "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from extractors.linkedin_parser import LinkedInJobScraper  # type: ignore
from utils.data_exporter import DataExporter  # type: ignore

SAMPLE_HTML = """
<html>
  <body>
    <ul class="jobs-search__results-list">
      <li class="jobs-search-results__list-item">
        <div class="base-card">
          <h3 class="base-search-card__title">Software Engineer</h3>
          <a class="job-card-container__company-name">Techify Inc.</a>
          <span class="job-search-card__location">New York, NY</span>
          <time datetime="2025-10-28">3 days ago</time>
          <div class="job-card-list__insight">
            Develop and maintain backend APIs using Python and Node.js.
          </div>
          <span class="job-card-container__metadata-item--seniority">Mid-Level</span>
          <span class="job-card-container__metadata-item--employment-type">Full-Time</span>
          <ul class="job-card-container__industry-list">
            <li>Software Development</li>
            <li>Information Technology</li>
          </ul>
        </div>
      </li>
    </ul>
  </body>
</html>
"""

def test_parse_listings_extracts_basic_fields():
    scraper = LinkedInJobScraper(proxy_manager=None)
    jobs = scraper.parse_listings(SAMPLE_HTML)
    assert len(jobs) == 1

    job = jobs[0].to_dict()
    assert job["job_title"] == "Software Engineer"
    assert job["company_name"] == "Techify Inc."
    assert job["location"] == "New York, NY"
    assert "Python and Node.js" in job["job_description"]
    assert job["seniority_level"] == "Mid-Level"
    assert job["employment_type"] == "Full-Time"
    assert job["industries"] == ["Software Development", "Information Technology"]

def test_data_exporter_json_and_csv(tmp_path: pytest.TempPathFactory):
    records: List[Dict[str, Any]] = [
        {
            "job_title": "Software Engineer",
            "company_name": "Techify Inc.",
            "location": "New York, NY",
            "date_posted": "2025-10-28",
            "job_description": "Develop and maintain backend APIs using Python and Node.js.",
            "seniority_level": "Mid-Level",
            "employment_type": "Full-Time",
            "industries": ["Software Development", "Information Technology"],
        }
    ]

    json_path = tmp_path.mktemp("out") / "jobs.json"
    csv_path = tmp_path.mktemp("out") / "jobs.csv"

    DataExporter.export(records, "json", str(json_path))
    DataExporter.export(records, "csv", str(csv_path))

    assert json_path.exists()
    assert csv_path.exists()

    with open(json_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert isinstance(loaded, list)
    assert loaded[0]["job_title"] == "Software Engineer"