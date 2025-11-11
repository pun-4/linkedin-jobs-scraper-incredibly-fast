thonimport logging
import random
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from utils.proxy_manager import ProxyManager  # type: ignore

logger = logging.getLogger("linkedin_parser")

@dataclass
class JobPosting:
    job_title: str
    company_name: str
    location: str
    date_posted: str
    job_description: str
    seniority_level: str
    employment_type: str
    industries: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class LinkedInJobScraper:
    """
    Lightweight LinkedIn job scraper.

    Note: LinkedIn frequently changes its markup and aggressively throttles bots.
    This class is written to be structurally correct and easy to extend, but
    real-world scraping should always respect robots.txt, site terms, and law.
    """

    def __init__(
        self,
        proxy_manager: Optional[ProxyManager] = None,
        max_retries: int = 3,
        request_timeout: int = 20,
        delay_range: tuple[float, float] = (0.5, 1.5),
    ) -> None:
        self.session = requests.Session()
        self.proxy_manager = proxy_manager
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.delay_range = delay_range

    def _get_headers(self) -> Dict[str, str]:
        # Basic desktop browser header; rotate User-Agent here if desired.
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }

    def _fetch_page(self, url: str, params: Optional[Dict[str, Any]] = None) -> str:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                proxy = self.proxy_manager.get_proxy() if self.proxy_manager else None
                proxies = {"http": proxy, "https": proxy} if proxy else None
                logger.debug(
                    "Fetching URL (attempt %d/%d) %s via proxy %s",
                    attempt,
                    self.max_retries,
                    url,
                    proxy,
                )
                resp = self.session.get(
                    url,
                    params=params,
                    headers=self._get_headers(),
                    proxies=proxies,
                    timeout=self.request_timeout,
                )
                resp.raise_for_status()
                return resp.text
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Request attempt %d/%d failed for %s: %s",
                    attempt,
                    self.max_retries,
                    url,
                    exc,
                )
                time.sleep(random.uniform(*self.delay_range))
        assert last_exc is not None
        raise last_exc

    def _append_start_param(self, base_url: str, start: int) -> str:
        from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

        parsed = urlparse(base_url)
        query = dict(parse_qsl(parsed.query))
        query["start"] = str(start)
        new_query = urlencode(query)
        return urlunparse(parsed._replace(query=new_query))

    def scrape_search(self, search_url: str, max_jobs: int = 1000) -> List[Dict[str, Any]]:
        """
        Crawl paginated LinkedIn search results and return a list of job dicts.
        """
        all_jobs: List[Dict[str, Any]] = []
        start = 0
        page_size_guess = 25

        while len(all_jobs) < max_jobs:
            paged_url = self._append_start_param(search_url, start)
            logger.info("Scraping page starting at %d: %s", start, paged_url)
            html = self._fetch_page(paged_url)
            page_jobs = self.parse_listings(html)
            if not page_jobs:
                logger.info("No further jobs found; stopping pagination.")
                break

            for job in page_jobs:
                all_jobs.append(job.to_dict())
                if len(all_jobs) >= max_jobs:
                    break

            logger.info(
                "Accumulated %d jobs so far (page yielded %d).",
                len(all_jobs),
                len(page_jobs),
            )
            start += page_size_guess
            time.sleep(random.uniform(*self.delay_range))

        return all_jobs

    def parse_listings(self, html: str) -> List[JobPosting]:
        """
        Parse a LinkedIn jobs search HTML page into JobPosting objects.

        The method is intentionally defensive: it prefers getting partial data
        over failing completely when markup changes.
        """
        soup = BeautifulSoup(html, "lxml")

        # Try common LinkedIn job card containers.
        cards = soup.select("li.jobs-search-results__list-item")
        if not cards:
            cards = soup.select("div.base-card")

        jobs: List[JobPosting] = []
        for card in cards:
            job = self._parse_single_card(card)
            if job:
                jobs.append(job)
        return jobs

    def _parse_single_card(self, card: Any) -> Optional[JobPosting]:
        def safe_text(selector: str) -> str:
            el = card.select_one(selector)
            return el.get_text(strip=True) if el else ""

        # Title
        title = (
            safe_text("a.job-card-list__title")
            or safe_text("a.base-card__full-link")
            or safe_text("h3.base-search-card__title")
            or safe_text("h3")
        )

        if not title:
            # Card is probably malformed or an ad; skip.
            return None

        company = (
            safe_text("a.job-card-container__company-name")
            or safe_text("a.base-search-card__subtitle")
            or safe_text("span.job-card-container__primary-description")
        )

        location = (
            safe_text("span.job-card-container__metadata-item")
            or safe_text("span.job-search-card__location")
        )

        date_posted = (
            safe_text("time")
            or safe_text("div.job-card-container__listed-time")
            or safe_text("div.job-search-card__listdate")
        )

        description = ""
        description_el = card.select_one("div.job-card-list__insight")
        if description_el:
            description = description_el.get_text(" ", strip=True)

        # These fields are often not on the list view; leave blank if missing.
        seniority_level = safe_text("span.job-card-container__metadata-item--seniority")
        employment_type = safe_text(
            "span.job-card-container__metadata-item--employment-type"
        )

        industries: List[str] = []
        industries_el = card.select_one("ul.job-card-container__industry-list")
        if industries_el:
            industries = [
                li.get_text(strip=True) for li in industries_el.select("li") if li
            ]

        return JobPosting(
            job_title=title,
            company_name=company,
            location=location,
            date_posted=date_posted,
            job_description=description,
            seniority_level=seniority_level,
            employment_type=employment_type,
            industries=industries,
        )