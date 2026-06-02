import asyncio
import json
from pathlib import Path
from app.scraper import ConcurrentHumbleBundleScraper
import prefect 


@prefect.flow(name="Scrape single HB bundle")
async def scrape_bundle(hb_scraper: ConcurrentHumbleBundleScraper, bundle_url: str):
    bundle = await hb_scraper.scrape_single_bundle(bundle_url, 0, 0)
    if not bundle:
        raise Exception(f"Bundle not found")
    

@prefect.flow(name="Scrape all current HB bundles")
async def scrape_all_bundles(hb_scraper: ConcurrentHumbleBundleScraper, hb_url: str):
    ...