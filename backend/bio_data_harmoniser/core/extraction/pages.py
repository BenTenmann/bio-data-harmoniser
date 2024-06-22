from dataclasses import dataclass

import html2text
import tqdm
from playwright.sync_api import sync_playwright


@dataclass
class Page:
    url: str
    content: str

    @property
    def text(self) -> str:
        h2t = html2text.HTML2Text()
        h2t.ignore_links = False
        h2t.ignore_images = True
        return h2t.handle(self.content)


def extract_pages(urls: list[str]) -> list[Page]:
    content: list[Page] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        for url in tqdm.tqdm(urls, desc="Extracting pages"):
            page.goto(url)
            html = page.content()
            content.append(Page(url, html))
        browser.close()
    return content
