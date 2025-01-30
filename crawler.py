from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def crawl_links(start_url):
    visited_links = set()
    links_to_visit = [start_url]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        while links_to_visit:
            current_url = links_to_visit.pop(0)
            if current_url in visited_links:
                continue

            try:
                page.goto(current_url)
                page.wait_for_load_state("networkidle") # Wait for dynamic content
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                visited_links.add(current_url)

                for a_tag in soup.find_all('a', href=True):
                    absolute_url = urljoin(current_url, a_tag['href'])
                    
                    if urlparse(absolute_url).netloc == urlparse(start_url).netloc: # Check if link is within the same domain
                        if absolute_url not in visited_links and absolute_url not in links_to_visit:
                            links_to_visit.append(absolute_url)

            except Exception as e:
                print(f"Error crawling {current_url}: {e}")
        browser.close()
    return list(visited_links)