import argparse
import asyncio
import logging
import multiprocessing
import re
import time
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urljoin, urlparse

import fitz
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from tqdm import tqdm

logging.basicConfig(filename='crawler.log', level=logging.ERROR)

class Crawler:
    def __init__(self, root_url: str, max_depth: int = 3):
        self.root_url = root_url
        self.max_depth = max_depth
        self.visited = set()
        self.to_visit = []
        self.base_domain = urlparse(root_url).netloc
        self.base_path = urlparse(root_url).path.rstrip('/')

    def is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc != self.base_domain:
            return False
        if not parsed.path.startswith(self.base_path):
            return False
        if parsed.fragment:
            return False
        return url not in self.visited

    def extract_links(self, url: str, html: str) -> List[str]:
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(url, href).split('#')[0]
            if self.is_valid_url(full_url):
                links.append(full_url)
        return links

    def crawl(self) -> List[str]:
        self.to_visit = [(self.root_url, 0)]
        ordered_urls = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            
            while self.to_visit:
                url, depth = self.to_visit.pop(0)
                if depth > self.max_depth:
                    continue
                
                try:
                    page = browser.new_page()
                    page.goto(url, timeout=60000)
                    page.wait_for_selector('body', timeout=30000)
                    html = page.content()
                    page.close()
                    
                    self.visited.add(url)
                    ordered_urls.append(url)
                    
                    if depth < self.max_depth:
                        links = self.extract_links(url, html)
                        for link in links:
                            self.visited.add(link)
                            self.to_visit.append((link, depth + 1))
                    
                    time.sleep(0.5)  # Polite delay
                
                except Exception as e:
                    logging.error(f"Error crawling {url}: {str(e)}")
            
            browser.close()
        
        return ordered_urls

def generate_pdf(task: Tuple[int, str, Path]) -> Tuple[int, str, bool]:
    index, url, temp_dir = task
    pdf_path = temp_dir / f"page_{index:04d}.pdf"
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            page.goto(url, timeout=60000)
            page.wait_for_selector('body', timeout=30000)
            
            # Try to wait for main content (customize selector as needed)
            page.wait_for_selector('main', timeout=5000)
            
            title = page.title()
            
            # Generate PDF with print styles
            page.emulate_media(media='print')
            page.pdf(
                path=str(pdf_path),
                format='A4',
                print_background=True,
                margin={'top': '20mm', 'right': '20mm', 
                        'bottom': '20mm', 'left': '20mm'}
            )
            
            browser.close()
            return (index, title, True)
    
    except Exception as e:
        logging.error(f"Error generating PDF for {url}: {str(e)}")
        return (index, "", False)

def merge_pdfs(pdf_files: List[Tuple[int, str]], output_path: str, temp_dir: Path):
    merged = fitz.open()
    toc = []
    
    # Sort by original index
    pdf_files.sort(key=lambda x: x[0])
    
    for index, title in pdf_files:
        pdf_path = temp_dir / f"page_{index:04d}.pdf"
        if not pdf_path.exists():
            continue
        
        with fitz.open(pdf_path) as doc:
            merged.insert_pdf(doc)
            # Add bookmark at first page of each document
            toc.append([1, title, merged.page_count - doc.page_count])
        
        pdf_path.unlink()  # Clean up temp file
    
    merged.set_toc(toc)
    merged.save(output_path, deflate=True, garbage=4)
    merged.close()

def main():
    parser = argparse.ArgumentParser(description='Website to PDF Book Converter')
    parser.add_argument('url', help='Root URL to start crawling')
    parser.add_argument('output', help='Output PDF filename')
    parser.add_argument('--max-depth', type=int, default=2, 
                       help='Maximum crawl depth (default: 2)')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of parallel workers (default: 4)')
    parser.add_argument('--temp-dir', default='temp_pages',
                       help='Temporary directory for PDFs (default: temp_pages)')
    
    args = parser.parse_args()
    
    temp_dir = Path(args.temp_dir)
    temp_dir.mkdir(exist_ok=True)
    
    # Step 1: Crawl website
    print("Crawling website...")
    crawler = Crawler(args.url, args.max_depth)
    urls = crawler.crawl()
    print(f"Found {len(urls)} pages to convert")
    
    # Step 2: Generate PDFs in parallel
    print("Generating PDFs...")
    tasks = [(i, url, temp_dir) for i, url in enumerate(urls, 1)]
    
    with multiprocessing.Pool(processes=args.workers) as pool:
        results = []
        for result in tqdm(pool.imap(generate_pdf, tasks), total=len(tasks)):
            results.append(result)
    
    # Filter successful results
    successful = [(i, t) for i, t, success in results if success]
    
    # Step 3: Merge PDFs
    print("Merging PDFs...")
    merge_pdfs(successful, args.output, temp_dir)
    
    # Cleanup
    temp_dir.rmdir()
    print(f"Done! Output saved to {args.output}")

if __name__ == "__main__":
    main()