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
        self.progress_bar = None

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

        with sync_playwright() as p, \
            tqdm(desc=f"Crawling {self.root_url}", unit="page", 
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:

            self.progress_bar = pbar
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
                            if link not in self.visited:  # prevent duplicates in to_visit
                                self.visited.add(link)
                                self.to_visit.append((link, depth + 1))
                    
                    # Update progress bar
                    pbar.total = len(self.visited) + len(self.to_visit)
                    pbar.set_postfix({
                        'depth': depth,
                        'queued': len(self.to_visit),
                        'found': len(self.visited)
                    })
                    pbar.update(1)
                    time.sleep(0.5)
                
                except Exception as e:
                    logging.error(f"Error crawling {url}: {str(e)}")
                    pbar.write(f"Error crawling {url}: {str(e)}")
            
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
            try:
                page.wait_for_selector('main', timeout=5000)
            except:
                pass
            
            title = page.title()
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
    
    pdf_files.sort(key=lambda x: x[0])
    
    with tqdm(total=len(pdf_files), desc="Merging PDFs", unit="page",
         bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}",
         mininterval=0.5) as pbar: # change total from tasks to pdf_files
        for index, title in pdf_files:
            pdf_path = temp_dir / f"page_{index:04d}.pdf"
            if not pdf_path.exists():
                continue
            
            with fitz.open(pdf_path) as doc:
                merged.insert_pdf(doc)
                toc.append([1, title, merged.page_count - doc.page_count + 1]) # corrected page number
            
            pdf_path.unlink()
            pbar.set_postfix({'current': title[:20] + '...'})
            pbar.update(1)

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
    
    print(f"\n{' Starting PDF Generator ':=^50}")
    print(f"Root URL: {args.url}")
    print(f"Max Depth: {args.max_depth}")
    print(f"Workers: {args.workers}\n")
    
    # Crawling phase
    crawler = Crawler(args.url, args.max_depth)
    urls = crawler.crawl()
    
    print(f"\n{' Conversion Phase ':=^50}")
    print(f"Total unique pages found: {len(urls)}")
    print(f"Starting PDF generation with {args.workers} workers...\n")
    
    # PDF Generation phase
    tasks = [(i, url, temp_dir) for i, url in enumerate(urls, 1)]
    success_count = 0
    failed_urls = []
    
    with multiprocessing.Pool(processes=args.workers) as pool:
        with tqdm(total=len(tasks), desc="Generating PDFs", unit="page",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}") as pbar: # corrected bar_format
            results = []
            for result in pool.imap_unordered(generate_pdf, tasks): # changed imap to imap_unordered
                index, title, success = result
                results.append(result)
                
                if success:
                    success_count += 1
                    pbar.set_postfix_str(f"Last: {title[:30]}...")
                else:
                    failed_urls.append((index, title))
                
                pbar.update(1)
    
    # Merging phase
    print(f"\n{' Merging Phase ':=^50}")
    print(f"Successfully converted {success_count}/{len(urls)} pages")
    successful = [(i, t) for i, t, success in results if success]
    
    if successful:
        merge_pdfs(successful, args.output, temp_dir)
        print(f"\n{' Final Stats ':=^50}")
        print(f"Total pages crawled: {len(urls)}")
        print(f"Successfully converted: {success_count}")
        print(f"Failed conversions: {len(urls) - success_count}")
        print(f"Final PDF size: {Path(args.output).stat().st_size / 1024:.1f} KB")
    else:
        print("No pages converted successfully!")
    
    # Cleanup
    for file in temp_dir.glob("*.pdf"): # remove each file before removing directory
        file.unlink()
    temp_dir.rmdir()
    print(f"\n{' Done! ':=^50}")
    print(f"Output saved to: {args.output}\n")

if __name__ == "__main__":
    main()