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
from playwright.async_api import async_playwright
from tqdm import tqdm

logging.basicConfig(filename='crawler.log', level=logging.ERROR)

class AsyncCrawler:
    def __init__(self, root_url: str, max_depth: int = 3):
        self.root_url = root_url
        self.max_depth = max_depth
        self.visited = set()
        self.to_visit = []
        self.base_domain = urlparse(root_url).netloc
        self.base_path = urlparse(root_url).path.rstrip('/')
        self.progress_bar = None
        self.lock = asyncio.Lock()

    def is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc != self.base_domain:
            return False
        if not parsed.path.startswith(self.base_path):
            return False
        if parsed.fragment:
            return False
        return url not in self.visited

    async def extract_links(self, url: str, html: str) -> List[str]:
        soup = BeautifulSoup(html, 'lxml')  # Faster parser
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(url, href).split('#')[0]
            if self.is_valid_url(full_url):
                links.append(full_url)
        return links

    async def crawl_page(self, browser, url: str, depth: int):
        async with self.lock:
            if url in self.visited or depth > self.max_depth:
                return []
            self.visited.add(url)
            self.progress_bar.update(1)  # Update progress for each visited page

        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            java_script_enabled=True,
            bypass_csp=True
        )

        page = await context.new_page()
        try:
            await page.goto(url, wait_until='networkidle', timeout=20000)
            await page.wait_for_load_state('networkidle', timeout=10000)

            # Get HTML after JavaScript execution
            html = await page.content()

            # Extract links concurrently with other processing
            links = await self.extract_links(url, html)

            # Schedule new links for crawling
            async with self.lock:
                new_links = [link for link in links if link not in self.visited]
                self.to_visit.extend([(link, depth + 1) for link in new_links])
                # self.visited.update(new_links) # Not needed, already added in is_valid_url
            
            # Update progress bar postfix
            self.progress_bar.set_postfix({
                'depth': depth,
                'queued': len(self.to_visit),
                'found': len(self.visited)
            })

            return [(url, depth)]  # Return visited URL with depth for ordering
        except Exception as e:
            logging.error(f"Error crawling {url}: {str(e)}")
            self.progress_bar.write(f"Error crawling {url}: {str(e)}")
            return []  # Return empty list on error
        finally:
            await context.close()
            await page.close()

    async def crawl(self):
        self.to_visit = [(self.root_url, 0)]
        ordered_urls = []
        sem = asyncio.Semaphore(16)  # Concurrent page limit

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-gpu',
                    '--single-process',
                    '--no-zygote',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )

            with tqdm(desc=f"Crawling {self.root_url}", unit="page",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                self.progress_bar = pbar

                while self.to_visit:
                    tasks = []
                    current_batch = self.to_visit.copy()
                    self.to_visit.clear()

                    for url, depth in current_batch:
                        async with sem:
                            task = asyncio.create_task(
                                self.crawl_page(browser, url, depth)
                            )
                            tasks.append(task)

                    for task in asyncio.as_completed(tasks):
                        visited_pages = await task  # Get visited pages with depth
                        ordered_urls.extend(visited_pages)

            await browser.close()

        # Sort visited URLs based on depth and order of discovery
        ordered_urls.sort()
        return [url for url, _ in ordered_urls]

async def generate_pdf_async(task: Tuple[int, str, Path]) -> Tuple[int, str, bool]:
    index, url, temp_dir = task
    pdf_path = temp_dir / f"page_{index:04d}.pdf"
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-gpu',
                    '--single-process',
                    '--no-zygote',
                    '--no-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
            context = await browser.new_context()
            page = await context.new_page()
            
            await page.goto(url, wait_until='networkidle', timeout=15000)
            await page.wait_for_load_state('networkidle', timeout=10000)
            
            # Critical CSS/JS might be loaded after initial render
            await asyncio.sleep(1)
            
            title = await page.title()
            await page.emulate_media(media='print')
            
            await page.pdf(
                path=str(pdf_path),
                format='A4',
                print_background=True,
                margin={'top': '20mm', 'right': '20mm', 
                        'bottom': '20mm', 'left': '20mm'},
                prefer_css_page_size=True
            )
            
            await context.close()
            await browser.close()
            return (index, title, True)
    
    except Exception as e:
        logging.error(f"Error generating PDF for {url}: {str(e)}")
        return (index, "", False)

def pdf_worker_wrapper(task):
    return asyncio.run(generate_pdf_async(task))

def merge_pdfs(pdf_files: List[Tuple[int, str]], output_path: str, temp_dir: Path):
    merged = fitz.open()
    toc = []
    
    pdf_files.sort(key=lambda x: x[0])
    
    with tqdm(total=len(pdf_files), desc="Merging PDFs", unit="page",
             bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}",
             mininterval=0.5) as pbar: # corrected format and total
        for index, title in pdf_files:
            pdf_path = temp_dir / f"page_{index:04d}.pdf"
            if not pdf_path.exists():
                continue
            
            with fitz.open(pdf_path) as doc:
                merged.insert_pdf(doc)
                toc.append([1, title, merged.page_count - doc.page_count + 1]) # corrected page number
            
            pdf_path.unlink()
            pbar.set_postfix({'current': title[:20] + '...'})
            pbar.update(1) # update progress

    merged.set_toc(toc)
    merged.save(output_path, deflate=True, garbage=4)
    merged.close()

async def main_async():
    parser = argparse.ArgumentParser(description='High-performance Website to PDF Converter')
    parser.add_argument('url', help='Root URL to start crawling')
    parser.add_argument('output', help='Output PDF filename')
    parser.add_argument('--max-depth', type=int, default=2, 
                       help='Maximum crawl depth (default: 2)')
    parser.add_argument('--workers', type=int, default=multiprocessing.cpu_count()*2,
                       help=f'Parallel workers (default: {multiprocessing.cpu_count()*2})')
    parser.add_argument('--temp-dir', default='temp_pages',
                       help='Temporary directory for PDFs (default: temp_pages)')
    
    args = parser.parse_args()
    
    temp_dir = Path(args.temp_dir)
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    print(f"\n{' Starting PDF Generator ':=^50}")
    print(f"Root URL: {args.url}")
    print(f"Max Depth: {args.max_depth}")
    print(f"Workers: {args.workers}\n")
    
    # Crawling phase
    crawler = AsyncCrawler(args.url, args.max_depth)
    urls = await crawler.crawl()
    
    print(f"\n{' Conversion Phase ':=^50}")
    print(f"Total unique pages found: {len(urls)}")
    print(f"Starting PDF generation with {args.workers} workers...\n")
    
    # PDF Generation phase
    tasks = [(i, url, temp_dir) for i, url in enumerate(urls, 1)]
    success_count = 0
    failed_urls = []
    
    with multiprocessing.Pool(processes=args.workers) as pool:
        with tqdm(total=len(tasks), desc="Generating PDFs", unit="page",
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}",
                 mininterval=0.5) as pbar:
            results = []
            for result in pool.imap_unordered(pdf_worker_wrapper, tasks, chunksize=4): # use imap_unordered and chunksize
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
        print(f"Final PDF size: {Path(args.output).stat().st_size / 1024 / 1024:.1f} MB")
    else:
        print("No pages converted successfully!")
    
    # Cleanup
    for file in temp_dir.glob("*.pdf"): # remove each file before removing directory
        file.unlink()
    temp_dir.rmdir()
    print(f"\n{' Done! ':=^50}")
    print(f"Output saved to: {args.output}\n")

if __name__ == "__main__":
    asyncio.run(main_async())