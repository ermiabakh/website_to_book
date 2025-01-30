import argparse
import asyncio
import logging
import concurrent.futures
import re
import time
import multiprocessing  # Import multiprocessing
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urljoin, urlparse

import fitz
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from tqdm import tqdm

logging.basicConfig(filename='crawler.log', level=logging.ERROR)

class Crawler:
    # ... (Rest of the Crawler class remains unchanged)
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

    async def crawl(self) -> List[str]:
        self.to_visit = [(self.root_url, 0)]
        ordered_urls = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page_pool = [await context.new_page() for _ in range(multiprocessing.cpu_count() * 2)]

            tasks = set()

            with tqdm(desc=f"Crawling {self.root_url}", unit="page",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                self.progress_bar = pbar

                while self.to_visit or tasks:
                    while self.to_visit and len(tasks) < len(page_pool):
                        url, depth = self.to_visit.pop(0)
                        if depth > self.max_depth:
                            continue
                        page = page_pool.pop()
                        task = asyncio.create_task(self.crawl_page(url, depth, page))
                        tasks.add(task)
                        task.add_done_callback(lambda t, p=page: (tasks.remove(t), page_pool.append(p)))

                    if tasks:
                        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        for task in done:
                            try:
                                result_url, result_depth, new_links = task.result()
                                if result_url:
                                    ordered_urls.append(result_url)
                                    for link in new_links:
                                        if link not in self.visited:
                                            self.visited.add(link)
                                            self.to_visit.append((link, result_depth + 1))

                                pbar.total = len(self.visited) + len(self.to_visit) + len(tasks)
                                pbar.set_postfix({
                                    'depth': result_depth,
                                    'queued': len(self.to_visit),
                                    'found': len(self.visited),
                                    'pending': len(tasks)
                                })
                                pbar.update(1)
                            except Exception as e:
                                logging.error(f"Error processing task: {str(e)}")

                    await asyncio.sleep(0.1)

            await browser.close()
        return ordered_urls

    async def crawl_page(self, url: str, depth: int, page):
        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_selector('body', timeout=30000)
            html = await page.content()

            self.visited.add(url)

            links = []
            if depth < self.max_depth:
                links = self.extract_links(url, html)

            return url, depth, links
        except Exception as e:
            logging.error(f"Error crawling {url}: {str(e)}")
            self.progress_bar.write(f"Error crawling {url}: {str(e)}")
            return None, None, []

async def generate_pdf_batch(tasks: List[Tuple[int, str, Path]], batch_index: int) -> List[Tuple[int, str, bool]]:
    results = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            with tqdm(total=len(tasks), desc=f"Generating PDFs (Batch {batch_index})", unit="page",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}") as pbar:

                for index, url, temp_dir in tasks:
                    pdf_path = temp_dir / f"page_{index:04d}.pdf"
                    try:
                        await page.goto(url, timeout=60000)
                        await page.wait_for_selector('body', timeout=30000)
                        try:
                            await page.wait_for_selector('main', timeout=5000)
                        except:
                            pass

                        title = await page.title()
                        await page.emulate_media(media='print')
                        await page.pdf(
                            path=str(pdf_path),
                            format='A4',
                            print_background=True,
                            margin={'top': '20mm', 'right': '20mm',
                                    'bottom': '20mm', 'left': '20mm'}
                        )
                        results.append((index, title, True))
                        pbar.set_postfix_str(f"Last: {title[:30]}...")
                    except Exception as e:
                        logging.error(f"Error generating PDF for {url}: {str(e)}")
                        results.append((index, "", False))
                    finally:
                        pbar.update(1)

            await browser.close()
    except Exception as e:
        logging.error(f"Error in batch {batch_index}: {str(e)}")

    return results

def merge_pdfs(pdf_files: List[Tuple[int, str]], output_path: str, temp_dir: Path):
    merged = fitz.open()
    toc = []
    
    pdf_files.sort(key=lambda x: x[0])
    
    with tqdm(total=len(pdf_files), desc="Merging PDFs", unit="page",
         bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}",
         mininterval=0.5) as pbar: 
        for index, title in pdf_files:
            pdf_path = temp_dir / f"page_{index:04d}.pdf"
            if not pdf_path.exists():
                continue
            
            with fitz.open(pdf_path) as doc:
                merged.insert_pdf(doc)
                toc.append([1, title, merged.page_count - doc.page_count + 1]) 
            
            pdf_path.unlink()
            pbar.set_postfix({'current': title[:20] + '...'})
            pbar.update(1)

    merged.set_toc(toc)
    merged.save(output_path, deflate=True, garbage=4)
    merged.close()

async def run_pdf_generation(tasks, workers, temp_dir):
    """
    Generates PDFs in batches using a ProcessPoolExecutor.
    """
    batch_size = 50  # Define your batch size
    batches = [tasks[i:i + batch_size] for i in range(0, len(tasks), batch_size)]
    results = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(executor, generate_pdf_batch_sync, batch, i + 1, temp_dir)
            for i, batch in enumerate(batches)
        ]
        for future in tqdm(asyncio.as_completed(futures), total=len(futures), desc="Overall Progress", unit="batch"):
            result = await future
            results.extend(result)

    return results

def generate_pdf_batch_sync(batch, batch_index, temp_dir):
    """
    Synchronous wrapper for generate_pdf_batch to be used with ProcessPoolExecutor.
    """
    async def wrapper():
        return await generate_pdf_batch(batch, batch_index)
    return asyncio.run(wrapper())

async def main():
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
    urls = await crawler.crawl()

    print(f"\n{' Conversion Phase ':=^50}")
    print(f"Total unique pages found: {len(urls)}")
    print(f"Starting PDF generation with {args.workers} workers...\n")

    # PDF Generation phase
    tasks = [(i, url, temp_dir) for i, url in enumerate(urls, 1)]
    results = await run_pdf_generation(tasks, args.workers, temp_dir)

    success_count = sum(1 for _, _, success in results if success)
    failed_urls = [(index, title) for index, title, success in results if not success]

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
    for file in temp_dir.glob("*.pdf"):
        file.unlink()
    temp_dir.rmdir()
    print(f"\n{' Done! ':=^50}")
    print(f"Output saved to: {args.output}\n")

if __name__ == "__main__":
    asyncio.run(main())