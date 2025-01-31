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

class Crawler:
    def __init__(self, root_url: str, max_depth: int = 3):
        self.root_url = root_url
        self.max_depth = max_depth
        self.visited = set()
        self.to_visit = []
        self.base_domain = urlparse(root_url).netloc
        self.progress_bar = None
        # Mark root as visited immediately
        self.visited.add(root_url)
        self.to_visit.append((root_url, 0))

    def is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc != self.base_domain:
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
        ordered_urls = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page_pool = [await context.new_page() for _ in range(multiprocessing.cpu_count() * 2)]

            tasks = set()
            with tqdm(desc=f"Crawling {self.root_url}", unit="page",
                      dynamic_ncols=True, bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                self.progress_bar = pbar

                while self.to_visit or tasks:
                    # Schedule new tasks
                    while self.to_visit and len(tasks) < len(page_pool):
                        url, depth = self.to_visit.pop(0)
                        if depth > self.max_depth:
                            continue
                        page = page_pool.pop()
                        task = asyncio.create_task(self.crawl_page(url, depth, page))
                        tasks.add(task)
                        task.add_done_callback(lambda t, p=page: (tasks.remove(t), page_pool.append(p)))

                    # Process completed tasks
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

                                pbar.total = len(self.visited)
                                pbar.set_postfix({
                                    'depth': result_depth,
                                    'queued': len(self.to_visit),
                                    'found': len(self.visited),
                                    'pending': len(tasks)
                                }, refresh=False)
                                pbar.update(1)
                            except Exception as e:
                                logging.error(f"Error processing task: {str(e)}")

                    await asyncio.sleep(0.1)

            await browser.close()
        return ordered_urls

    async def crawl_page(self, url: str, depth: int, page):
        try:
            # Wait for network idle to capture dynamic content
            await page.goto(url, timeout=120000, wait_until='networkidle')
            await page.wait_for_selector('body', timeout=30000)
            html = await page.content()

            links = []
            if depth < self.max_depth:
                links = self.extract_links(url, html)
                self.progress_bar.write(f"Found {len(links)} links at depth {depth}")

            return url, depth, links
        except Exception as e:
            logging.error(f"Error crawling {url}: {str(e)}")
            self.progress_bar.write(f"Error crawling {url}: {str(e)}")
            return None, None, []

async def generate_pdf(task: Tuple[int, str, Path, asyncio.Queue]) -> Tuple[int, str, bool]:
    index, url, temp_dir, page_queue = task
    pdf_path = temp_dir / f"page_{index:04d}.pdf"

    try:
        page = await page_queue.get()
        # Wait for network idle before generating PDF
        await page.goto(url, timeout=120000, wait_until='networkidle')
        await page.wait_for_selector('body', timeout=30000)
        
        # Try to wait for main content
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

        page_queue.put_nowait(page)
        return (index, title, True)

    except Exception as e:
        logging.error(f"Error generating PDF for {url}: {str(e)}")
        return (index, "", False)

# Rest of the code remains the same as original for merge_pdfs and main function

def merge_pdfs(pdf_files: List[Tuple[int, str]], output_path: str, temp_dir: Path):
    num_processes = multiprocessing.cpu_count()
    chunk_size = len(pdf_files) // num_processes + 1
    chunks = [pdf_files[i:i + chunk_size] for i in range(0, len(pdf_files), chunk_size)]

    with multiprocessing.Pool(processes=num_processes) as pool:
        with tqdm(total=len(chunks), desc="Merging PDF chunks", unit="chunk",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}") as pbar:
            results = []
            for i, result in enumerate(pool.imap_unordered(merge_chunk, [(chunk, temp_dir, i) for i, chunk in enumerate(chunks)])):
                results.append(result)
                pbar.update()

    results.sort()  # Sort by chunk index

    merged = fitz.open()
    toc = []
    
    with tqdm(total=len(results), desc="Merging Chunks to Final PDF", unit="chunk",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}") as pbar:
        for i, (chunk_toc, chunk_path) in enumerate(results):
            with fitz.open(chunk_path) as doc:
                merged.insert_pdf(doc)
                for lvl, title, page in chunk_toc:
                    toc.append([lvl, title, page + merged.page_count - doc.page_count])
            Path(chunk_path).unlink()
            pbar.update()

    merged.set_toc(toc)
    merged.save(output_path, deflate=True, garbage=4)
    merged.close()

def merge_chunk(args):
    chunk, temp_dir, chunk_index = args
    chunk_output_path = temp_dir / f"chunk_{chunk_index:04d}.pdf"

    merged_chunk = fitz.open()
    chunk_toc = []

    for index, title in chunk:
        pdf_path = temp_dir / f"page_{index:04d}.pdf"
        if not pdf_path.exists():
            continue

        with fitz.open(pdf_path) as doc:
            merged_chunk.insert_pdf(doc)
            chunk_toc.append([1, title, merged_chunk.page_count - doc.page_count + 1])

        pdf_path.unlink()

    merged_chunk.set_toc(chunk_toc)
    merged_chunk.save(chunk_output_path, deflate=True, garbage=4)
    merged_chunk.close()

    return chunk_toc, str(chunk_output_path)

async def main():
    parser = argparse.ArgumentParser(description='Website to PDF Book Converter')
    parser.add_argument('url', help='Root URL to start crawling')
    parser.add_argument('output', help='Output PDF filename')
    parser.add_argument('--max-depth', type=int, default=5, 
                       help='Maximum crawl depth (default: 5)')
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
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        page_queue = asyncio.Queue()
        for _ in range(multiprocessing.cpu_count() * 2):
            page = await context.new_page()
            await page_queue.put(page)

        tasks = [(i, url, temp_dir, page_queue) for i, url in enumerate(urls, 1)]
        success_count = 0
        failed_urls = []
    
        async def run_pdf_generation():
            nonlocal success_count
            
            pdf_tasks = [generate_pdf(task) for task in tasks]
            
            with tqdm(total=len(pdf_tasks), desc="Generating PDFs", unit="page",
                    dynamic_ncols=True, bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:.0f}%] {postfix}") as pbar:
                
                results = []
                for f in asyncio.as_completed(pdf_tasks):
                    result = await f
                    index, title, success = result
                    results.append(result)
                    
                    if success:
                        success_count += 1
                        pbar.set_postfix_str(f"Last: {title[:30]}...", refresh=False)
                    else:
                        failed_urls.append((index, title))
                    
                    pbar.update(1)
            return results

        results = await run_pdf_generation()
        
        while not page_queue.empty():
            page = await page_queue.get()
            await page.close()

        await context.close()
        await browser.close()

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