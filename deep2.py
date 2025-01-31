# netlify/functions/deep2/deep2.py

import argparse
import asyncio
import json
import logging
import multiprocessing
import re
import time
from pathlib import Path
from typing import List, Tuple, Dict, Any
from urllib.parse import urljoin, urlparse
import os

import pymupdf
from bs4 import BeautifulSoup
from quart import Quart, request, render_template, Response, send_file
from quart.helpers import stream_with_context
from playwright.async_api import async_playwright
from tqdm import tqdm
from werkzeug.utils import secure_filename

logging.basicConfig(filename='crawler.log', level=logging.ERROR)
app = Quart(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Ensure output directory exists
OUTPUT_DIR = 'output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Global state for progress tracking
current_process = {
    'active': False,
    'progress': None,
    'messages': asyncio.Queue(),
    'output_file': None,
    'output_filename': None  # Store the desired filename
}

class TqdmToQueue(tqdm):
    def __init__(self, *args, **kwargs):
        # Initialize queue FIRST before super().__init__
        self.queue = current_process['messages']
        # Initialize parent
        super().__init__(*args, **kwargs)


        # Set required attributes that parent __del__ might need
        if not hasattr(self, 'last_print_t'):
            self.last_print_t = self.start_t - self.delay


    def display(self, msg=None, pos=None):
        # Call parent display first
        super().display(msg, pos)

        # Send update to queue
        asyncio.run_coroutine_threadsafe(
            self.queue.put({
                'type': 'progress',
                'current': self.n,
                'total': self.total,
                'message': self.desc
            }),
            loop=asyncio.get_event_loop()
        )

    def close(self):
        # Ensure parent cleanup happens properly
        if hasattr(self, 'last_print_t'):
            super().close()

class Crawler:
    def __init__(self, root_url: str, max_depth: int = 3):
        self.root_url = root_url
        self.max_depth = max_depth
        self.visited = set()
        self.to_visit = []
        self.base_domain = urlparse(root_url).netloc
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
            with TqdmToQueue(desc=f"Crawling {self.root_url}", unit="page",
                      dynamic_ncols=True, bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
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
        await page.goto(url, timeout=120000, wait_until='networkidle')
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

        page_queue.put_nowait(page)
        return (index, title, True)

    except Exception as e:
        logging.error(f"Error generating PDF for {url}: {str(e)}")
        return (index, "", False)

def merge_pdfs(pdf_files: List[Tuple[int, str]], output_path: str, temp_dir: Path):
    num_processes = multiprocessing.cpu_count()
    chunk_size = len(pdf_files) // num_processes + 1
    chunks = [pdf_files[i:i + chunk_size] for i in range(0, len(pdf_files), chunk_size)]

    with multiprocessing.Pool(processes=num_processes) as pool:
        with tqdm(total=len(chunks), desc="Merging PDF chunks", unit="chunk") as pbar:
            results = []
            for i, result in enumerate(pool.imap_unordered(merge_chunk, [(chunk, temp_dir, i) for i, chunk in enumerate(chunks)])):
                results.append(result)
                pbar.update()

    results.sort()

    merged = pymupdf.open()
    toc = []

    with tqdm(total=len(results), desc="Merging Chunks to Final PDF", unit="chunk") as pbar:
        for i, (chunk_toc, chunk_path) in enumerate(results):
            with pymupdf.open(chunk_path) as doc:
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

    merged_chunk = pymupdf.open()
    chunk_toc = []

    for index, title in chunk:
        pdf_path = temp_dir / f"page_{index:04d}.pdf"
        if not pdf_path.exists():
            continue

        with pymupdf.open(pdf_path) as doc:
            merged_chunk.insert_pdf(doc)
            chunk_toc.append([1, title, merged_chunk.page_count - doc.page_count + 1])

        pdf_path.unlink()

    merged_chunk.set_toc(chunk_toc)
    merged_chunk.save(chunk_output_path, deflate=True, garbage=4)
    merged_chunk.close()

    return chunk_toc, str(chunk_output_path)

async def run_conversion(url: str, max_depth: int, workers: int, output_path: str):
    temp_dir = Path('temp_pages')
    temp_dir.mkdir(exist_ok=True)

    try:
        crawler = Crawler(url, max_depth)
        urls = await crawler.crawl()

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

            with TqdmToQueue(total=len(tasks), desc="Generating PDFs", unit="page") as pbar:
                results = []
                for task in asyncio.as_completed([generate_pdf(t) for t in tasks]):
                    result = await task
                    index, title, success = result
                    results.append(result)

                    if success:
                        success_count += 1
                        pbar.set_postfix_str(f"Last: {title[:30]}...", refresh=False)
                    else:
                        failed_urls.append((index, title))

                    pbar.update(1)

            await browser.close()

        successful = [(i, t) for i, t, success in results if success]
        if successful:
            merge_pdfs(successful, output_path, temp_dir)
            current_process['output_file'] = output_path
        else:
            raise Exception("No pages converted successfully!")

    finally:
        for file in temp_dir.glob("*.pdf"):
            file.unlink()
        temp_dir.rmdir()

@app.route('/')
async def index():
    return await render_template('index.html')

@app.route('/convert', methods=['POST'])
async def convert():
    if current_process['active']:
        return {"status": "error", "message": "A process is already running"}, 400

    current_process['active'] = True
    current_process['output_file'] = None
    current_process['output_filename'] = None # Reset filename

    # Proper form handling in Quart
    form_data = await request.form
    data = form_data.to_dict()

    filename_input = data.get('filename', 'output') # Default filename if not provided
    filename = secure_filename(filename_input) # Sanitize filename
    if not filename:
        filename = 'output' # Fallback if sanitization results in empty string
    output_filename = f"{filename}.pdf"
    output_path = os.path.join(OUTPUT_DIR, output_filename) # Save to output directory

    current_process['output_filename'] = output_filename # Store for download route
    current_process['output_file'] = output_path # Store full output path

    async def run():
        try:
            await run_conversion(
                url=data['url'],
                max_depth=int(data['depth']),
                workers=int(data['workers']),
                output_path=output_path
            )
            await current_process['messages'].put({'type': 'complete'})
        except Exception as e:
            await current_process['messages'].put({'type': 'error', 'message': str(e)})
        finally:
            current_process['active'] = False

    asyncio.create_task(run())
    return {"status": "started"}

@app.route('/progress')
async def progress():
    @stream_with_context
    async def generate():
        try:  # Add try-except block
            while True:
                try:
                    message = await asyncio.wait_for(
                        current_process['messages'].get(),
                        timeout=0.5
                    )

                    if message['type'] == 'progress':
                        yield f"data: {json.dumps(message)}\n\n"
                    elif message['type'] == 'complete':
                        yield f"data: {json.dumps({'type': 'complete'})}\n\n"
                        break
                    elif message['type'] == 'error':
                        yield f"data: {json.dumps(message)}\n\n"
                        break
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
        except Exception as e: # Catch any errors in the generator
            logging.error(f"Error in /progress SSE stream: {e}") # Log the error
            yield f"data: {json.dumps({'type': 'error', 'message': 'SSE stream error'})}\n\n" # Send error to client

    return Response(generate(), mimetype='text/event-stream')

@app.route('/download')
async def download():
    if not current_process['output_file'] or not Path(current_process['output_file']).exists():
        return {"status": "error", "message": "File not ready"}, 404

    output_filename = current_process.get('output_filename', 'output.pdf') # Get stored filename or default
    output_file_path = current_process['output_file'] # Full path already stored

    return await send_file(
        output_file_path,
        as_attachment=True # Force download, removed filename keyword argument
    )

if __name__ == "__main__":
    app.run(debug=True)