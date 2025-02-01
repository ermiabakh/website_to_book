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
import sqlite3
from datetime import datetime

import pymupdf
from bs4 import BeautifulSoup
from quart import Quart, request, render_template, Response, send_file, jsonify
from quart.helpers import stream_with_context
from playwright.async_api import async_playwright
from tqdm import tqdm
from werkzeug.utils import secure_filename
from asgiref.wsgi import WsgiToAsgi

# --- Setup and Configuration ---
logging.basicConfig(filename='/tmp/crawler.log', level=logging.ERROR) # Keeping ERROR for now, might want to temporarily change to DEBUG for more info
app = Quart(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

OUTPUT_DIR = '/tmp/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)
DATABASE_PATH = os.path.join(OUTPUT_DIR, 'website_pdfs.db')

# --- Database Initialization ---
def init_db():
    """Initializes the SQLite database if it doesn't exist."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pdf_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE NOT NULL,
                filepath TEXT NOT NULL,
                download_url TEXT NOT NULL,
                website_url TEXT NOT NULL,
                conversion_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_links_crawled INTEGER,
                successful_pages INTEGER,
                failed_pages INTEGER
            )
        ''')
        conn.commit()
        logging.info("Database initialized or already exists.") # Added log
    except Exception as e:
        logging.error(f"Database initialization error: {e}") # More specific error log
    finally:
        conn.close()

init_db()

# --- Global State Management ---
current_process = {
    'active': False,
    'progress': None,
    'messages': asyncio.Queue(),
    'output_file': None,
    'output_filename': None,
    'generated_files': [],
    'crawled_urls': []
}

# --- Tqdm Progress Bar for Async Operations ---
class TqdmToQueue(tqdm):
    """Custom tqdm progress bar that sends updates to an asyncio queue."""
    def __init__(self, *args, **kwargs):
        self.queue = current_process['messages']
        super().__init__(*args, **kwargs)

    def display(self, msg=None, pos=None):
        super().display(msg, pos)
        try: # Added try-except for queue put operations
            asyncio.run_coroutine_threadsafe(
                self.queue.put({
                    'type': 'progress',
                    'current': self.n,
                    'total': self.total,
                    'message': msg if msg else self.desc
                }),
                loop=asyncio.get_event_loop()
            )
        except Exception as e:
            logging.error(f"Error putting progress to queue: {e}")

    def write(self, s, file=None, end="\n"):
        msg = s + end
        try: # Added try-except for queue put operations
            asyncio.run_coroutine_threadsafe(
                self.queue.put({
                    'type': 'message',
                    'message': msg.strip()
                }),
                loop=asyncio.get_event_loop()
            )
        except Exception as e:
            logging.error(f"Error putting message to queue: {e}")

# --- Crawler Class ---
class Crawler:
    """Crawls a website to extract URLs."""
    def __init__(self, root_url: str, max_depth: int = 3):
        self.root_url = root_url
        self.max_depth = max_depth
        self.visited = set()
        self.to_visit = []
        self.base_domain = urlparse(root_url).netloc
        self.visited.add(root_url)
        self.to_visit.append((root_url, 0))
        self.crawled_urls_list = []

    def is_valid_url(self, url: str) -> bool:
        """Checks if a URL is valid to crawl."""
        parsed = urlparse(url)
        return parsed.netloc == self.base_domain and not parsed.fragment and url not in self.visited

    def extract_links(self, url: str, html: str) -> List[str]:
        """Extracts valid links from HTML content."""
        soup = BeautifulSoup(html, 'html.parser')
        links = [urljoin(url, a['href']).split('#')[0]
                 for a in soup.find_all('a', href=True)
                 if self.is_valid_url(urljoin(url, a['href']).split('#')[0])]
        return links

    async def crawl(self) -> Tuple[List[str], List[str]]:
        """Crawls the website and returns ordered and crawled URL lists."""
        ordered_urls = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            # Increased page pool size to potentially leverage more concurrency, consider hardware and website limits.
            page_pool = [await context.new_page() for _ in range(multiprocessing.cpu_count() * 2)] # Consider adjusting multiplier

            tasks = set()
            with TqdmToQueue(desc=f"Crawling {self.root_url}", unit="page", dynamic_ncols=True) as pbar: # Removed redundant bar_format
                self.progress_bar = pbar

                while self.to_visit or tasks:
                    # Fill tasks queue as long as there are URLs to visit and available pages
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
                                    self.crawled_urls_list.append(result_url)
                                    for link in new_links:
                                        if link not in self.visited:
                                            self.visited.add(link)
                                            self.to_visit.append((link, result_depth + 1))

                                pbar.total = len(self.visited)
                                pbar.set_postfix({'depth': result_depth, 'queued': len(self.to_visit),
                                                    'found': len(self.visited), 'pending': len(tasks)}, refresh=False)
                                pbar.update(1)
                            except Exception as e:
                                logging.error(f"Error processing task: {str(e)}")
                                pbar.write(f"Error processing task: {str(e)}")

                    await asyncio.sleep(0.01) # Reduced sleep, can be tuned, or removed if CPU usage is too high

            await browser.close()
        return ordered_urls, self.crawled_urls_list

    async def crawl_page(self, url: str, depth: int, page):
        """Crawls a single page and extracts links."""
        try:
            await page.goto(url, timeout=90000, wait_until='networkidle') # Reduced timeout for faster failure if needed
            await page.wait_for_selector('body', timeout=20000) # Reduced timeout

            html = await page.content()
            links = []
            if depth < self.max_depth:
                links = self.extract_links(url, html)
                self.progress_bar.write(f"Depth {depth}: Found {len(links)} links on {url}") # Improved logging

            return url, depth, links
        except Exception as e:
            error_message = f"Error crawling {url}: {str(e)}"
            logging.error(error_message)
            self.progress_bar.write(error_message)
            return None, None, []

# --- PDF Generation Function ---
async def generate_pdf(task: Tuple[int, str, Path, asyncio.Queue]) -> Tuple[int, str, bool]:
    """Generates PDF for a single URL using a shared page queue."""
    index, url, temp_dir, page_queue = task
    pdf_path = temp_dir / f"page_{index:04d}.pdf"

    try:
        page = await page_queue.get() # Get a page from the shared pool
        await page.goto(url, timeout=90000, wait_until='networkidle') # Reduced timeout
        await page.wait_for_selector('body', timeout=20000) # Reduced timeout

        # Wait for main element - helps with fully loaded content, but non-blocking if not found quickly
        try:
            await page.wait_for_selector('main', timeout=3000) # Reduced timeout
        except:
            pass # Main element is optional

        title = await page.title()
        await page.emulate_media(media='print')
        await page.pdf(
            path=str(pdf_path),
            format='A4',
            print_background=True,
            margin={'top': '10mm', 'right': '10mm', 'bottom': '10mm', 'left': '10mm'} # Reduced margins slightly for potentially smaller PDFs and less content clipping
        )
        await page_queue.put(page) # Return page to the pool
        return index, title, True

    except Exception as e:
        error_message = f"Error generating PDF for {url}: {str(e)}"
        logging.error(error_message)
        return index, "", False

# --- PDF Merging Functions ---
def merge_pdfs(pdf_files: List[Tuple[int, str]], output_path: str, temp_dir: Path):
    """Merges multiple PDF files into a single PDF using multiprocessing."""
    num_processes = multiprocessing.cpu_count()
    chunk_size = len(pdf_files) // num_processes + 1
    chunks = [pdf_files[i:i + chunk_size] for i in range(0, len(pdf_files), chunk_size)]

    temp_dir_str = str(temp_dir) # Pass temp dir as string to avoid Path serialization issues
    with multiprocessing.Pool(processes=num_processes) as pool:
        with TqdmToQueue(total=len(chunks), desc="Merging PDF chunks", unit="chunk") as pbar:
            results = list(pbar(pool.imap_unordered(merge_chunk, [(chunk, temp_dir_str, i) for i, chunk in enumerate(chunks)])))
            # Changed to list to ensure full iteration and progress bar completion

    results.sort() # Ensure chunks are merged in the correct order

    merged_pdf = pymupdf.open()
    toc = []

    with TqdmToQueue(total=len(results), desc="Merging Chunks to Final PDF", unit="chunk") as pbar:
        for chunk_toc, chunk_path in pbar(results): # Iterating results directly for progress
            try:
                with pymupdf.open(chunk_path) as doc:
                    merged_pdf.insert_pdf(doc)
                    for lvl, title, page in chunk_toc:
                        toc.append([lvl, title, page + merged_pdf.page_count - doc.page_count])
            finally: # Ensure file is unlinked even if pymupdf fails to open/read
                try:
                    Path(chunk_path).unlink()
                except OSError as e:
                    logging.error(f"Error deleting chunk file: {chunk_path} - {e}")

    merged_pdf.set_toc(toc)
    merged_pdf.save(output_path, deflate=True, garbage=4)
    merged_pdf.close()

def merge_chunk(args: Tuple[List[Tuple[int, str]], str, int]) -> Tuple[List[List[Any]], str]:
    """Merges a chunk of PDF files into a single chunk PDF."""
    chunk, temp_dir_str, chunk_index = args
    temp_dir = Path(temp_dir_str)
    chunk_output_path = temp_dir / f"chunk_{chunk_index:04d}.pdf"

    merged_chunk_pdf = pymupdf.open()
    chunk_toc = []

    for index, title in chunk:
        pdf_path = temp_dir / f"page_{index:04d}.pdf"
        if not pdf_path.exists():
            continue

        try:
            with pymupdf.open(pdf_path) as doc:
                merged_chunk_pdf.insert_pdf(doc)
                chunk_toc.append([1, title, merged_chunk_pdf.page_count - doc.page_count + 1])
        finally: # Ensure file is unlinked even if pymupdf fails to open/read
            try:
                pdf_path.unlink()
            except OSError as e:
                logging.error(f"Error deleting page file: {pdf_path} - {e}")


    merged_chunk_pdf.set_toc(chunk_toc)
    merged_chunk_pdf.save(chunk_output_path, deflate=True, garbage=4)
    merged_chunk_pdf.close()
    return chunk_toc, str(chunk_output_path)

# --- Database Interaction Functions ---
def save_pdf_info_to_db(filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages):
    """Saves PDF file information to the database."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        logging.debug(f"Saving PDF info to DB - Filename: {filename}, Website: {website_url}") # Log before insert
        cursor.execute('''
            INSERT INTO pdf_files (filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages))
        conn.commit()
        logging.info(f"PDF info saved to DB for: {filename}") # Log on successful insert
    except sqlite3.IntegrityError:
        logging.warning(f"PDF info for filename '{filename}' already exists. Skipping save.") # Keep IntegrityError log
        conn.rollback()
    except Exception as e:
        logging.error(f"Error saving PDF info to DB: {e}") # Catch other potential DB errors
        conn.rollback() # Rollback on any error to ensure data consistency
    finally:
        conn.close()

def get_pdf_files_from_db():
    """Retrieves PDF file information from the database."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute('''SELECT filename, download_url, website_url, conversion_timestamp, total_links_crawled, successful_pages, failed_pages
                          FROM pdf_files ORDER BY conversion_timestamp DESC''')
        files = cursor.fetchall()
        logging.debug(f"Retrieved {len(files)} PDF files from DB.") # Log how many files retrieved
        return files
    except Exception as e:
        logging.error(f"Error retrieving PDF files from DB: {e}") # Log on retrieval error
        return [] # Return empty list in case of error
    finally:
        conn.close()

# --- Main Conversion Orchestration ---
async def run_conversion(url: str, max_depth: int, workers: int, output_path: str):
    """Main function to run the website to PDF conversion process."""
    temp_dir = Path('/tmp/temp_pages')
    temp_dir.mkdir(exist_ok=True)

    start_time = time.time()
    success_count = 0
    failed_count = 0
    total_crawled_links = 0

    try:
        crawler = Crawler(url, max_depth)
        urls, crawled_urls_list = await crawler.crawl()
        total_crawled_links = len(urls)
        current_process['crawled_urls'] = crawled_urls_list

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()

            # Shared page pool for PDF generation
            page_queue = asyncio.Queue()
            page_pool_size = multiprocessing.cpu_count() * 2 # Match crawl page pool for consistency, can be tuned independently.
            for _ in range(page_pool_size):
                page = await context.new_page()
                await page_queue.put(page)

            tasks = [(i, url, temp_dir, page_queue) for i, url in enumerate(urls, 1)]

            with TqdmToQueue(total=len(tasks), desc="Generating PDFs", unit="page") as pbar:
                pdf_gen_futures = [generate_pdf(task) for task in tasks]
                for result_future in asyncio.as_completed(pdf_gen_futures):
                    index, title, success = await result_future
                    if success:
                        success_count += 1
                        pbar.set_postfix_str(f"Last: {title[:30]}...", refresh=False)
                    else:
                        failed_count += 1
                    pbar.update(1)


            # Cleanup page pool - IMPORTANT: Ensure pages are closed.
            while not page_queue.empty():
                page = await page_queue.get_nowait()
                await page.close()
            await browser.close()

        successful_pdf_tasks = [(i, t) for i, t, success in [(await f) for f in pdf_gen_futures] if success]

        if successful_pdf_tasks:
            merge_pdfs(successful_pdf_tasks, output_path, temp_dir)
            current_process['output_file'] = output_path


            logging.debug("Preparing to save PDF info...") # Log before save_pdf_info_to_db call
            save_pdf_info_to_db(
                filename=current_process['output_filename'],
                filepath=current_process['output_file'],
                download_url=f"/download?filename={current_process['output_filename']}",
                website_url=url,
                total_links_crawled=total_crawled_links,
                successful_pages=success_count,
                failed_pages=failed_count
            )
            logging.debug("save_pdf_info_to_db call completed.") # Log after save_pdf_info_to_db call
        else:
            raise Exception("No pages converted successfully!")

    finally:
        end_time = time.time()
        conversion_duration = end_time - start_time
        logging.info(f"Conversion for {url} took {conversion_duration:.2f} seconds. Successful: {success_count}, Failed: {failed_count}, Total Links: {total_crawled_links}")

        # Robust temp directory cleanup even on errors
        for file in temp_dir.glob("*.pdf"):
            try:
                file.unlink()
            except OSError as e:
                logging.error(f"Error deleting temp PDF file: {file} - {e}")
        try:
            temp_dir.rmdir()
        except OSError as e:
            logging.error(f"Error removing temp directory: {temp_dir} - {e}")


# --- Quart Routes ---
@app.route('/')
async def index():
    pdf_files = get_pdf_files_from_db()
    return await render_template('index.html', pdf_files=pdf_files)

@app.route('/convert', methods=['POST'])
async def convert():
    if current_process['active']:
        return jsonify({"status": "error", "message": "A process is already running"}), 400

    current_process['active'] = True
    current_process['output_file'] = None
    current_process['output_filename'] = None
    current_process['generated_files'] = []
    current_process['crawled_urls'] = [] # Reset crawled urls

    form_data = await request.form
    data = form_data.to_dict()

    filename_input = data.get('filename', 'output')
    filename = secure_filename(filename_input)
    output_filename = f"{filename}.pdf" if filename else 'output.pdf'
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    current_process['output_filename'] = output_filename
    current_process['output_file'] = output_path

    async def run_background_conversion(): # Renamed for clarity
        try:
            await current_process['messages'].put({'type': 'crawling_start'})
            await run_conversion(
                url=data['url'],
                max_depth=int(data['depth']),
                workers=int(data['workers']), # Workers parameter not directly used in PDF gen currently, kept for future use/config
                output_path=output_path
            )

            if current_process['output_file'] and os.path.exists(current_process['output_file']):
                current_process['generated_files'] = [{
                    'filename': current_process['output_filename'],
                    'download_url': f"/download?filename={current_process['output_filename']}"
                }]
            else:
                current_process['generated_files'] = []

            await current_process['messages'].put({'type': 'complete', 'files': current_process['generated_files'], 'crawled_urls': current_process['crawled_urls']})

        except Exception as e:
            await current_process['messages'].put({'type': 'error', 'message': str(e)})
        finally:
            current_process['active'] = False

    asyncio.create_task(run_background_conversion()) # Start background task
    return jsonify({"status": "started"})

@app.route('/progress')
async def progress():
    @stream_with_context
    async def generate():
        yield f"data: {json.dumps({'type': 'crawling_urls_init'})}\n\n"
        while True:
            try:
                message = await asyncio.wait_for(current_process['messages'].get(), timeout=0.5)
            except asyncio.TimeoutError:
                yield ": keep-alive\n\n"
                continue

            yield f"data: {json.dumps(message)}\n\n"
            if message['type'] in ['complete', 'error']:
                return # End stream on completion or error

    return Response(generate(), mimetype='text/event-stream')

@app.route('/files')
async def list_files():
    pdf_files = get_pdf_files_from_db()
    files_list = [{
        'filename': filename, 'download_url': download_url, 'website_url': website_url,
        'conversion_timestamp': conversion_timestamp, 'total_links_crawled': total_links_crawled,
        'successful_pages': successful_pages, 'failed_pages': failed_pages
    } for filename, download_url, website_url, conversion_timestamp, total_links_crawled, successful_pages, failed_pages in pdf_files]
    return jsonify({'files': files_list})

@app.route('/download')
async def download():
    filename = request.args.get('filename')
    if not filename:
        return jsonify({"status": "error", "message": "Filename not provided"}), 400

    output_file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(output_file_path):
        return jsonify({"status": "error", "message": "File not found"}), 404

    response = await send_file(output_file_path, as_attachment=True, download_name=filename) # download_name for cleaner filenames
    return response

# --- ASGI App for Netlify ---
asgi_app = WsgiToAsgi(app)

# --- Main Execution ---
if __name__ == "__main__":
    app.run(debug=True)