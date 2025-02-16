import argparse
import asyncio
import json
import logging
import multiprocessing
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

logging.basicConfig(filename='/tmp/crawler.log', level=logging.ERROR)
app = Quart(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Ensure output directory exists in /tmp for Netlify functions
OUTPUT_DIR = '/tmp/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)
DATABASE_PATH = os.path.join(OUTPUT_DIR, 'website_pdfs.db')

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
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
    conn.close()

init_db()

# Global state for progress tracking
current_process = {
    'active': False,
    'progress': None,
    'messages': asyncio.Queue(),
    'output_file': None,
    'output_filename': None,
    'generated_files': [],
    'crawled_urls': [] # To store crawled URLs before conversion
}

class TqdmToQueue(tqdm):
    def __init__(self, *args, **kwargs):
        self.queue = current_process['messages']
        super().__init__(*args, **kwargs)

    def display(self, msg=None, pos=None):
        super().display(msg, pos)
        asyncio.run_coroutine_threadsafe(
            self.queue.put({
                'type': 'progress',
                'current': self.n,
                'total': self.total,
                'message': msg if msg else self.desc
            }),
            loop=asyncio.get_event_loop()
        )

    def write(self, s, file=None, end="\n"):
        msg = s + end
        asyncio.run_coroutine_threadsafe(
            self.queue.put({
                'type': 'message',
                'message': msg.strip()
            }),
            loop=asyncio.get_event_loop()
        )

class Crawler:
    def __init__(self, root_url: str, max_depth: int = 3, exclude_url: str = None):
        self.root_url = root_url
        self.max_depth = max_depth
        self.visited = set()
        self.to_visit = []
        self.base_domain = urlparse(root_url).netloc
        self.root_path = urlparse(root_url).path
        if not self.root_path:
            self.root_path = "/"
        # Ensure root_path always starts and ends with a slash for consistent matching
        if not self.root_path.startswith('/'):
            self.root_path = '/' + self.root_path
        if not self.root_path.endswith('/'):
            self.root_path += '/'

        self.exclude_url = exclude_url
        self.visited.add(root_url)
        self.to_visit.append((root_url, 0))
        self.crawled_urls_list = [] # Store crawled URLs in order

    def is_valid_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        if parsed_url.netloc != self.base_domain:
            return False
        if parsed_url.fragment:
            return False

        url_path = parsed_url.path
        # Ensure url_path also starts with a slash for consistent comparison
        if not url_path.startswith('/'):
            url_path = '/' + url_path

        # Correctly check if the url_path starts with the root_path
        if not url_path.startswith(self.root_path):
            return False

        if self.exclude_url and self.exclude_url in url:
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
            # Limit page pool size to a reasonable number
            page_pool_size = min(multiprocessing.cpu_count() * 2, 20)
            page_pool = [await context.new_page() for _ in range(page_pool_size)]

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
                                    self.crawled_urls_list.append(result_url) # Add to crawled URLs list
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
                                pbar.write(f"Error processing task: {str(e)}")

                    await asyncio.sleep(0.1)

            await browser.close()
        return ordered_urls, self.crawled_urls_list # Return ordered URLs and crawled URLs list

    async def crawl_page(self, url: str, depth: int, page):
        try:
            await page.goto(url, timeout=120000, wait_until='networkidle')
            await page.wait_for_selector('body', timeout=30000)

            # Wait for javascript to render content - Increased wait time and ensure 'body' is rendered first
            await asyncio.sleep(5)

            html = await page.content()

            links = []
            if depth < self.max_depth:
                links = self.extract_links(url, html)
                self.progress_bar.write(f"Depth {depth}: Found {len(links)} links")

            return url, depth, links
        except Exception as e:
            error_message = f"Error crawling {url}: {str(e)}"
            logging.error(error_message)
            self.progress_bar.write(error_message)
            return None, None, []

async def generate_pdf(task: Tuple[int, str, Path, asyncio.Queue]) -> Tuple[int, str, bool]:
    index, url, temp_dir, page_queue = task
    pdf_path = temp_dir / f"page_{index:04d}.pdf"

    try:
        page = await page_queue.get()
        await page.goto(url, timeout=120000, wait_until='networkidle')
        await page.wait_for_selector('body', timeout=30000)

        # Wait for javascript to render before pdf generation
        await asyncio.sleep(5)

        try:
            await page.wait_for_selector('main', timeout=5000)
        except:
            pass

        title = await page.title()
        await page.emulate_media(media='print')
        await page.pdf(
            path=str(pdf_path),
            format='A3', # Changed to A3 paper size
            print_background=True,
            margin={'top': '10mm', 'right': '10mm', # Reduced margins
                    'bottom': '10mm', 'left': '10mm'} # Reduced margins
        )

        await page_queue.put(page)
        return (index, title, True)

    except Exception as e:
        error_message = f"Error generating PDF for {url}: {str(e)}"
        logging.error(error_message)
        return (index, "", False)

def merge_pdfs(pdf_files: List[Tuple[int, str]], output_path: str, temp_dir: Path):
    num_processes = multiprocessing.cpu_count()
    chunk_size = len(pdf_files) // num_processes + 1
    chunks = [pdf_files[i:i + chunk_size] for i in range(0, len(pdf_files), chunk_size)]

    temp_dir_str = str(temp_dir)
    with multiprocessing.Pool(processes=num_processes) as pool:
        with TqdmToQueue(total=len(chunks), desc="Merging PDF chunks", unit="chunk") as pbar:
            results = []
            for i, result in enumerate(pool.imap_unordered(merge_chunk, [(chunk, temp_dir_str, i) for i, chunk in enumerate(chunks)])):
                results.append(result)
                pbar.update()

    results.sort()

    merged = pymupdf.open()
    toc = []

    with TqdmToQueue(total=len(results), desc="Merging Chunks to Final PDF", unit="chunk") as pbar:
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
    chunk, temp_dir_str, chunk_index = args
    temp_dir = Path(temp_dir_str)
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

def save_pdf_info_to_db(filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO pdf_files (filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages))
        conn.commit()
    except sqlite3.IntegrityError:
        logging.warning(f"PDF info for filename '{filename}' already exists in the database.")
        conn.rollback() # Avoid commit if there's an error
    finally:
        conn.close()

def get_pdf_files_from_db():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('''SELECT filename, download_url, website_url, conversion_timestamp, total_links_crawled, successful_pages, failed_pages FROM pdf_files ORDER BY conversion_timestamp DESC''')
    files = cursor.fetchall()
    conn.close()
    return files

async def run_conversion(url: str, max_depth: int, workers: int, output_path: str, exclude_url: str = None):
    temp_dir = Path('/tmp/temp_pages')
    temp_dir.mkdir(exist_ok=True)
    crawled_urls_list_for_display = [] # list to display on frontend before conversion

    start_time = time.time() # Start time for stats

    try:
        crawler = Crawler(url, max_depth, exclude_url=exclude_url) # Pass exclude_url to Crawler
        urls, crawled_urls_list = await crawler.crawl() # Get crawled urls list
        crawled_urls_list_for_display = crawled_urls_list # for frontend display
        current_process['crawled_urls'] = crawled_urls_list_for_display # Store for frontend to access

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()

            # Use multiprocessing.cpu_count() * 2 as a default page pool size, limit by workers input (clarify workers meaning)
            page_pool_size = min(workers if workers > 0 else multiprocessing.cpu_count() * 2, 20) # Limit to 20 for reasonable load, and use workers input
            page_queue = asyncio.Queue()
            for _ in range(page_pool_size):
                page = await context.new_page()
                await page_queue.put(page)

            tasks = [(i, url, temp_dir, page_queue) for i, url in enumerate(urls, 1)]
            success_count = 0
            failed_count = 0 # Track failed pages
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
                        failed_count += 1 # Increment failed page count
                        failed_urls.append((index, title))

                    pbar.update(1)

            await browser.close()

        successful = [(i, t) for i, t, success in results if success]
        if successful:
            merge_pdfs(successful, output_path, temp_dir)
            current_process['output_file'] = output_path

            # Save PDF info to database after successful merge
            save_pdf_info_to_db(
                filename=current_process['output_filename'],
                filepath=current_process['output_file'],
                download_url=f"/download?filename={current_process['output_filename']}",
                website_url=url,
                total_links_crawled=len(urls), # total_links_crawled
                successful_pages=success_count, # successful_pages
                failed_pages=failed_count # failed_pages
            )

        else:
            raise Exception("No pages converted successfully!")

    finally:
        end_time = time.time() # End time for stats
        conversion_duration = end_time - start_time
        logging.info(f"Conversion for {url} took {conversion_duration:.2f} seconds. Successful pages: {success_count}, Failed pages: {failed_count}, Total Links Crawled: {len(urls)}")

        for file in temp_dir.glob("*.pdf"):
            try:
                file.unlink()
            except:
                pass
        try:
            temp_dir.rmdir()
        except:
            pass

@app.route('/')
async def index():
    pdf_files = get_pdf_files_from_db()
    return await render_template('index.html', pdf_files=pdf_files) # Pass pdf_files to template

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
    if not filename:
        filename = 'output'
    output_filename = f"{filename}.pdf"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    exclude_url = data.get('exclude_url', None) # Get exclude URL from form
    workers_input = int(data.get('workers', 2)) # Get workers from form, default to 2 if not provided, convert to int

    current_process['output_filename'] = output_filename
    current_process['output_file'] = output_path

    async def run():
        try:
            await current_process['messages'].put({'type': 'crawling_start'}) # Indicate crawling start
            await run_conversion(
                url=data['url'],
                max_depth=int(data['depth']),
                workers=workers_input, # Use workers input from form
                output_path=output_path,
                exclude_url=exclude_url # Pass exclude_url to run_conversion
            )

            if current_process['output_file'] and os.path.exists(current_process['output_file']):
                current_process['generated_files'] = [{
                    'filename': current_process['output_filename'],
                    'download_url': f"/download?filename={current_process['output_filename']}"
                }]
            else:
                current_process['generated_files'] = []

            await current_process['messages'].put({'type': 'complete', 'files': current_process['generated_files'], 'crawled_urls': current_process['crawled_urls']}) # Send crawled URLs on complete

        except Exception as e:
            await current_process['messages'].put({'type': 'error', 'message': str(e)})
        finally:
            current_process['active'] = False

    asyncio.create_task(run())
    return jsonify({"status": "started"})

@app.route('/progress')
async def progress():
    @stream_with_context
    async def generate():
        try:
            yield f"data: {json.dumps({'type': 'crawling_urls_init'})}\n\n" # Initial signal to clear crawled URL table on frontend
            messages_count = 0 # Initialize message counter
            while True:
                try:
                    message = await asyncio.wait_for(
                        current_process['messages'].get(),
                        timeout=0.5
                    )
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue

                if message['type'] == 'progress':
                    yield f"data: {json.dumps(message)}\n\n"
                elif message['type'] == 'message':
                    messages_count += 1
                    message_payload = {'type': 'message', 'message': message['message'], 'count': messages_count} # Send message count
                    yield f"data: {json.dumps(message_payload)}\n\n"
                elif message['type'] == 'crawling_start':
                    yield f"data: {json.dumps({'type': 'crawling_start'})}\n\n" # Signal crawling started
                elif message['type'] == 'complete':
                    message['crawled_urls'] = current_process['crawled_urls'] # Add crawled URLs to complete message
                    yield f"data: {json.dumps(message)}\n\n"
                    return
                elif message['type'] == 'error':
                    yield f"data: {json.dumps(message)}\n\n"
                    return
        except Exception as e:
            logging.error(f"Error in /progress SSE stream: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': 'SSE stream error'})}\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/files')
async def list_files():
    pdf_files = get_pdf_files_from_db()
    files_list = []
    for file_data in pdf_files:
        filename, download_url, website_url, conversion_timestamp, total_links_crawled, successful_pages, failed_pages = file_data
        files_list.append({
            'filename': filename,
            'download_url': download_url,
            'website_url': website_url,
            'conversion_timestamp': conversion_timestamp,
            'total_links_crawled': total_links_crawled,
            'successful_pages': successful_pages,
            'failed_pages': failed_pages
        })
    return jsonify({'files': files_list}) # Return extended file info

@app.route('/download')
async def download():
    filename = request.args.get('filename')
    if not filename:
        return jsonify({"status": "error", "message": "Filename not provided"}), 400

    output_file_path = os.path.join(OUTPUT_DIR, filename)

    if not os.path.exists(output_file_path):
        return jsonify({"status": "error", "message": "File not found"}), 404

    response = await send_file(
        output_file_path,
        as_attachment=True
    )
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response


if __name__ == "__main__":
    app.run(debug=True)