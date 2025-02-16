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

import aiohttp
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

OUTPUT_DIR = '/tmp/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)
DATABASE_PATH = os.path.join(OUTPUT_DIR, 'website_pdfs.db')

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

current_process = {
    'active': False,
    'progress': None,
    'messages': asyncio.Queue(),
    'output_file': None,
    'output_filename': None,
    'generated_files': [],
    'crawled_urls': []
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

class AsyncCrawler:
    def __init__(self, root_url: str, max_depth: int = 3, exclude_url: str = None):
        self.root_url = root_url
        self.max_depth = max_depth
        self.exclude_url = exclude_url
        self.visited = set()
        self.ordered_urls = []
        self.base_domain = urlparse(root_url).netloc
        self.root_path = urlparse(root_url).path
        
        if not self.root_path.startswith('/'):
            self.root_path = '/' + self.root_path
        if not self.root_path.endswith('/'):
            self.root_path += '/'

    def is_valid_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        if parsed_url.netloc != self.base_domain:
            return False
        if parsed_url.fragment:
            return False

        url_path = parsed_url.path
        if not url_path.startswith('/'):
            url_path = '/' + url_path

        if not url_path.startswith(self.root_path):
            return False

        if self.exclude_url and self.exclude_url in url:
            return False

        return url not in self.visited

    async def fetch_and_extract(self, session: aiohttp.ClientSession, url: str, current_depth: int):
        if url in self.visited or current_depth > self.max_depth:
            return []
        
        self.visited.add(url)
        self.ordered_urls.append(url)
        
        try:
            async with session.get(url, timeout=20) as response:
                if response.status != 200:
                    return []
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                links = []
                
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    full_url = urljoin(url, href).split('#')[0]
                    if self.is_valid_url(full_url):
                        links.append(full_url)
                
                return links
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return []

    async def crawl(self):
        self.visited.add(self.root_url)
        self.ordered_urls.append(self.root_url)
        current_level = [self.root_url]
        current_depth = 0

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
            with TqdmToQueue(desc=f"Crawling {self.root_url}", unit="page") as pbar:
                while current_level and current_depth < self.max_depth:
                    tasks = [self.fetch_and_extract(session, url, current_depth) for url in current_level]
                    results = await asyncio.gather(*tasks)
                    
                    next_level = []
                    for links in results:
                        next_level.extend(links)
                    
                    seen = set()
                    unique_next = []
                    for url in next_level:
                        if url not in self.visited and url not in seen:
                            seen.add(url)
                            unique_next.append(url)
                    
                    pbar.total = len(self.visited)
                    pbar.update(len(current_level))
                    pbar.set_postfix({
                        'depth': current_depth,
                        'queued': len(unique_next),
                        'found': len(self.visited)
                    })
                    
                    current_level = unique_next
                    current_depth += 1

        return self.ordered_urls

async def generate_pdf(task: Tuple[int, str, Path, asyncio.Queue]) -> Tuple[int, str, bool]:
    index, url, temp_dir, page_queue = task
    pdf_path = temp_dir / f"page_{index:04d}.pdf"

    try:
        page = await page_queue.get()
        await page.goto(url, timeout=120000, wait_until='networkidle')
        await page.wait_for_selector('body', timeout=30000)
        await asyncio.sleep(3)

        title = await page.title()
        await page.emulate_media(media='print')
        await page.pdf(
            path=str(pdf_path),
            format='A3',
            print_background=True,
            margin={'top': '10mm', 'right': '10mm', 'bottom': '10mm', 'left': '10mm'}
        )

        await page_queue.put(page)
        return (index, title, True)
    except Exception as e:
        logging.error(f"Error generating PDF for {url}: {str(e)}")
        return (index, "", False)

def get_pdf_files_from_db():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('''SELECT filename, download_url, website_url, conversion_timestamp, total_links_crawled, successful_pages, failed_pages FROM pdf_files ORDER BY conversion_timestamp DESC''')
    files = cursor.fetchall()
    conn.close()
    return files

def merge_pdfs(pdf_files: List[Tuple[int, str]], output_path: str, temp_dir: Path):
    num_processes = multiprocessing.cpu_count()
    chunk_size = len(pdf_files) // num_processes + 1
    chunks = [pdf_files[i:i + chunk_size] for i in range(0, len(pdf_files), chunk_size)]

    with multiprocessing.Pool(processes=num_processes) as pool:
        with TqdmToQueue(total=len(chunks), desc="Merging PDF chunks") as pbar:
            results = []
            for result in pool.imap_unordered(merge_chunk, [(chunk, str(temp_dir), i) for i, chunk in enumerate(chunks)]):
                results.append(result)
                pbar.update()

    results.sort()
    merged = pymupdf.open()
    toc = []

    with TqdmToQueue(total=len(results), desc="Finalizing PDF") as pbar:
        for chunk_toc, chunk_path in results:
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
        if pdf_path.exists():
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
    cursor.execute('''
        INSERT INTO pdf_files 
        (filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages))
    conn.commit()
    conn.close()

async def run_conversion(url: str, max_depth: int, workers: int, output_path: str, exclude_url: str = None):
    temp_dir = Path('/tmp/temp_pages')
    temp_dir.mkdir(exist_ok=True)
    start_time = time.time()

    try:
        crawler = AsyncCrawler(url, max_depth, exclude_url)
        urls = await crawler.crawl()
        current_process['crawled_urls'] = urls.copy()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page_pool_size = min(workers, 20) if workers > 0 else 10
            page_queue = asyncio.Queue()
            for _ in range(page_pool_size):
                page = await context.new_page()
                await page_queue.put(page)

            tasks = [(i, url, temp_dir, page_queue) for i, url in enumerate(urls, 1)]
            success_count = 0
            failed_count = 0

            with TqdmToQueue(total=len(tasks), desc="Generating PDFs") as pbar:
                results = []
                for coro in asyncio.as_completed([generate_pdf(t) for t in tasks]):
                    result = await coro
                    index, title, success = result
                    results.append(result)
                    if success:
                        success_count += 1
                        pbar.set_postfix_str(f"Last: {title[:30]}...")
                    else:
                        failed_count += 1
                    pbar.update(1)

            await browser.close()

        successful = [(i, t) for i, t, success in results if success]
        if successful:
            merge_pdfs(successful, output_path, temp_dir)
            save_pdf_info_to_db(
                current_process['output_filename'],
                output_path,
                f"/download?filename={current_process['output_filename']}",
                url,
                len(urls),
                success_count,
                failed_count
            )
        else:
            raise Exception("No pages converted successfully!")

    finally:
        logging.info(f"Conversion completed in {time.time() - start_time:.2f}s")

@app.route('/convert', methods=['POST'])
async def convert():
    if current_process['active']:
        return jsonify({"status": "error", "message": "Process already running"}), 400

    current_process.update({
        'active': True,
        'output_file': None,
        'output_filename': None,
        'generated_files': [],
        'crawled_urls': []
    })

    form = await request.form
    output_filename = f"{secure_filename(form.get('filename', 'output'))}.pdf"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    current_process.update({
        'output_filename': output_filename,
        'output_file': output_path
    })

    async def conversion_task():
        try:
            await run_conversion(
                url=form['url'],
                max_depth=int(form.get('depth', 3)),
                workers=int(form.get('workers', 10)),
                output_path=output_path,
                exclude_url=form.get('exclude_url')
            )
            await current_process['messages'].put({
                'type': 'complete',
                'files': [{
                    'filename': output_filename,
                    'download_url': f"/download?filename={output_filename}"
                }],
                'crawled_urls': current_process['crawled_urls']
            })
        except Exception as e:
            await current_process['messages'].put({'type': 'error', 'message': str(e)})
        finally:
            current_process['active'] = False

    asyncio.create_task(conversion_task())
    return jsonify({"status": "started"})

@app.route('/')
async def index():
    pdf_files = get_pdf_files_from_db()
    return await render_template('index.html', pdf_files=pdf_files) # Pass pdf_files to template

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