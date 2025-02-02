#!/usr/bin/env python
import os
import re
import sys
import subprocess
import threading
import asyncio
import traceback
import logging
from urllib.parse import urlparse, urljoin

# Set up logging for detailed output.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Auto-install Dependencies ---
def auto_install_dependencies():
    import importlib
    packages = {
        "flask": "flask",
        "bs4": "bs4",
        "playwright": "playwright",
        "fitz": "PyMuPDF",      # PyMuPDF is imported as fitz.
        "psutil": "psutil"      # For detecting system memory.
    }
    for mod, pkg in packages.items():
        try:
            importlib.import_module(mod)
        except ImportError:
            logging.info(f"Installing {pkg}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
    try:
        subprocess.check_call([sys.executable, "-m", "playwright", "install"])
    except Exception as e:
        logging.exception("Error installing Playwright browsers:")

auto_install_dependencies()

# --- Imports after auto-install ---
from flask import Flask, render_template_string, request, jsonify, send_from_directory
from bs4 import BeautifulSoup
import fitz  # PyMuPDF (imported as fitz)
import psutil
from playwright.async_api import async_playwright

# --- Global Progress and PDF Info ---
progress = {
    "total_urls": 0,
    "downloaded": 0,
    "converted": 0,
    "state": "Idle",
    "error": "",
    "final_pdf": ""  # Final PDF filename once merging is complete.
}
pdf_info_list = []  # Will hold info for each generated PDF page.

# --- HTML Template using Bootstrap 5 ---
HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Website to PDF Book Generator</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- Bootstrap 5 CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
      body { padding-top: 2rem; background-color: #f8f9fa; }
      .container { max-width: 800px; }
      .progress { height: 25px; }
    </style>
    <script>
      function startScraping() {
        document.getElementById('state').innerText = "Starting...";
        var url = document.getElementById('url').value;
        var workers = document.getElementById('workers').value;
        var depth = document.getElementById('depth').value;
        var chunk = document.getElementById('chunk').value;
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "/start", true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.send(JSON.stringify({
          url: url,
          workers: workers,
          depth: depth,
          chunk: chunk
        }));
      }
      function updateProgress() {
        var xhr = new XMLHttpRequest();
        xhr.open("GET", "/progress", true);
        xhr.onload = function() {
          if (xhr.status === 200) {
            var data = JSON.parse(xhr.responseText);
            document.getElementById('total').innerText = data.total_urls;
            document.getElementById('downloaded').innerText = data.downloaded;
            document.getElementById('converted').innerText = data.converted;
            document.getElementById('state').innerText = data.state;
            var prog = document.getElementById('progressBar');
            if(data.total_urls > 0){
              prog.style.width = ((data.converted / data.total_urls) * 100) + "%";
              prog.innerText = Math.round((data.converted / data.total_urls) * 100) + "%";
            }
            if(data.final_pdf){
              var dlLink = document.getElementById('downloadLink');
              dlLink.href = "/download/" + data.final_pdf;
              dlLink.style.display = "block";
            }
            if(data.error){
              var errDiv = document.getElementById('error');
              errDiv.innerText = data.error;
              errDiv.style.display = "block";
            }
          }
        }
        xhr.send();
      }
      setInterval(updateProgress, 1000);
    </script>
  </head>
  <body>
    <div class="container">
      <h1 class="mb-4">Website to PDF Book Generator</h1>
      <div class="card mb-4">
        <div class="card-body">
          <form onsubmit="startScraping(); return false;">
            <div class="mb-3">
              <label for="url" class="form-label">Website URL:</label>
              <input type="text" id="url" name="url" class="form-control" required/>
            </div>
            <div class="mb-3">
              <label for="workers" class="form-label">Workers (parallel):</label>
              <input type="number" id="workers" name="workers" value="4" min="1" class="form-control" required/>
            </div>
            <div class="mb-3">
              <label for="depth" class="form-label">Crawl Depth:</label>
              <input type="number" id="depth" name="depth" value="1" min="1" class="form-control" required/>
            </div>
            <div class="mb-3">
              <label for="chunk" class="form-label">Chunk Size (pages per batch):</label>
              <input type="number" id="chunk" name="chunk" value="5" min="1" class="form-control" required/>
            </div>
            <button type="submit" class="btn btn-primary">Start</button>
          </form>
        </div>
      </div>
      <div class="mb-3">
        <h3>Progress</h3>
        <ul class="list-group mb-3">
          <li class="list-group-item">Total URLs Found: <span id="total">0</span></li>
          <li class="list-group-item">Pages Downloaded: <span id="downloaded">0</span></li>
          <li class="list-group-item">Pages Converted to PDF: <span id="converted">0</span></li>
          <li class="list-group-item">Current State: <span id="state">Idle</span></li>
        </ul>
        <div class="progress mb-3">
          <div id="progressBar" class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%;">0%</div>
        </div>
        <div class="alert alert-danger" id="error" style="display: none;"></div>
        <a id="downloadLink" href="#" class="btn btn-success" style="display: none;" download>Download Final PDF</a>
      </div>
    </div>
    <!-- Bootstrap 5 JS (optional) -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  </body>
</html>
"""

# --- Flask Application ---
app = Flask(__name__)

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route("/start", methods=["POST"])
def start():
    data = request.get_json()
    url = data.get("url")
    workers = int(data.get("workers", 4))
    depth = int(data.get("depth", 1))
    chunk_size = int(data.get("chunk", 5))
    
    # Detect system resources.
    num_cores = os.cpu_count() or 1
    vm = psutil.virtual_memory()
    logging.debug(f"Detected CPU cores: {num_cores}, Total Memory: {vm.total/1024/1024:.2f} MB, Available: {vm.available/1024/1024:.2f} MB")
    
    # If provided workers exceed core count, limit to available cores.
    if workers > num_cores:
        logging.debug(f"Reducing worker count from {workers} to {num_cores} based on CPU cores.")
        workers = num_cores

    # Optionally, you could adjust chunk_size based on available memory if needed.
    # For now we just log it.
    logging.debug(f"Using chunk size: {chunk_size}")
    
    # Reset progress and PDF info.
    progress.update({
        "total_urls": 0,
        "downloaded": 0,
        "converted": 0,
        "state": "Starting",
        "error": "",
        "final_pdf": ""
    })
    global pdf_info_list
    pdf_info_list = []
    
    threading.Thread(target=run_scraping, args=(url, workers, depth, chunk_size), daemon=True).start()
    return jsonify({"status": "started"})

@app.route("/progress")
def get_progress():
    return jsonify(progress)

# Endpoint to download final PDF.
@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory("pdf", filename, as_attachment=True)

# --- Background Process Runner ---
def run_scraping(root_url, workers, max_depth, chunk_size):
    try:
        asyncio.run(main_scraping(root_url, workers, max_depth, chunk_size))
    except Exception as e:
        progress["state"] = "Fatal Error"
        progress["error"] = str(e)
        logging.exception("Error in run_scraping:")

# --- Main Asynchronous Function ---
async def main_scraping(root_url, workers, max_depth, chunk_size):
    global progress, pdf_info_list
    progress["state"] = "Initializing"
    base_domain = get_base_domain(root_url)
    download_dir = os.path.join("downloads", sanitize_filename(base_domain))
    os.makedirs(download_dir, exist_ok=True)
    pdf_dir = "pdf"
    os.makedirs(pdf_dir, exist_ok=True)
    temp_pdf_dir = os.path.join("temp_pdfs")
    os.makedirs(temp_pdf_dir, exist_ok=True)

    progress["state"] = "Crawling links"
    try:
        crawled_urls = await crawl_links(root_url, base_domain, max_depth)
    except Exception as e:
        progress["state"] = "Error during crawling"
        progress["error"] = str(e)
        logging.exception("Error during crawl_links:")
        return
    progress["total_urls"] = len(crawled_urls)
    logging.debug(f"Found {len(crawled_urls)} unique URLs.")

    progress["state"] = "Generating PDFs"
    semaphore = asyncio.Semaphore(workers)
    total = len(crawled_urls)
    
    # Process URLs in chunks.
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for start in range(0, total, chunk_size):
            chunk_urls = crawled_urls[start:start+chunk_size]
            chunk_tasks = []
            for i, url in enumerate(chunk_urls, start=start):
                chunk_tasks.append(generate_pdf_task(browser, url, temp_pdf_dir, semaphore, i))
            results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, dict):
                    pdf_info_list.append(res)
        await browser.close()

    pdf_info_list.sort(key=lambda x: x["order"])
    
    progress["state"] = "Merging PDFs"
    try:
        final_pdf_filename = f"{sanitize_filename(base_domain)}_book.pdf"
        final_pdf_path = os.path.join(pdf_dir, final_pdf_filename)
        merge_pdfs_with_toc(pdf_info_list, final_pdf_path)
        progress["final_pdf"] = final_pdf_filename
        progress["state"] = "Completed. Final PDF ready."
        logging.debug("PDF merging completed successfully.")
    except Exception as e:
        progress["state"] = "Error during merging"
        progress["error"] = str(e)
        logging.exception("Error during merge_pdfs_with_toc:")

# --- Helper Functions ---
def get_base_domain(url):
    parsed = urlparse(url)
    return parsed.netloc

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

async def crawl_links(root_url, base_domain, max_depth):
    visited = set()
    queue = [(root_url, 0)]
    result = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        while queue:
            current_url, depth = queue.pop(0)
            if current_url in visited or depth > max_depth:
                continue
            visited.add(current_url)
            result.append(current_url)
            try:
                page = await context.new_page()
                await page.goto(current_url, timeout=60000)
                await page.wait_for_load_state("networkidle", timeout=60000)
                content = await page.content()
                soup = BeautifulSoup(content, "html.parser")
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    abs_url = urljoin(current_url, href)
                    if base_domain in urlparse(abs_url).netloc and abs_url not in visited:
                        queue.append((abs_url, depth + 1))
                await page.close()
            except Exception as e:
                logging.exception(f"Error crawling {current_url}:")
        await browser.close()
    return list(visited)

async def generate_pdf_task(browser, url, temp_pdf_dir, semaphore, idx):
    global progress
    result = None
    async with semaphore:
        try:
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("networkidle", timeout=60000)
            title = await page.title()
            if not title:
                title = url
            pdf_bytes = await page.pdf(format="A4", print_background=True)
            filename = os.path.join(temp_pdf_dir, f"page_{idx}.pdf")
            with open(filename, "wb") as f:
                f.write(pdf_bytes)
            progress["downloaded"] += 1
            progress["converted"] += 1
            logging.debug(f"PDF generated for {url} with title: {title}")
            result = {"url": url, "title": title, "filename": filename, "order": idx}
            await page.close()
            await context.close()
        except Exception as e:
            err = f"Error generating PDF for {url}: {e}"
            progress["error"] = err
            logging.exception(err)
        return result

def merge_pdfs_with_toc(pdf_info_list, output_path):
    if not pdf_info_list:
        raise ValueError("No PDFs to merge.")
    doc = fitz.open()
    toc = []
    page_offset = 0
    for info in pdf_info_list:
        filename = info["filename"]
        title = info["title"]
        try:
            with fitz.open(filename) as mdoc:
                num_pages = mdoc.page_count
                toc.append([1, title, page_offset + 1])
                doc.insert_pdf(mdoc)
                page_offset += num_pages
        except Exception as e:
            logging.exception(f"Error merging {filename}:")
    if toc:
        doc.set_toc(toc)
    doc.save(output_path)
    doc.close()

# --- Run Flask App on Port 9999 ---
if __name__ == "__main__":
    app.run(port=9999)
