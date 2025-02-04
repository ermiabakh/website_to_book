#!/usr/bin/env python
import os
import re
import sys
import subprocess
import threading
import asyncio
import traceback
import logging
import sqlite3
import requests
import base64
import mimetypes
import shutil
from datetime import datetime
from urllib.parse import urlparse, urljoin
from flask import Flask, render_template_string, request, jsonify, send_from_directory, redirect
from bs4 import BeautifulSoup
import psutil
from playwright.async_api import async_playwright
from threading import Event
import concurrent.futures
import random # For User-Agent randomization

# Automatically install aiohttp if not available
try:
    import aiohttp  # for asynchronous HTTP crawling
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "aiohttp"])
    import aiohttp

# Use Ghostscript for PDF compression.
from dotenv import load_dotenv
load_dotenv()

client_id = os.getenv('GOOGLE_OAUTH_CLIENT_ID')
client_secret = os.getenv('GOOGLE_OAUTH_CLIENT_SECRET')

# --- Google Drive OAuth and API Imports ---
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "google-auth", "google-auth-oauthlib", "google-api-python-client"])
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

# --- Auto-install Dependencies ---
def auto_install_dependencies():
    import importlib
    packages = {
        "flask": "flask",
        "bs4": "bs4",
        "playwright": "playwright",
        "playwright_stealth": "playwright_stealth", # Install stealth plugin
        "psutil": "psutil",
        "aiohttp": "aiohttp",
        "dotenv": "dotenv"
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

from playwright_stealth import stealth_async # Import stealth

# --- Global Configuration & Logging ---
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("app.log", mode="a")
file_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# --- SQLite Database Setup ---
DB_FILENAME = "jobs.db"
def init_db():
    conn = sqlite3.connect(DB_FILENAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT,
        start_time TEXT,
        finish_time TEXT,
        url_count INTEGER,
        html_file TEXT,
        pdf_file TEXT,
        compressed_pdf TEXT,
        drive_link TEXT,
        drive_token TEXT,
        status TEXT
    )''')
    conn.commit()
    conn.close()
def insert_job(job_name, start_time, status):
    conn = sqlite3.connect(DB_FILENAME)
    c = conn.cursor()
    c.execute("INSERT INTO jobs (job_name, start_time, status) VALUES (?, ?, ?)", (job_name, start_time, status))
    job_id = c.lastrowid
    conn.commit()
    conn.close()
    return job_id
def update_job(job_id, **kwargs):
    conn = sqlite3.connect(DB_FILENAME)
    c = conn.cursor()
    fields = ", ".join(f"{k} = ?" for k in kwargs.keys())
    values = list(kwargs.values())
    values.append(job_id)
    c.execute(f"UPDATE jobs SET {fields} WHERE id = ?", values)
    conn.commit()
    conn.close()
def get_all_jobs():
    conn = sqlite3.connect(DB_FILENAME)
    c = conn.cursor()
    c.execute("SELECT * FROM jobs ORDER BY id DESC")
    jobs = c.fetchall()
    conn.close()
    return jobs
init_db()

# --- Global Variables for Scraping Job ---
progress = {
    "total_urls": 0,
    "downloaded": 0,
    "merged": 0,
    "state": "Idle",
    "error": "",
    "final_html": "",
    "pdf_filename": "",
    "compressed_pdf": "",
    "timeline": []
}
html_pages = []
resource_cache = {}
DISALLOWED_DOMAINS = ["googleads.g.doubleclick.net"]

# --- User-Agent List for Randomization ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
]
def get_random_user_agent():
    return random.choice(USER_AGENTS)

DEFAULT_HEADERS = {
    "User-Agent": get_random_user_agent(), # Use random User-Agent
    "Accept-Language": "en-US,en;q=0.9",  # Include Accept-Language
    "Accept-Encoding": "gzip, deflate, br", # Include Accept-Encoding
    "Connection": "keep-alive",            # Include Connection keep-alive
    "Upgrade-Insecure-Requests": "1",     # Request secure upgrades
    "Sec-Fetch-Dest": "document",          # Indicate document fetch
    "Sec-Fetch-Mode": "navigate",          # Indicate navigation mode
    "Sec-Fetch-Site": "none",              # Indicate no site context
    "Sec-Fetch-User": "?1",                # Indicate user navigation
}


# --- Google Drive Configuration ---
GOOGLE_CLIENT_CONFIG = {
    "installed": {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uris": [],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
}
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# --- Job Control Events ---
job_pause_event = Event()
job_cancel_event = Event()

# --- Helper: Timeline Logging ---
def add_timeline(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    msg = f"[{timestamp}] {message}"
    progress["timeline"].append(msg)
    if len(progress["timeline"]) > 100:
        progress["timeline"] = progress["timeline"][-100:]
    logging.debug(msg)

# --- Async helper to wait if paused ---
async def wait_if_paused():
    while job_pause_event.is_set():
        await asyncio.sleep(1)
        if job_cancel_event.is_set():
            raise Exception("Job Cancelled")

# --- Helper: Validate URL ---
def is_valid_url(url):
    disallowed_extensions = [
        ".zip", ".rar", ".7z", ".exe", ".mp4", ".avi", ".mov", ".wmv",
        ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".mp3", ".wav", ".flv",
        ".mkv", ".iso"
    ]
    parsed = urlparse(url)
    if parsed.scheme not in ["http", "https"]:
        return False
    path = parsed.path.lower()
    for ext in disallowed_extensions:
        if path.endswith(ext):
            return False
    return True

# --- Helper: Inline Resources ---
def inline_resources(html, page_url):
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("link", rel="stylesheet"):
        href = link.get("href")
        if href:
            full_url = urljoin(page_url, href)
            if any(domain in full_url for domain in DISALLOWED_DOMAINS):
                logging.debug(f"Skipping disallowed CSS URL: {full_url}")
                link.decompose()
                continue
            css_content = resource_cache.get(full_url)
            if css_content is None:
                try:
                    r = requests.get(full_url, headers=DEFAULT_HEADERS, timeout=30)
                    if r.status_code == 200:
                        css_content = r.text
                        resource_cache[full_url] = css_content
                    else:
                        logging.debug(f"Non-200 status for CSS {full_url}: {r.status_code}")
                        link.decompose()
                        continue
                except Exception as e:
                    logging.exception(f"Error downloading CSS {full_url}: {e}")
                    link.decompose()
                    continue
            if css_content:
                style_tag = soup.new_tag("style")
                style_tag.string = css_content
                link.replace_with(style_tag)
    for img in soup.find_all("img"):
        src = img.get("src")
        if src and not src.startswith("data:"):
            full_url = urljoin(page_url, src)
            if any(domain in full_url for domain in DISALLOWED_DOMAINS):
                logging.debug(f"Skipping disallowed image URL: {full_url}")
                img.decompose()
                continue
            img_data = resource_cache.get(full_url)
            if img_data is None:
                try:
                    r = requests.get(full_url, headers=DEFAULT_HEADERS, timeout=30)
                    if r.status_code == 200:
                        img_data = r.content
                        resource_cache[full_url] = img_data
                    else:
                        logging.debug(f"Non-200 status for image {full_url}: {r.status_code}")
                        img.decompose()
                        continue
                except Exception as e:
                    logging.exception(f"Error downloading image {full_url}: {e}")
                    img.decompose()
                    continue
            if img_data:
                mime = mimetypes.guess_type(full_url)[0] or "image/png"
                b64 = base64.b64encode(img_data).decode("utf-8")
                data_uri = f"data:{mime};base64,{b64}"
                img["src"] = data_uri
    return str(soup)

# --- Helper: Generate PDF using Playwright ---
async def generate_pdf_version(html_path, pdf_path):
    file_url = "file:///" + os.path.abspath(html_path)
    async with async_playwright() as p:
        # Launch browser with stealth arguments and potentially hide headless mode:
        browser = await p.chromium.launch(headless=True, args=[
            "--no-sandbox",  # often needed in docker/CI
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--no-first-run",
            "--no-zygote",
            "--single-process",
            "--disable-gpu",
            "--lang=en-US,en", # Set language
             # Try to hide headless - may not be fully effective
            '--disable-blink-features=AutomationControlled' # Important for stealth
        ])
        context = await browser.new_context()
        page = await context.new_page()
        await wait_if_paused()
        await page.goto(file_url, timeout=60000)
        await page.pdf(path=pdf_path, format="A4")
        await browser.close()

# --- Helper: Compress PDF ---
def compress_pdf(input_pdf, output_pdf):
    gs_command = shutil.which("gs")
    if gs_command is None:
        logging.error("Ghostscript not found. Please install Ghostscript for PDF compression.")
        return False
    cmd = [
        gs_command,
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        "-dPDFSETTINGS=/ebook",
        "-dNOPAUSE",
        "-dQUIET",
        "-dBATCH",
        f"-sOutputFile={output_pdf}",
        input_pdf
    ]
    try:
        subprocess.check_call(cmd)
        return True
    except subprocess.CalledProcessError as e:
        logging.exception(f"Ghostscript compression error: {e}")
        return False

# --- Process Pool for HTML Parsing ---
process_executor = concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count())
def extract_links(html, current_url, base_domain):
    new_links = []
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link["href"]
        abs_url = urljoin(current_url, href)
        if base_domain in urlparse(abs_url).netloc and is_valid_url(abs_url):
            new_links.append(abs_url)
    return new_links

# --- Revised Crawl Links Using aiohttp + ProcessPoolExecutor ---
async def crawl_links(root_url, base_domain, max_depth):
    visited = set()
    q = asyncio.Queue()
    await q.put((root_url, 0))
    timeout = aiohttp.ClientTimeout(total=10)
    headers = DEFAULT_HEADERS.copy() # Use default headers for crawling
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session: # Pass headers here
        async def worker():
            while True:
                try:
                    current_url, depth = await q.get()
                except asyncio.CancelledError:
                    break
                if current_url in visited or depth > max_depth:
                    q.task_done()
                    continue
                visited.add(current_url)
                try:
                    async with session.get(current_url) as response:
                        text = await response.text()
                    loop = asyncio.get_running_loop()
                    new_links = await loop.run_in_executor(
                        process_executor, extract_links, text, current_url, base_domain
                    )
                    for link in new_links:
                        if link not in visited:
                            await q.put((link, depth + 1))
                except Exception as e:
                    logging.exception(f"Error fetching {current_url}: {e}")
                q.task_done()
        workers_tasks = [asyncio.create_task(worker()) for _ in range(100)]
        await q.join()
        for w in workers_tasks:
            w.cancel()
    return list(visited)

# --- Updated Generate HTML Task with Retries and Stealth Context ---
async def generate_html_task(browser, url, temp_html_dir, semaphore, idx, retries=3):
    global progress
    result = None
    async with semaphore:
        for attempt in range(1, retries+1):
            context = None
            try:
                await wait_if_paused()
                # Create a new browser context with realistic settings AND stealth:
                context = await browser.new_context(
                    user_agent=get_random_user_agent(), # Randomized User-Agent for each context
                    viewport={"width": 1280, "height": 800},
                    ignore_https_errors=True,
                    # Add more human-like preferences, optional:
                    locale="en-US",
                    timezone_id="America/New_York" # Example timezone
                )
                await context.set_extra_http_headers({
                    "Accept-Language": "en-US,en;q=0.9"
                })
                page = await context.new_page()
                await stealth_async(page) # Apply stealth to this page
                await wait_if_paused()
                await page.goto(url, timeout=90000) # Increased timeout for slow pages/CDNs
                await page.wait_for_load_state("networkidle", timeout=90000) # Increased timeout
                # Add a small delay to simulate human reading time before getting content
                await asyncio.sleep(random.uniform(2, 5)) # Wait 2-5 seconds randomly
                title = await page.title()
                if not title:
                    title = url
                full_html = await page.content()
                inlined_html = await asyncio.to_thread(inline_resources, full_html, url)
                temp_filename = os.path.join(temp_html_dir, f"page_{idx}.html")
                with open(temp_filename, "w", encoding="utf-8") as f:
                    f.write(inlined_html)
                logging.debug(f"HTML downloaded for {url} with title: {title}")
                result = {"url": url, "title": title, "content": inlined_html, "order": idx}
                await page.close()
                await context.close()
                break  # Success, exit retry loop.
            except Exception as e:
                err = f"Attempt {attempt} - Error downloading HTML for {url}: {e}"
                progress["error"] = err
                add_timeline(err)
                logging.exception(err)
                if context is not None:
                    try:
                        await context.close()
                    except Exception:
                        pass
                await asyncio.sleep(5) # Wait before retrying to avoid rate limiting
        return result

def merge_html_pages(pages, output_path):
    head = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Merged HTML Book</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    .page-break { margin-top: 2rem; border-top: 2px dashed #ccc; padding-top: 2rem; }
    body { word-wrap: break-word; } /* Prevent horizontal overflow in PDF */
  </style>
</head>
<body>
<div class="container">
<h1>Merged HTML Book</h1>
"""
    body_parts = []
    for page in pages:
        section = f"""<div class="page-break">
  <h2>{page['title']}</h2>
  {page['content']}
</div>
"""
        body_parts.append(section)
    footer = """
</div>
</body>
</html>
"""
    final_html = head + "\n".join(body_parts) + footer
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_html)
    logging.debug(f"Merged HTML saved to {output_path}")

def get_base_domain(url):
    parsed = urlparse(url)
    return parsed.netloc

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

# --- Flask HTML Template ---
HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Website to HTML Book Generator</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
      body { padding-top: 2rem; background-color: #f8f9fa; }
      .container { max-width: 800px; }
      .progress { height: 25px; }
      .page-break { margin-top: 2rem; border-top: 2px dashed #ccc; padding-top: 2rem; }
      table { margin-top: 2rem; }
      #timeline { height: 200px; overflow-y: scroll; background: #fff; border: 1px solid #ccc; padding: 10px; }
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
        xhr.send(JSON.stringify({ url: url, workers: workers, depth: depth, chunk: chunk }));
      }
      function pauseJob() {
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "/pause", true);
        xhr.send();
      }
      function resumeJob() {
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "/resume", true);
        xhr.send();
      }
      function cancelJob() {
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "/cancel", true);
        xhr.send();
      }
      function updateProgress() {
        var xhr = new XMLHttpRequest();
        xhr.open("GET", "/progress", true);
        xhr.onload = function() {
          if (xhr.status === 200) {
            var data = JSON.parse(xhr.responseText);
            document.getElementById('total').innerText = data.total_urls;
            document.getElementById('downloaded').innerText = data.downloaded;
            document.getElementById('merged').innerText = data.merged;
            document.getElementById('state').innerText = data.state;
            var prog = document.getElementById('progressBar');
            if(data.total_urls > 0){
              var pct = Math.round((data.merged / data.total_urls) * 100);
              prog.style.width = pct + "%";
              prog.innerText = pct + "%";
            }
            if(data.final_html){
              var dlHtml = document.getElementById('downloadLinkHtml');
              dlHtml.href = "/download/" + data.final_html;
              dlHtml.style.display = "inline-block";
            }
            if(data.pdf_filename){
              var dlPdf = document.getElementById('downloadLinkPdf');
              dlPdf.href = "/download/" + data.pdf_filename;
              dlPdf.style.display = "inline-block";
            }
            if(data.compressed_pdf){
              var dlComp = document.getElementById('downloadLinkCompressedPdf');
              dlComp.href = "/download/" + data.compressed_pdf;
              dlComp.style.display = "inline-block";
            }
            if(data.error){
              var errDiv = document.getElementById('error');
              errDiv.innerText = data.error;
              errDiv.style.display = "block";
            }
            if(data.timeline) {
              document.getElementById('timeline').innerHTML = data.timeline.join("<br>");
            }
          }
        }
        xhr.send();
      }
      function updateJobsTable() {
        var xhr = new XMLHttpRequest();
        xhr.open("GET", "/jobs", true);
        xhr.onload = function() {
          if (xhr.status === 200) {
            document.getElementById("jobsTable").innerHTML = xhr.responseText;
          }
        }
        xhr.send();
      }
      setInterval(updateProgress, 1000);
      setInterval(updateJobsTable, 5000);
    </script>
  </head>
  <body>
    <div class="container">
      <h1 class="mb-4">Website to HTML Book Generator</h1>
      <div class="card mb-4">
        <div class="card-body">
          <form onsubmit="startScraping(); return false;">
            <div class="mb-3">
              <label for="url" class="form-label">Website URL:</label>
              <input type="text" id="url" name="url" class="form-control" required/>
            </div>
            <div class="mb-3">
              <label for="workers" class="form-label">Workers (parallel):</label>
              <input type="number" id="workers" name="workers" value="8" min="1" class="form-control" required/>
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
            <button type="button" class="btn btn-warning" onclick="pauseJob()">Pause</button>
            <button type="button" class="btn btn-success" onclick="resumeJob()">Resume</button>
            <button type="button" class="btn btn-danger" onclick="cancelJob()">Cancel</button>
          </form>
        </div>
      </div>
      <div class="mb-3">
        <h3>Progress</h3>
        <ul class="list-group mb-3">
          <li class="list-group-item">Total URLs Found: <span id="total">0</span></li>
          <li class="list-group-item">Pages Downloaded: <span id="downloaded">0</span></li>
          <li class="list-group-item">Pages Merged: <span id="merged">0</span></li>
          <li class="list-group-item">Current State: <span id="state">Idle</span></li>
        </ul>
        <div class="progress mb-3">
          <div id="progressBar" class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%;">0%</div>
        </div>
        <a id="downloadLinkHtml" href="#" class="btn btn-success" style="display: none;" download>Download Final HTML</a>
        <a id="downloadLinkPdf" href="#" class="btn btn-primary" style="display: none;" download>Download Original PDF</a>
        <a id="downloadLinkCompressedPdf" href="#" class="btn btn-secondary" style="display: none;" download>Download Compressed PDF</a>
      </div>
      <div class="card mb-3">
        <div class="card-header">Timeline</div>
        <div class="card-body" id="timeline"></div>
      </div>
      <h3>Previous Jobs</h3>
      <div id="jobsTable">
        {{ jobs_table|safe }}
      </div>
      <div id="error" class="alert alert-danger" style="display: none;"></div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  </body>
</html>
"""

# --- Flask Application ---
app = Flask(__name__)
@app.route("/")
def index():
    jobs = get_all_jobs()
    table_html = build_jobs_table(jobs)
    return render_template_string(HTML_TEMPLATE, jobs_table=table_html)
@app.route("/jobs")
def jobs():
    jobs = get_all_jobs()
    return build_jobs_table(jobs)
def build_jobs_table(jobs):
    table_html = """<table class="table table-striped">
    <thead><tr>
      <th>ID</th><th>Job Name</th><th>Start Time</th><th>Finish Time</th>
      <th>URL Count</th><th>HTML</th><th>Original PDF</th><th>Compressed PDF</th><th>Drive Link</th><th>Action</th>
    </tr></thead><tbody>"""
    for job in jobs:
        if len(job) == 10:
            (job_id, job_name, start_time, finish_time, url_count, html_file, pdf_file,
             drive_link, drive_token, status) = job
            compressed_pdf = ""
        else:
            (job_id, job_name, start_time, finish_time, url_count, html_file, pdf_file,
             compressed_pdf, drive_link, drive_token, status) = job
        html_dl = f'<a href="/download/{html_file}" class="btn btn-success btn-sm" download>HTML</a>' if html_file else ""
        pdf_dl = f'<a href="/download/{pdf_file}" class="btn btn-primary btn-sm" download>PDF</a>' if pdf_file else ""
        comp_dl = f'<a href="/download/{compressed_pdf}" class="btn btn-secondary btn-sm" download>Compressed PDF</a>' if compressed_pdf else ""
        drive_dl = f'<a href="{drive_link}" target="_blank" class="btn btn-info btn-sm">Drive</a>' if drive_link else "Not Uploaded"
        action_btn = ""
        if not drive_link and html_file:
            action_btn = f'<a href="/upload/{job_id}" class="btn btn-warning btn-sm">Upload to Drive</a>'
        table_html += f"<tr><td>{job_id}</td><td>{job_name}</td><td>{start_time}</td><td>{finish_time or ''}</td><td>{url_count or 0}</td><td>{html_dl}</td><td>{pdf_dl}</td><td>{comp_dl}</td><td>{drive_dl}</td><td>{action_btn}</td></tr>"
    table_html += "</tbody></table>"
    return table_html
@app.route("/start", methods=["POST"])
def start():
    data = request.get_json()
    url = data.get("url")
    workers = int(data.get("workers", 4))
    depth = int(data.get("depth", 1))
    chunk_size = int(data.get("chunk", 5))
    num_cores = os.cpu_count() or 1
    vm = psutil.virtual_memory()
    logging.debug(f"Detected CPU cores: {num_cores}, Total Memory: {vm.total/1024/1024:.2f} MB, Available: {vm.available/1024/1024:.2f} MB")
    if workers > num_cores:
        logging.debug(f"Reducing worker count from {workers} to {num_cores} based on CPU cores.")
        workers = num_cores
    logging.debug(f"Using chunk size: {chunk_size}")
    job_name = url
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job_id = insert_job(job_name, start_time, "Started")
    progress.update({
        "total_urls": 0,
        "downloaded": 0,
        "merged": 0,
        "state": "Starting",
        "error": "",
        "final_html": "",
        "pdf_filename": "",
        "compressed_pdf": "",
        "timeline": []
    })
    add_timeline("Job started. Initializing...")
    global html_pages
    html_pages = []
    resource_cache.clear()
    if os.path.exists("temp_html"):
        shutil.rmtree("temp_html")
    os.makedirs("temp_html", exist_ok=True)
    job_pause_event.clear()
    job_cancel_event.clear()
    threading.Thread(target=run_scraping, args=(url, workers, depth, chunk_size, job_id), daemon=True).start()
    return jsonify({"status": "started"})
@app.route("/pause", methods=["POST"])
def pause():
    job_pause_event.set()
    progress["state"] = "Paused"
    add_timeline("Job paused by user.")
    return jsonify({"status": "paused"})
@app.route("/resume", methods=["POST"])
def resume():
    job_pause_event.clear()
    progress["state"] = "Resumed"
    add_timeline("Job resumed by user.")
    return jsonify({"status": "resumed"})
@app.route("/cancel", methods=["POST"])
def cancel():
    job_cancel_event.set()
    progress["state"] = "Cancelled"
    add_timeline("Job cancelled by user.")
    try:
        if os.path.exists("temp_html"):
            shutil.rmtree("temp_html")
        resource_cache.clear()
        global html_pages
        html_pages = []
    except Exception as e:
        logging.exception("Error during cleanup on cancel:")
    return jsonify({"status": "cancelled"})
@app.route("/progress")
def get_progress():
    return jsonify(progress)
@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory("html", filename, as_attachment=True)
@app.route("/upload/<int:job_id>")
def upload(job_id):
    conn = sqlite3.connect(DB_FILENAME)
    c = conn.cursor()
    c.execute("SELECT html_file, drive_link FROM jobs WHERE id = ?", (job_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return "Job not found", 404
    html_file, drive_link = row
    if drive_link:
        return redirect(drive_link)
    redirect_uri = request.host_url.rstrip("/") + "/oauth2callback"
    client_config = {
        "installed": {
            "client_id": GOOGLE_CLIENT_CONFIG["installed"]["client_id"],
            "client_secret": GOOGLE_CLIENT_CONFIG["installed"]["client_secret"],
            "redirect_uris": [redirect_uri],
            "auth_uri": GOOGLE_CLIENT_CONFIG["installed"]["auth_uri"],
            "token_uri": GOOGLE_CLIENT_CONFIG["installed"]["token_uri"]
        }
    }
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    auth_url, state = flow.authorization_url(prompt="consent", access_type="offline")
    with open("oauth_state.txt", "w") as f:
        f.write(f"{state}:{job_id}")
    return redirect(auth_url)
@app.route("/oauth2callback")
def oauth2callback():
    state_code = request.args.get("state")
    code = request.args.get("code")
    try:
        with open("oauth_state.txt", "r") as f:
            stored = f.read().strip()
        stored_state, job_id = stored.split(":")
        job_id = int(job_id)
    except Exception as e:
        logging.exception("Error retrieving OAuth state:")
        return "Error retrieving OAuth state.", 500
    if state_code != stored_state:
        logging.error("State mismatch in OAuth callback.")
        return "State mismatch.", 400
    redirect_uri = request.host_url.rstrip("/") + "/oauth2callback"
    client_config = {
        "installed": {
            "client_id": GOOGLE_CLIENT_CONFIG["installed"]["client_id"],
            "client_secret": GOOGLE_CLIENT_CONFIG["installed"]["client_secret"],
            "redirect_uris": [redirect_uri],
            "auth_uri": GOOGLE_CLIENT_CONFIG["installed"]["auth_uri"],
            "token_uri": GOOGLE_CLIENT_CONFIG["installed"]["token_uri"]
        }
    }
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES, state=state_code)
    flow.redirect_uri = redirect_uri
    flow.fetch_token(code=code)
    credentials = flow.credentials
    drive_service = build("drive", "v3", credentials=credentials)
    conn = sqlite3.connect(DB_FILENAME)
    c = conn.cursor()
    c.execute("SELECT html_file FROM jobs WHERE id = ?", (job_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return "Job not found.", 404
    html_file = row[0]
    file_path = os.path.join("html", html_file)
    if not os.path.exists(file_path):
        return "File not found.", 404
    file_metadata = {"name": os.path.basename(file_path)}
    media = MediaFileUpload(file_path, mimetype="text/html")
    file_uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    drive_link = f"https://drive.google.com/uc?id={file_uploaded.get('id')}&export=download"
    update_job(job_id, drive_link=drive_link, drive_token=credentials.to_json())
    return redirect("/")
def run_scraping(root_url, workers, max_depth, chunk_size, job_id):
    try:
        asyncio.run(main_scraping(root_url, workers, max_depth, chunk_size, job_id))
    except Exception as e:
        progress["state"] = "Fatal Error"
        progress["error"] = str(e)
        add_timeline(f"Fatal error: {e}")
        logging.exception("Error in run_scraping:")
        update_job(job_id, status="Error")
async def main_scraping(root_url, workers, max_depth, chunk_size, job_id):
    global progress, html_pages
    progress["state"] = "Initializing"
    add_timeline("Initializing crawling process...")
    await wait_if_paused()
    base_domain = get_base_domain(root_url)
    downloads_dir = os.path.join("downloads", sanitize_filename(base_domain))
    os.makedirs(downloads_dir, exist_ok=True)
    html_dir = "html"
    os.makedirs(html_dir, exist_ok=True)
    temp_html_dir = os.path.join("temp_html")
    os.makedirs(temp_html_dir, exist_ok=True)
    progress["state"] = "Crawling links"
    add_timeline("Crawling links...")
    try:
        crawled_urls = await crawl_links(root_url, base_domain, max_depth)
    except Exception as e:
        progress["state"] = "Error during crawling"
        progress["error"] = str(e)
        add_timeline(f"Error during crawling: {e}")
        logging.exception("Error during crawl_links:")
        update_job(job_id, status="Error")
        return
    progress["total_urls"] = len(crawled_urls)
    add_timeline(f"Found {len(crawled_urls)} unique URLs.")
    logging.debug(f"Found {len(crawled_urls)} unique URLs.")
    progress["state"] = "Downloading HTML pages"
    add_timeline("Downloading HTML pages...")
    semaphore = asyncio.Semaphore(workers)
    total = len(crawled_urls)
    async with async_playwright() as p:
        # Launch browser with stealth arguments:
        browser = await p.chromium.launch(headless=True, args=[
            "--no-sandbox",  # often needed in docker/CI
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--no-first-run",
            "--no-zygote",
            "--single-process",
            "--disable-gpu",
            "--lang=en-US,en", # Set language
             # Try to hide headless - may not be fully effective
            '--disable-blink-features=AutomationControlled' # Important for stealth
        ])
        for start in range(0, total, chunk_size):
            await wait_if_paused()
            chunk_urls = crawled_urls[start:start+chunk_size]
            tasks = []
            for i, url in enumerate(chunk_urls, start=start):
                tasks.append(generate_html_task(browser, url, temp_html_dir, semaphore, i))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, dict):
                    html_pages.append(res)
                    progress["downloaded"] += 1
        await browser.close()
    html_pages.sort(key=lambda x: x["order"])
    progress["state"] = "Merging HTML pages"
    add_timeline("Merging HTML pages...")
    try:
        final_html_filename = f"{sanitize_filename(base_domain)}_book.html"
        final_html_path = os.path.join(html_dir, final_html_filename)
        merge_html_pages(html_pages, final_html_path)
        progress["final_html"] = final_html_filename
        pdf_filename = f"{sanitize_filename(base_domain)}_book.pdf"
        pdf_path = os.path.join(html_dir, pdf_filename)
        await generate_pdf_version(final_html_path, pdf_path)
        progress["pdf_filename"] = pdf_filename
        compressed_pdf_filename = f"{sanitize_filename(base_domain)}_book_compressed.pdf"
        compressed_pdf_path = os.path.join(html_dir, compressed_pdf_filename)
        if compress_pdf(pdf_path, compressed_pdf_path):
            progress["compressed_pdf"] = compressed_pdf_filename
            add_timeline("Compressed PDF generated.")
        else:
            add_timeline("Failed to generate compressed PDF.")
        progress["merged"] = progress["total_urls"]
        progress["state"] = "Completed. Final HTML and PDFs ready."
        add_timeline("Merging completed. Final HTML, original PDF, and compressed PDF generated.")
        logging.debug("HTML merging, PDF generation, and compression completed successfully.")
        finish_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_job(job_id, finish_time=finish_time, url_count=progress["total_urls"],
                   html_file=final_html_filename, pdf_file=pdf_filename, compressed_pdf=progress["compressed_pdf"], status="Completed")
    except Exception as e:
        progress["state"] = "Error during merging/PDF generation"
        progress["error"] = str(e)
        add_timeline(f"Error during merging/PDF generation: {e}")
        logging.exception("Error during merge_html_pages or generate_pdf_version:")
        update_job(job_id, status="Error")
if __name__ == "__main__":
    app.run(port=9999)