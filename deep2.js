const express = require('express');
const { URL } = require('url');
const puppeteer = require('puppeteer');
const sqlite3 = require('better-sqlite3');
const formidable = require('formidable');
const path = require('path');
const os = require('os');
const fs = require('fs').promises; // Use fs.promises for async operations, but for existsSync use the sync version
const fs_sync = require('fs'); // Import the synchronous fs module for existsSync
const { v4: uuidv4 } = require('uuid');
const EventEmitter = require('events');

const app = express();
const port = process.env.PORT || 3000;
const outputDir = path.join(os.tmpdir(), 'output'); // Use /tmp/output in production as in python
const databasePath = path.join(outputDir, 'website_pdfs.db');

// Ensure output directory exists
fs.mkdir(outputDir, { recursive: true }).catch(console.error);

// Initialize SQLite database
function initDB() {
    const db = new sqlite3(databasePath);
    db.exec(`
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
    `);
    db.close();
}

initDB();

// Global state for progress tracking - Using EventEmitter for simpler state management
const currentProcess = {
    active: false,
    progressEmitter: new EventEmitter(),
    outputFile: null,
    outputFilename: null,
    generatedFiles: [],
    crawledUrls: []
};

class Crawler {
    constructor(rootUrl, maxDepth = 3, excludeUrl = null) {
        this.rootUrl = rootUrl;
        this.maxDepth = maxDepth;
        this.visited = new Set();
        this.toVisit = [];
        this.baseUrl = new URL(rootUrl).hostname;
        this.rootPath = new URL(rootUrl).pathname || '/';
        if (!this.rootPath.endsWith('/')) this.rootPath += '/';
        if (!this.rootPath.startsWith('/')) this.rootPath = '/' + this.rootPath;
        this.excludeUrl = excludeUrl;
        this.visited.add(rootUrl);
        this.toVisit.push({ url: rootUrl, depth: 0 });
        this.crawledUrlsList = [];
    }

    isValidUrl(url) {
        // ... (same isValidUrl, extractLinks, crawlPage, crawl as before) ...
        try {
            const parsedUrl = new URL(url);
            if (parsedUrl.hostname !== this.baseUrl) {
                return false;
            }
            if (parsedUrl.hash) {
                return false;
            }
            let urlPath = parsedUrl.pathname || '/';
            if (!urlPath.startsWith('/')) urlPath = '/' + urlPath;
            if (!urlPath.startsWith(this.rootPath)) {
                return false;
            }

            if (this.excludeUrl && url.includes(this.excludeUrl)) {
                return false;
            }
            return !this.visited.has(url);
        } catch (error) {
            return false; // Invalid URL format
        }
    }

    extractLinks(url, html) {
        const { JSDOM } = require('jsdom');
        const dom = new JSDOM(html);
        const document = dom.window.document;
        const links = [];
        const anchors = document.querySelectorAll('a[href]');
        anchors.forEach(anchor => {
            try {
                const href = anchor.getAttribute('href');
                const fullUrl = new URL(href, url).href.split('#')[0]; // urljoin equivalent, remove fragment
                if (this.isValidUrl(fullUrl)) {
                    links.push(fullUrl);
                }
            } catch (e) {
                // Ignore invalid URLs
            }
        });
        return links;
    }

    async crawl(progressEmitter) {
        const orderedUrls = [];
        const browser = await puppeteer.launch({ headless: 'new' });
        const context = await browser.createBrowserContext();
        const pagePoolSize = Math.min(os.cpus().length * 2, 20); // Limit pool size like in Python
        const pagePool = [];
        for (let i = 0; i < pagePoolSize; i++) {
            pagePool.push(await context.newPage());
        }

        const tasks = new Set();
        let crawledCount = 0;

        while (this.toVisit.length > 0 || tasks.size > 0) {
            while (this.toVisit.length > 0 && tasks.size < pagePoolSize) {
                const { url, depth } = this.toVisit.shift();
                if (depth > this.maxDepth) {
                    continue;
                }
                const page = pagePool.pop();
                const task = this.crawlPage(url, depth, page, progressEmitter);
                tasks.add(task);
                task.finally(() => {
                    tasks.delete(task);
                    pagePool.push(page); // Return page to pool
                });
            }
            if (tasks.size > 0) {
                await Promise.race([...tasks]); // Wait for at least one task to complete
            } else {
                await new Promise(resolve => setTimeout(resolve, 100)); // Small delay if no tasks
            }
             progressEmitter.emit('progress', { current: this.visited.size, total: this.visited.size , message: `Crawling ${this.rootUrl}`}); // Emit progress
        }

        await browser.close();
        return [orderedUrls, this.crawledUrlsList]; // Return ordered and crawled URLs
    }


    async crawlPage(url, depth, page, progressEmitter) {
        try {
            // URL Encoding attempt:
            const encodedUrl = encodeURI(url); // Encode the URL
            console.log("Navigating to URL:", encodedUrl); // Debug: Check the URL being navigated
            await page.goto(encodedUrl, { waitUntil: 'networkidle2', timeout: 120000 });
            await page.waitForSelector('body', { timeout: 30000 });
            await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for JS render

            const html = await page.content();
            let links = [];
            if (depth < this.maxDepth) {
                links = this.extractLinks(url, html);
                progressEmitter.emit('message', `Depth ${depth}: Found ${links.length} links`); // Emit message
            }
             this.crawledUrlsList.push(url); // Add to crawled URLs list
            return [url, depth, links];

        } catch (error) {
            const errorMessage = `Error crawling ${url}: ${error.message}`;
            console.error(errorMessage);
            progressEmitter.emit('message', errorMessage); // Emit error message
            return [null, null, []];
        }
    }
}

// **Simplified generatePDF function for testing**
async function generatePDF({ index, url, tempDir, page }) {
    const pdfPath = path.join(tempDir, `page_${String(index).padStart(4, '0')}.pdf`);

    try {
        // Increased timeout, simplified - just goto and pdf
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 180000 }); // Increased timeout to 180s (3 min)
        await page.pdf({
            path: pdfPath,
            format: 'A3',
            printBackground: true,
            margin: { top: '10mm', right: '10mm', bottom: '10mm', left: '10mm' },
        });

        return { index, title: `Page ${index}`, success: true }; // Simplified title

    } catch (error) {
        const errorMessage = `[SIMPLIFIED generatePDF] Error generating PDF for ${url}: ${error.message}`; // MORE EXPLICIT LOGGING for simplified version
        console.error(errorMessage);
        progressEmitter.emit('message', errorMessage);
        return { index, title: '', success: false };
    }
}


async function mergePDFs(pdfFiles, outputPath, tempDir, progressEmitter) {
    const { PDFDocument, PDFPage, StandardFonts } = require('pdf-lib');

    const mergedPdf = await PDFDocument.create();
    const toc = [];

    for (const { index, title } of pdfFiles) {
        const pdfPath = path.join(tempDir, `page_${String(index).padStart(4, '0')}.pdf`);
        // Use fs_sync.existsSync (the synchronous version) here - this is the correct usage for existsSync
        if (!fs_sync.existsSync(pdfPath)) continue;

        const sourcePdfBytes = await fs.readFile(pdfPath);
        try {
          const sourcePdfDoc = await PDFDocument.load(sourcePdfBytes);
          const pageCount = sourcePdfDoc.getPages().length;
          const copiedPages = await mergedPdf.copyPages(sourcePdfDoc, sourcePdfDoc.getPages());
          copiedPages.forEach(page => mergedPdf.addPage(page));
          toc.push([1, title, mergedPdf.getPages().length - pageCount + 1]);
        } catch (error) {
          console.error(`Error loading PDF for merging: ${pdfPath}`, error);
          progressEmitter.emit('message', `Error merging PDF page_${String(index).padStart(4, '0')}.pdf - potentially corrupt.`);
        }
        await fs.unlink(pdfPath).catch(e => console.error(`Error deleting temp file: ${pdfPath}`, e)); // Delete temp file after merge

        progressEmitter.emit('progress', { message: `Merging page: ${title}`, current: pdfFiles.indexOf({index, title}) + 1, total: pdfFiles.length});
    }


    const tocPage = mergedPdf.insertPage(0); // Insert TOC as the first page

    // Generate simple TOC text (You can customize TOC formatting here)
    let y = tocPage.getHeight() - 100;
    const fontSize = 18;
    const font = await mergedPdf.embedFont(StandardFonts.TimesRoman); // Or any suitable font
    tocPage.drawText('Table of Contents', { x: 50, y, font, size: fontSize + 4 });
    y -= 40;

    for (const [level, title, pageNum] of toc) {
        const tocEntry = `${title}  ......................... ${pageNum}`;
        tocPage.drawText(tocEntry, { x: 50, y, font, size: fontSize });
        y -= 25;
    }

    const pdfBytes = await mergedPdf.save();
    await fs.writeFile(outputPath, pdfBytes);
    console.log(`Merged PDF saved to ${outputPath}`);
}

// ... (rest of savePdfInfoToDB, getPdfFilesFromDB, runConversion, Express routes - same as in deep4.js, just replace generatePDF function with the simplified version above) ...

function savePdfInfoToDB(filename, filepath, downloadUrl, websiteUrl, totalLinksCrawled, successfulPages, failedPages) {
    const db = new sqlite3(databasePath);
    try {
        const stmt = db.prepare(`
            INSERT INTO pdf_files (filename, filepath, download_url, website_url, total_links_crawled, successful_pages, failed_pages)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `);
        stmt.run(filename, filepath, downloadUrl, websiteUrl, totalLinksCrawled, successfulPages, failedPages);
    } catch (e) {
        console.warn(`Error saving PDF info to DB for ${filename}`, e.message); // Log error instead of throwing
    } finally {
        db.close();
    }
}

function getPdfFilesFromDB() {
    const db = new sqlite3(databasePath);
    const files = db.prepare(`
        SELECT filename, download_url, website_url, conversion_timestamp, total_links_crawled, successful_pages, failed_pages
        FROM pdf_files
        ORDER BY conversion_timestamp DESC
    `).all();
    db.close();
    return files;
}

async function runConversion(url, maxDepth, workers, outputPath, excludeUrl, progressEmitter) {
    const tempDir = path.join(os.tmpdir(), 'temp_pages', uuidv4()); // Unique temp dir for each conversion
    await fs.mkdir(tempDir, { recursive: true });
    let crawledUrlsListForDisplay = [];
    let urls = []; // Define urls here to be accessible in finally block

    const startTime = Date.now();
    let successCount = 0;
    let failedCount = 0;
    try {
        const crawler = new Crawler(url, maxDepth, excludeUrl);
        [urls, crawled_urls_list] = await crawler.crawl(progressEmitter); // Pass progress emitter to crawler
        crawledUrlsListForDisplay = crawled_urls_list;
        currentProcess.crawledUrls = crawledUrlsListForDisplay; // Update crawled URLs list


        const browser = await puppeteer.launch({ headless: 'new' });
        const context = await browser.createBrowserContext();
        const pagePoolSize = Math.min(workers > 0 ? workers : os.cpus().length * 2, 20);
        const pagePool = [];
        for (let i = 0; i < pagePoolSize; i++) {
            pagePool.push(await context.newPage());
        }


        const tasks = urls.map((url, index) => {
            return generatePDF({ index: index + 1, url, tempDir, page: pagePool.pop() });
        });

        const results = [];
        let completedTasks = 0;

        for (const taskPromise of tasks) {
            const result = await taskPromise;
            results.push(result);
            pagePool.push(await context.newPage()); // Replenish page pool

            if (result.success) {
                successCount++;
            } else {
                failedCount++;
            }
            completedTasks++;
             progressEmitter.emit('progress', { message: `Generating PDF ${result.title || 'Page'}`, current: completedTasks, total: urls.length }); // Emit PDF gen progress
        }
        await browser.close();

        const successful = results.filter(res => res.success).map(res => ({ index: res.index, title: res.title })); // Keep title for TOC

        if (successful.length > 0) {
             progressEmitter.emit('message', 'Merging PDFs...');
            await mergePDFs(successful, outputPath, tempDir, progressEmitter);
            currentProcess.outputFile = outputPath;

            savePdfInfoToDB(
                currentProcess.outputFilename,
                currentProcess.outputFile,
                `/download?filename=${currentProcess.outputFilename}`,
                url,
                urls.length,
                successCount,
                failedCount
            );
        } else {
            throw new Error("No pages converted to PDF successfully!");
        }


    } catch (error) {
        progressEmitter.emit('error', error.message);
        console.error("Conversion error during crawl or PDF generation:", error); // More specific error log
    } finally {
        const endTime = Date.now();
        const conversionDuration = (endTime - startTime) / 1000;
        console.log(`Conversion for ${url} took ${conversionDuration.toFixed(2)} seconds. Successful pages: ${successCount}, Failed pages: ${failedCount}, Total Links Crawled: ${urls ? urls.length : 'Error in crawl - urls undefined'}`); // Check if urls is defined

        try {
             await fs.rm(tempDir, { recursive: true, force: true }); // Clean up temp dir
        } catch (e) {
            console.error(`Error cleaning up temp dir: ${tempDir}`, e);
        }
    }
}

// Express routes (same as before - copy from your deep4.js)

app.use(express.static(path.join(__dirname, 'public'))); // Serve static files from 'public'


app.get('/', (req, res) => {
    const pdfFiles = getPdfFilesFromDB();
    res.sendFile(path.join(__dirname, 'public', 'index.html')); // Or render a template if you use one
    // if you are rendering index.html from node directly and not from separate html file
    // res.send(indexHTML); // Replace indexHTML with your HTML content from index.html, potentially using template engine.
});


app.post('/convert', async (req, res) => {
    if (currentProcess.active) {
        return res.status(400).json({ status: 'error', message: 'A process is already running' });
    }

    currentProcess.active = true;
    currentProcess.outputFile = null;
    currentProcess.outputFilename = null;
    currentProcess.generatedFiles = [];
    currentProcess.crawledUrls = []; // Reset crawled urls
    currentProcess.progressEmitter = new EventEmitter(); // New emitter for each conversion

    const form = new formidable.IncomingForm({});

    form.parse(req, async (err, fields, files) => {
        if (err) {
            currentProcess.active = false;
            return res.status(500).json({ status: 'error', message: 'Error parsing form data.' });
        }

        // Ensure filenameInput is treated as a string
        let filenameInput = fields.filename;
        if (Array.isArray(filenameInput)) {
            filenameInput = filenameInput[0]; // Take the first element if it's an array
        }
        filenameInput = filenameInput || 'output'; // Default value if still null or undefined after array check

        const filename = filenameInput.replace(/[^a-zA-Z0-9_-]/g, '_'); // Secure filename - basic sanitization
        const outputFilename = `${filename}.pdf`;
        const outputPath = path.join(outputDir, outputFilename);
        const excludeUrl = fields.exclude_url || null;
        const workersInput = parseInt(fields.workers, 10) || 2;

        currentProcess.outputFilename = outputFilename;
        currentProcess.outputFile = outputPath;

        currentProcess.progressEmitter.emit('crawling_start'); // Signal crawling start

        async function run() {
            try {
                await runConversion(
                    fields.url,
                    parseInt(fields.depth, 10),
                    workersInput,
                    outputPath,
                    excludeUrl,
                    currentProcess.progressEmitter // Pass the emitter
                );

                if (currentProcess.outputFile && fs_sync.existsSync(currentProcess.outputFile)) { // Use fs_sync.existsSync here as well, for consistency. Although fs.existsSync should also work (it is sync).
                    currentProcess.generatedFiles = [{
                        filename: currentProcess.outputFilename,
                        download_url: `/download?filename=${currentProcess.outputFilename}`
                    }];
                } else {
                    currentProcess.generatedFiles = [];
                }

                currentProcess.progressEmitter.emit('complete', { files: currentProcess.generatedFiles, crawledUrls: currentProcess.crawledUrls }); // Send crawled URLs on complete

            } catch (error) {
                currentProcess.progressEmitter.emit('error', { message: error.message });
            } finally {
                currentProcess.active = false;
            }
        }

        run(); // Start conversion process asynchronously

        res.json({ status: 'started' }); // Immediate response to frontend
    });
});


app.get('/progress', (req, res) => {
    // ... (same progress, files, download routes as before - copy from deep4.js) ...
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    let messagesCount = 0; // Counter for messages
    res.write(`data: ${JSON.stringify({ type: 'crawling_urls_init' })}\n\n`); // Initial signal

    const progressListener = (progressData) => {
        res.write(`data: ${JSON.stringify({ type: 'progress', ...progressData })}\n\n`);
    };

    const messageListener = (messageText) => {
        messagesCount++;
        const messagePayload = { type: 'message', message: messageText, count: messagesCount };
        res.write(`data: ${JSON.stringify(messagePayload)}\n\n`);
    };
    const crawlingStartListener = () => {
        res.write(`data: ${JSON.stringify({ type: 'crawling_start' })}\n\n`);
    };

    const completeListener = (completionData) => {
        completionData.crawledUrls = currentProcess.crawledUrls; // Attach crawled urls
        res.write(`data: ${JSON.stringify({ type: 'complete', ...completionData })}\n\n`);
        cleanup();
    };

    const errorListener = (errorData) => {
        res.write(`data: ${JSON.stringify({ type: 'error', message: errorData.message || errorData })}\n\n`);
        cleanup();
    };


    currentProcess.progressEmitter.on('progress', progressListener);
    currentProcess.progressEmitter.on('message', messageListener);
    currentProcess.progressEmitter.on('crawling_start', crawlingStartListener);
    currentProcess.progressEmitter.on('complete', completeListener);
    currentProcess.progressEmitter.on('error', errorListener);


    const cleanup = () => {
        currentProcess.progressEmitter.off('progress', progressListener);
        currentProcess.progressEmitter.off('message', messageListener);
        currentProcess.progressEmitter.off('crawling_start', crawlingStartListener);
        currentProcess.progressEmitter.off('complete', completeListener);
        currentProcess.progressEmitter.off('error', errorListener);
        res.end();
    };

    req.on('close', () => {
        cleanup();
    });
});


app.get('/files', (req, res) => {
    const pdfFiles = getPdfFilesFromDB();
    const filesList = pdfFiles.map(fileData => ({
        filename: fileData.filename,
        download_url: fileData.download_url,
        website_url: fileData.website_url,
        conversion_timestamp: fileData.conversion_timestamp,
        total_links_crawled: fileData.total_links_crawled,
        successful_pages: fileData.successful_pages,
        failed_pages: fileData.failed_pages
    }));
    res.json({ files: filesList });
});

app.get('/download', async (req, res) => {
    const filename = req.query.filename;
    if (!filename) {
        return res.status(400).json({ status: 'error', message: 'Filename not provided' });
    }

    const filePath = path.join(outputDir, filename);
    if (!fs_sync.existsSync(filePath)) { // Use fs_sync.existsSync here as well for consistency.
        return res.status(404).json({ status: 'error', message: 'File not found' });
    }

    res.download(filePath, filename, (err) => {
        if (err) {
            console.error("Error during file download:", err);
            if (!res.headersSent) { // Prevent sending error response twice if headers already sent by res.download
                res.status(500).json({ status: 'error', message: 'Error downloading file.' });
            }
        }
    });
});


app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
});