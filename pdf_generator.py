from playwright.sync_api import sync_playwright
import os

def generate_pdf_for_link(link, output_dir):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(link)
            page.wait_for_load_state("networkidle")
            pdf_path = os.path.join(output_dir, f"{os.path.basename(urlparse(link).path)}.pdf".replace("/", "_"))

            if not pdf_path.endswith(".pdf"):
                pdf_path += ".pdf"
            page.pdf(path=pdf_path, format="A4", print_background=True)
            browser.close()
            return pdf_path
    except Exception as e:
        print(f"Error generating PDF for {link}: {e}")
        return None