import os
import time
import concurrent.futures
from crawler import crawl_links
from pdf_generator import generate_pdf_for_link
from pdf_merger import merge_pdfs
from utils import clean_up_temp_files, get_domain_from_url
from tqdm import tqdm

def main():
    start_time = time.time()

    root_url = "https://react.dev"  # Replace with your target website
    domain = get_domain_from_url(root_url)
    output_dir = "output"
    final_pdf_name = f"{domain}_book.pdf"
    max_workers = 4  # Adjust based on your CPU cores

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # --- Crawling with Progress ---
    print(f"Crawling links from {root_url}...")
    all_links = crawl_links(root_url)  # Get all links (no progress bar here yet)
    num_links = len(all_links)
    print(f"Found {num_links} unique links.")

    # --- PDF Generation with Progress ---
    print("Generating PDFs...")
    pdf_paths = []
    successful_pdfs = 0

    with tqdm(total=num_links, desc="Generating PDFs", unit="link") as progress_bar:
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(generate_pdf_for_link, link, output_dir): link for link in all_links}

            for future in concurrent.futures.as_completed(futures):
                link = futures[future]
                try:
                    pdf_path = future.result()
                    if pdf_path:  # Check if PDF generation was successful
                        pdf_paths.append(pdf_path)
                        successful_pdfs += 1
                    progress_bar.set_postfix({"success": successful_pdfs, "total": num_links}) # Update postfix info
                except Exception as e:
                    print(f"Error generating PDF for {link}: {e}")
                progress_bar.update(1)  # Update progress by 1 for each completed link

    # --- Merging with Progress ---
    print("Merging PDFs...")
    merge_pdfs(pdf_paths, os.path.join(output_dir, final_pdf_name))

    # Optional: Clean up temporary PDF files
    clean_up_temp_files(pdf_paths)

    end_time = time.time()
    print(f"Book generation completed in {end_time - start_time:.2f} seconds.")
    print(f"Final PDF saved to: {os.path.join(output_dir, final_pdf_name)}")

if __name__ == "__main__":
    main()