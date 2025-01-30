import fitz  # PyMuPDF
from tqdm import tqdm

def merge_pdfs(pdf_paths, output_path):
    merged_doc = fitz.open()

    for pdf_path in tqdm(pdf_paths, desc="Merging PDFs"):
        try:
            doc = fitz.open(pdf_path)
            merged_doc.insert_pdf(doc)
        except Exception as e:
            print(f"Error merging {pdf_path}: {e}")

    merged_doc.save(output_path)
    merged_doc.close()