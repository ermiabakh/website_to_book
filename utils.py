import os
from urllib.parse import urlparse

def clean_up_temp_files(file_paths):
    for file_path in file_paths:
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

def get_domain_from_url(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc