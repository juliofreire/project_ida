import requests
import os
from urllib.parse import urlparse, unquote


# API_BASE_URL = "https://dados.gov.br/api/publico/conjuntos-dados/indice-desempenho-atendimento"

# DOWNLOAD_PATH = os.path.join("../data", "raw")


class Extractor:
    """
    Extractor class to handle the extraction of file links from the API and downloading them.
    """

    def __init__(self, api_base_url, download_path):
        self.api_base_url = api_base_url
        self.download_path = download_path
        os.makedirs(download_path, exist_ok=True)


    def fix_url(self, url):
        """
        Fix the URL by unquoting it and ensuring it is properly formatted.
        """
        return url.replace("\\", "/")


    def get_filename_from_url(self, url):
        """
        Extract the filename from a URL.
        """
        return unquote(os.path.basename(urlparse(url).path)) 


    def fetch_file_links(self):
        """
        Fetch the list of file links from the API.
        """
        response = requests.get(self.api_base_url)
        response.raise_for_status()  # Raise an error for bad responses

        data = response.json()
        resources = data.get("resources", [])
        links = []

        for resource in resources:
            raw_url = resource.get("recursoForm", {}).get("link") # Navigate to the correct field and fetch the link
            if raw_url:
                fixed_url = self.fix_url(raw_url)
                links.append(fixed_url)                
        return links


    def download_file(self, url):
        """Download a file from a URL to a local path."""

        filename = self.get_filename_from_url(url)
        file_path = os.path.join(self.download_path, filename)

        if os.path.exists(file_path):
            print(f"File {filename} already exists. Skipping download.")
            return file_path

        print(f"Downloading {filename} from {url}...")
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an error for bad responses

            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(f"Downloaded {filename} to {file_path}.")
        except requests.RequestException as e:
            print(f"Failed to download {filename} from {url}. Error: {e}")
            return None


    def run(self):
        """
        Run the extractor to fetch and download files.
        """
        links = self.fetch_file_links()
        if not links:
            print("No file links found.")
            return

        for link in links:
            self.download_file(link)


    
    