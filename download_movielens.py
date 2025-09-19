
"""
OOP refactor: MovieLensDownloader class for downloading and extracting MovieLens dataset.
Encapsulates download and extraction logic for maintainability.
"""
import os
import zipfile
import urllib.request

class MovieLensDownloader:
    def __init__(self, data_url=None, data_dir="data"):
        self.data_url = data_url or "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
        self.data_dir = data_dir
        self.zip_path = os.path.join(self.data_dir, "ml-latest-small.zip")
        self.extracted_dir = os.path.join(self.data_dir, "ml-latest-small")
        os.makedirs(self.data_dir, exist_ok=True)

    def download(self):
        if not os.path.exists(self.zip_path):
            print("Downloading MovieLens dataset...")
            urllib.request.urlretrieve(self.data_url, self.zip_path)
            print("Download complete.")
        else:
            print("Zip file already exists.")

    def extract(self):
        if not os.path.exists(self.extracted_dir):
            print("Extracting dataset...")
            with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.data_dir)
            print("Extraction complete.")
        else:
            print("Dataset already extracted.")

    def print_status(self):
        print(f"Done. Ratings: {self.extracted_dir}/ratings.csv, Movies: {self.extracted_dir}/movies.csv")

if __name__ == "__main__":
    downloader = MovieLensDownloader()
    downloader.download()
    downloader.extract()
    downloader.print_status()
