import requests

class FileDownloader:
    def __init__(self):
        pass    

    def download_file(self, url: str, destination: str) -> None:
        print(f"Downloading from {url} to {destination}")

        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)