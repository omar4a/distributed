from mpi4py import MPI
import time
import logging

# Import necessary libraries for web crawling (e.g., requests, beautifulsoup4, scrapy), parsing, etc.
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')


def crawler_process():
    """
    Process for a crawler node.
    Fetches web pages, extracts URLs, and sends results back to the master.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Crawler node started with rank {rank} of {size}")

    while True:
        status = MPI.Status()
        url_to_crawl = comm.recv(source=0, tag=0, status=status)  # Receive URL from master (tag 0)
        if not url_to_crawl:  # Could be a shutdown signal (if you implement one)
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")

        try:
            # 1. Fetch the web page
            response = requests.get(url_to_crawl, timeout=5)
            response.raise_for_status()
            html = response.text

            # 2. Parse the HTML
            soup = BeautifulSoup(html, "html.parser")

            # 3. Extract new URLs
            extracted_urls = set()
            for link in soup.find_all('a', href=True):
                full_url = urljoin(url_to_crawl, link['href'])
                if urlparse(full_url).scheme in ['http', 'https']:
                    extracted_urls.add(full_url)

            # 4. Extract text content
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text(separator=' ', strip=True)

            logging.info(f" Crawler {rank}: Extracted {len(extracted_urls)} URLs, {len(text)} characters of text.")
            print(f" Crawler {rank} done. Found {len(extracted_urls)} links.")

            time.sleep(2)  # Simulate crawling delay


            logging.info(f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")

            # --- Send extracted URLs back to master ---
            comm.send(extracted_urls, dest=0, tag=1)  # Tag 1 for sending extracted URLs




            # ***************** LATER PHASES 3,4,**********************************************
            # ---  Optionally send extracted content to indexer node (or queue for indexer) ---
            # indexer_rank = 1 + (rank - 1) % (size - 2) # Example: Send to indexer in round-robin (adjust indexer ranks accordingly)
            # comm.send(extracted_content, dest=indexer_rank, tag=2) # Tag 2 for sending content to indexer
            comm.send(f"Crawler {rank} - Crawled URL: {url_to_crawl}", dest=0, tag=99)  # Send status update (tag 99)

        except Exception as e:

            logging.error(f"Crawler {rank} error crawling {url_to_crawl}: {e}")
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)  # Report error to master (tag 999)


if __name__ == '__main__':
    crawler_process()