from mpi4py import MPI
import time
import logging
import boto3
import json

# Import necessary libraries for web crawling
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
# to follow website Parsing Rules (Robot.txt)
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

# Initialize queue
sqs = boto3.resource('sqs', region_name = 'eu-north-1')
toCrawl_queue = sqs.get_queue_by_name(QueueName = 'Queue1.fifo')
crawled_Queue = sqs.get_queue_by_name(QueueName = 'crawled_URLs.fifo')

logging.info(f"Connected to queue: {toCrawl_queue.url}")


def is_allowed_by_robots(url):
    """
    Check if the given URL is allowed to be crawled based on robots.txt
    """
    parsed_url = urlparse(url)
    robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"

    rp = RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()  # Fetch robots.txt
        return rp.can_fetch("*", url)  # "*" means our user-agent is any crawler
    except Exception as e:
        # If fetching robots.txt fails, we assume allowed
        print(f"Failed to fetch robots.txt from {robots_url}: {e}")
        return True



def send_task_to_queue(task_data, groupID, batch_size=500):
    """
    Sends a crawled URL to the SQS queue.
    :param task_data: A dictionary with task details.
    """

    if not isinstance(task_data, list):
        task_data = list(task_data)

    for i in range(0, len(task_data), batch_size):
        batch = task_data[i:i+batch_size]  # Slice the list into batches
        message_body = json.dumps(batch)  # Serialize the batch as JSON
        crawled_Queue.send_message(
            MessageBody=message_body,
            MessageGroupId="crawled_URLs",
        )
        logging.info(f"Sent batch of {len(batch)} URLs to SQS.")
        # Send message to the queue

    

def fetch_task_from_queue():
    """
    Fetches a crawling task from the SQS queue.
    :return: The task data (as a dictionary) or None if no messages are available.
    """
    messages = toCrawl_queue.receive_messages(
        MaxNumberOfMessages=1,  # Fetch one message at a time
        WaitTimeSeconds=10,     # Enable long polling to reduce empty responses
        VisibilityTimeout=30    # Lock the message for processing (avoid duplicate work)
    )

    if messages:
        message = messages[0]  # Get the first message
        task_data = json.loads(message.body)  # Decode the JSON message body
        
        logging.info(f"Task received: {task_data}")
        
        # Delete the message from the queue after successful processing
        message.delete()
        logging.info("Message deleted from the queue")
        return task_data
    else:
        logging.info("No tasks available in the queue")
        return None


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
        url = fetch_task_from_queue()
        if not url:  # Could be a shutdown signal (if you implement one)
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url}")

        try:
            # 1. Fetch the web page
            url_to_crawl = url['url']

            # Follow the Robot.txt Rules of the website
            if not is_allowed_by_robots(url_to_crawl):
                logging.info(f"Crawler {rank}: Skipping {url_to_crawl} due to robots.txt rules.")
                continue  # Skip forbidden URLs

            try:
                response = requests.get(url_to_crawl, timeout=5)
                response.raise_for_status()
                html = response.text
            except requests.RequestException as e:
                logging.error(f"Crawler {rank} failed fetching {url_to_crawl}: {e}")
                continue  # If fetching failed, skip this URL and move to next

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

            logging.info(f"Crawler {rank}: Extracted {len(extracted_urls)} URLs, {len(text)} characters of text.")
            print(f"Crawler {rank} done. Found {len(extracted_urls)} links.")

            time.sleep(2)  # Simulate crawling delay

            logging.info(f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")

            # --- Send extracted URLs back to master ---
            send_task_to_queue(extracted_urls, "crawled_URLs")




            # ***************** LATER PHASES 3,4,**********************************************
            # ---  Optionally send extracted content to indexer node (or queue for indexer) ---
            # indexer_rank = 1 + (rank - 1) % (size - 2) # Example: Send to indexer in round-robin (adjust indexer ranks accordingly)
            # comm.send(extracted_content, dest=indexer_rank, tag=2) # Tag 2 for sending content to indexer
            # comm.send(f"Crawler {rank} - Crawled URL: {url_to_crawl}", dest=0, tag=99)  # Send status update (tag 99)

        except Exception as e:
            logging.error(f"Crawler {rank} error crawling {url_to_crawl}: {e}")
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)  # Report error to master

if __name__ == '__main__':
    crawler_process()
