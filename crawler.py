from mpi4py import MPI
import time
import logging
import boto3
import json

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

# Initialize SQS queues
sqs = boto3.resource('sqs', region_name='eu-north-1')
toCrawl_queue = sqs.get_queue_by_name(QueueName='Queue1.fifo')
crawled_Queue = sqs.get_queue_by_name(QueueName='crawled_URLs.fifo')
content_queue = sqs.get_queue_by_name(QueueName='crawled_content.fifo') 

# saving crawled
import hashlib
from urllib.parse import quote_plus

logging.info(f"Connected to queue: {toCrawl_queue.url}")


def save_to_s3(url, html, text):
    """
    Saves crawled HTML and extracted text to S3.
    """
    bucket_name = 'crawled-content-team29'
    s3 = boto3.client('s3', region_name='eu-north-1')

    # Generate a safe file name by hashing the URL
    safe_url = quote_plus(url)  # Encodes unsafe characters into URL-safe form
    base_key = f"{safe_url}/"

    try:
        s3.put_object(Bucket=bucket_name, Key=base_key + "raw.html", Body=html, ContentType='text/html')
        s3.put_object(Bucket=bucket_name, Key=base_key + "content.txt", Body=text, ContentType='text/plain')
        logging.info(f"Saved crawled content for {url} to S3 bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Failed to save {url} to S3: {e}")


def is_allowed_by_robots(url):
    parsed_url = urlparse(url)
    robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"

    rp = RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch("*", url)
    except Exception as e:
        logging.warning(f"Failed to fetch robots.txt from {robots_url}: {e}")
        return True


def send_task_to_queue(task_data, groupID, batch_size=500):
    if not isinstance(task_data, list):
        task_data = list(task_data)

    for i in range(0, len(task_data), batch_size):
        batch = task_data[i:i + batch_size]
        message_body = json.dumps(batch)
        crawled_Queue.send_message(
            MessageBody=message_body,
            MessageGroupId="crawled_URLs",
        )
        logging.info(f"Sent batch of {len(batch)} URLs to SQS.")


def fetch_task_from_queue():
    messages = toCrawl_queue.receive_messages(
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
        VisibilityTimeout=30
    )

    if messages:
        message = messages[0]
        task_data = json.loads(message.body)
        logging.info(f"Task received: {task_data}")
        message.delete()
        logging.info("Message deleted from the queue")
        return task_data
    else:
        logging.info("No tasks available in the queue")
        return None


def send_content_to_indexer(content):
    try:
        content_queue.send_message(
            MessageBody=json.dumps(content),
            MessageGroupId="crawled_content"
        )
        logging.info(f"Sent content for indexing: {content.get('url')}")
    except Exception as e:
        logging.error(f"Failed to send content to indexer: {e}")

def crawler_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Crawler node started with rank {rank} of {size}")

    total_urls_processed = 0
    error_count = 0
    skipped_due_to_robots = 0

    while True:
        url = fetch_task_from_queue()
        if not url:
            logging.info(f"Crawler {rank} received shutdown signal or no task. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url}")

        try:
            url_to_crawl = url['url']

            if not is_allowed_by_robots(url_to_crawl):
                logging.info(f"Crawler {rank}: Skipping {url_to_crawl} due to robots.txt rules.")
                skipped_due_to_robots += 1
                total_urls_processed += 1
                # comm.send(f"Skipped (robots.txt): {url_to_crawl}", dest=0, tag=98)
                continue

            try:
                response = requests.get(url_to_crawl, timeout=5)
                response.raise_for_status()
                html = response.text
            except requests.RequestException as e:
                logging.error(f"Crawler {rank} failed fetching {url_to_crawl}: {e}")
                error_count += 1
                total_urls_processed += 1
                continue

            soup = BeautifulSoup(html, "html.parser")

            extracted_urls = set()
            for link in soup.find_all('a', href=True):
                full_url = urljoin(url_to_crawl, link['href'])
                if urlparse(full_url).scheme in ['http', 'https']:
                    extracted_urls.add(full_url)

            for script in soup(["script", "style"]):
                script.decompose()

            text = soup.get_text(separator=' ', strip=True)

            # Save to S3 for persistence
            save_to_s3(url_to_crawl, html, text)

            logging.info(f"Crawler {rank}: Extracted {len(extracted_urls)} URLs, {len(text)} characters of text.")
            print(f"Crawler {rank} done. Found {len(extracted_urls)} links.")

            time.sleep(2)  # Simulate delay

            logging.info(f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")
            send_task_to_queue(extracted_urls, "crawled_URLs")

            total_urls_processed += 1

            message = {
                "url": url_to_crawl,
                "html": html,
                "title": soup.title.string if soup.title else url_to_crawl
            }
            send_content_to_indexer(message)


        except Exception as e:
            logging.error(f"Crawler {rank} error crawling {url_to_crawl}: {e}")
            error_count += 1
            total_urls_processed += 1
            # comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)

    # Final summary
    error_percentage = (error_count / total_urls_processed) * 100 if total_urls_processed else 0
    summary_message = (
        f"Crawler {rank} finished. "
        f"Processed: {total_urls_processed}, "
        f"Errors: {error_count} ({error_percentage:.2f}%), "
        f"Skipped (robots.txt): {skipped_due_to_robots}"
    )

    logging.info(summary_message)
    # comm.send(summary_message, dest=0, tag=100)


if __name__ == '__main__':
    crawler_process()