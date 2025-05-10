import time
import logging
import boto3
import json
import uuid
import asyncio


from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, quote_plus
from urllib.robotparser import RobotFileParser

from requests_html import HTMLSession  # For dynamic HTML and JS rendering

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

# Initialize SQS queues
sqs = boto3.resource('sqs', region_name='eu-north-1')
toCrawl_queue = sqs.get_queue_by_name(QueueName='Queue1.fifo')
crawled_Queue = sqs.get_queue_by_name(QueueName='crawled_URLs.fifo')
content_queue = sqs.get_queue_by_name(QueueName='crawled_content.fifo')


def fetch_rendered_html(url):
    # Ensure the current thread has an event loop
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    session = HTMLSession()
    try:
        # Get the URL with a 15-second timeout
        response = session.get(url, timeout=15)
        # Asynchronously render the dynamic content with a 15s overall timeout.
        # Using arender (the async version) wrapped in asyncio.wait_for.
        loop.run_until_complete(
            asyncio.wait_for(response.html.arender(timeout=15, sleep=1), timeout=15)
        )
        # Check the page readyState (allows "interactive" or "complete")
        ready_state = loop.run_until_complete(
            response.html.page.evaluate("document.readyState")
        )
        if ready_state not in ["interactive", "complete"]:
            logging.info(f"{url}: readyState is '{ready_state}' but proceeding anyway.")
        return response.html.html
    except asyncio.TimeoutError:
        logging.error(f"Timeout: Rendering {url} exceeded 15s. Aborting and resetting the browser.")
        session.close()  # Reset the underlying browser (Chromium) instance
        return ""
    except Exception as e:
        logging.error(f"Failed rendering {url}: {e}")
        session.close()
        return ""

def save_to_s3(url, html, text):
    """
    Saves crawled HTML and extracted text to S3.
    """
    bucket_name = 'crawled-content-team29'
    s3 = boto3.client('s3', region_name='eu-north-1')
    # Generate a safe file name by encoding the URL
    safe_url = quote_plus(url)
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
            MessageGroupId=str(uuid.uuid4())
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
        return None

def send_content_to_indexer(content):
    try:
        content_queue.send_message(
            MessageBody=json.dumps(content),
            MessageGroupId=str(uuid.uuid4())
        )
        logging.info(f"Sent content for indexing: {content.get('url')}")
    except Exception as e:
        logging.error(f"Failed to send content to indexer: {e}")

def crawler_process():
    shutdown_queue = sqs.get_queue_by_name(QueueName='shutdown.fifo')

    total_urls_processed = 0
    error_count = 0
    skipped_due_to_robots = 0

    printed = False

    while True:
        task = fetch_task_from_queue()
        if task is None:
            if not printed:
                logging.info("Task queue is empty. Polling indefinitely.")
                printed = True
            # Poll the shutdown queue if no tasks are available
            shutdown_msgs = shutdown_queue.receive_messages(
                MaxNumberOfMessages=1,
                WaitTimeSeconds=2
            )
            if shutdown_msgs:
                shutdown_data = json.loads(shutdown_msgs[0].body)
                if shutdown_data.get("status") == "finished":
                    logging.info("Shutdown message received on shutdown.fifo. Exiting crawler.")
                    break
            time.sleep(2)
            continue

        printed = False

        try:
            url_to_crawl = task['url']
            current_depth = task.get('current_depth', 0)
            max_depth = task.get('max_depth', 0)

            if not is_allowed_by_robots(url_to_crawl):
                logging.info(f"Crawler: Skipping {url_to_crawl} due to robots.txt rules.")
                skipped_due_to_robots += 1
                total_urls_processed += 1
                continue

            html = fetch_rendered_html(url_to_crawl)
            if not html:
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
            save_to_s3(url_to_crawl, html, text)

            logging.info(f"Crawler: Extracted {len(extracted_urls)} URLs, {len(text)} characters of text.")
            print(f"Crawler done. Found {len(extracted_urls)} links.")
            logging.info(f"Crawler crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")

            if current_depth < max_depth:
                new_tasks = [{"url": link, "current_depth": current_depth + 1, "max_depth": max_depth} for link in extracted_urls]
                send_task_to_queue(new_tasks, "crawled_URLs")
            else:
                logging.info(f"Crawler: Reached max depth for {url_to_crawl}. Not sending further URL tasks.")
                sqs_resource = boto3.resource('sqs', region_name='eu-north-1')
                crawler_done_queue = sqs_resource.get_queue_by_name(QueueName='crawler_completion.fifo')
                done_message = {"status": "done"}
                crawler_done_queue.send_message(MessageBody=json.dumps(done_message), MessageGroupId=str(uuid.uuid4()))
                logging.info("Crawler finished processing and sent done message.")

            total_urls_processed += 1
            message = {
                "url": url_to_crawl,
                "html": html,
                "title": soup.title.string if soup.title else url_to_crawl
            }
            send_content_to_indexer(message)

        except Exception as e:
            logging.error(f"Crawler error crawling {url_to_crawl}: {e}")
            error_count += 1
            total_urls_processed += 1

    error_percentage = (error_count / total_urls_processed) * 100 if total_urls_processed else 0
    summary_message = (
        f"Crawler finished. "
        f"Processed: {total_urls_processed}, "
        f"Errors: {error_count} ({error_percentage:.2f}%), "
        f"Skipped (robots.txt): {skipped_due_to_robots}"
    )
    logging.info(summary_message)

if __name__ == '__main__':
    logging.info("Crawler node started")
    crawler_process()