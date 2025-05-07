import logging
import json
import time
import boto3
import uuid

import os
import zipfile
import threading
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import MultifieldParser, OrGroup
from bs4 import BeautifulSoup

termination_event = threading.Event()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

# AWS SQS Setup
sqs = boto3.resource('sqs', region_name='eu-north-1')
content_queue = sqs.get_queue_by_name(QueueName='crawled_content.fifo')

# Whoosh index setup
INDEX_DIR = "whoosh_index"

schema = Schema(title=TEXT(stored=True), url=ID(stored=True, unique=True), content=TEXT)

if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)
    ix = create_in(INDEX_DIR, schema)
else:
    ix = open_dir(INDEX_DIR)

def clean_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for script in soup(["script", "style"]):
        script.decompose()
    return soup.get_text(separator=" ", strip=True)

def index_content(data):
    url = data.get("url")
    raw_html = data.get("html")
    title = data.get("title", url)

    if not url or not raw_html:
        logging.warning("Incomplete data received. Skipping.")
        return

    text = clean_text(raw_html)

    writer = ix.writer()
    writer.update_document(title=title, url=url, content=text)
    writer.commit()

    logging.info(f"Indexed: {url} ({len(text)} chars)")

# def wait_for_crawlers_done():
#     empty_period = 0
#     required_empty_period = 10  # seconds
#     to_crawl = sqs.get_queue_by_name(QueueName='Queue1.fifo')
#     crawled = sqs.get_queue_by_name(QueueName='crawled_content.fifo')
#     crawler_completion = sqs.get_queue_by_name(QueueName='crawler_completion.fifo')
    
#     logging.info("Waiting for crawl activity to cease.")
#     while empty_period < required_empty_period and not termination_event.is_set():
#         to_crawl_count = int(to_crawl.attributes.get('ApproximateNumberOfMessages', 0))
#         crawled_count = int(crawled.attributes.get('ApproximateNumberOfMessages', 0))
        
#         if to_crawl_count == 0 and crawled_count == 0:
#             done_messages = crawler_completion.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=5)
#             if not done_messages:
#                 empty_period += 5
#             else:
#                 empty_period = 0
#                 for msg in done_messages:
#                     msg.delete()
#         else:
#             empty_period = 0
        
#         time.sleep(5)
    
#     logging.info("No crawl activity detected for desired period; terminating indexing.")
#     termination_event.set()

def finalize_index():
    index_dir = "whoosh_index"
    zip_path = "index.zip"
    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(index_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, index_dir)
                    zipf.write(file_path, arcname)
        logging.info("Index successfully compressed to index.zip")
    except Exception as e:
        logging.error(f"Error compressing index: {e}")
        return None
    return zip_path

# After waiting for crawler completion in indexer_process():
zip_path = finalize_index()
if zip_path:
    s3 = boto3.client('s3', region_name='eu-north-1')
    bucket_name = 'distributed-index'
    try:
        s3.upload_file(zip_path, bucket_name, 'index.zip')
        logging.info("Uploaded final index to S3.")
    except Exception as e:
        logging.error(f"Failed to upload index to S3: {e}")

    # Notify the master that indexing is finished.
    index_completion_queue = sqs.get_queue_by_name(QueueName='index_completion.fifo')
    status_message = {
        "status": "finished",
        "location": f"s3://{bucket_name}/index.zip"
    }
    index_completion_queue.send_message(MessageBody=json.dumps(status_message), MessageGroupId=str(uuid.uuid4()))


def search_query_listener():
    search_query_queue = sqs.get_queue_by_name(QueueName='search_query.fifo')
    search_results_queue = sqs.get_queue_by_name(QueueName='search_results.fifo')
    while True:
        messages = search_query_queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=10)
        if messages:
            msg = messages[0]
            query_data = json.loads(msg.body)
            query = query_data.get("query", "").strip()
            logging.info(f"Indexer received search query: {query}")
            
            # Check for the quit command
            if query == "/.quit":
                logging.info("Received quit command. Terminating search query listener.")
                msg.delete()  # Optionally, delete the quit message
                termination_event.set()
                break

            # Process the regular query
            qp = MultifieldParser(["title", "content"], schema=ix.schema, group=OrGroup.factory(0.9))
            try:
                q = qp.parse(query)
                with ix.searcher() as searcher:
                    results_obj = searcher.search(q, limit=10)
                    if results_obj:
                        results = {"results": [f"{r['title']} ({r['url']})" for r in results_obj]}
                    else:
                        results = {"results": "No results found."}
            except Exception as e:
                results = {"error": f"Invalid query: {e}"}
            
            # Send back the results
            search_results_queue.send_message(
                MessageBody=json.dumps(results),
                MessageGroupId=str(uuid.uuid4())
            )
            msg.delete()
        else:
            time.sleep(1)

# Start this listener near the start of indexer_process():
threading.Thread(target=search_query_listener, daemon=True).start()

def indexer_process():
    logging.info("Indexer started. Listening for content to index...")
    
    # Change the loop from while True to check termination_event
    while not termination_event.is_set():
        messages = content_queue.receive_messages(
            MaxNumberOfMessages=5,
            WaitTimeSeconds=10,
            VisibilityTimeout=20
        )
        if not messages:
            continue
        
        for msg in messages:
            try:
                content_data = json.loads(msg.body)
                if isinstance(content_data, list):
                    for item in content_data:
                        index_content(item)
                else:
                    index_content(content_data)
                msg.delete()
            except Exception as e:
                logging.error(f"Failed to process message: {e}")
    
    logging.info("Indexing loop terminated; finalizing index...")
    # Finalize and upload the index after termination:
    zip_path = finalize_index()
    if zip_path:
        s3 = boto3.client('s3', region_name='eu-north-1')
        bucket_name = 'distributed-index'
        try:
            s3.upload_file(zip_path, bucket_name, 'index.zip')
            logging.info("Uploaded final index to S3.")
        except Exception as e:
            logging.error(f"Failed to upload index to S3: {e}")
    
        # Notify the master that indexing is finished.
        index_completion_queue = sqs.get_queue_by_name(QueueName='index_completion.fifo')
        status_message = {
            "status": "finished",
            "location": f"s3://{bucket_name}/index.zip"
        }
        index_completion_queue.send_message(MessageBody=json.dumps(status_message), MessageGroupId=str(uuid.uuid4()))

if __name__ == '__main__':
    
        indexer_process()
