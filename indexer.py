import logging
import json
import time
import boto3
import os
import zipfile
import uuid
import shutil
from requests_html import AsyncHTMLSession
import asyncio

from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import MultifieldParser, OrGroup
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

sqs = boto3.resource('sqs', region_name='eu-north-1')
content_queue = sqs.get_queue_by_name(QueueName='crawled_content.fifo')

INDEX_DIR = "whoosh_index"

schema = Schema(title=TEXT(stored=True), url=ID(stored=True, unique=True), content=TEXT)

if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)
    ix = create_in(INDEX_DIR, schema)
else:
    ix = open_dir(INDEX_DIR)


async def async_fetch_rendered_html(url):
    session = AsyncHTMLSession()
    try:

        response = await session.get(url, timeout=20)

        await asyncio.wait_for(response.html.arender(timeout=20, sleep=1), timeout=20)
        
        if response.html.page is not None:
            ready_state = await response.html.page.evaluate("document.readyState")
            if ready_state not in ["interactive", "complete"]:
                logging.info(f"{url}: readyState is '{ready_state}' but proceeding anyway.")
        else:
            logging.error(f"{url}: response.html.page is None. Proceeding without readyState validation.")

        html_content = response.html.html
        await session.close()  # Close the session to reset the browser
        return html_content
    except asyncio.TimeoutError:
        logging.error(f"Timeout: Rendering {url} exceeded 15s. Aborting and resetting the browser.")
        await session.close()
        return ""
    except Exception as e:
        logging.error(f"Failed rendering {url}: {e}")
        await session.close()
        return ""

def fetch_rendered_html(url):
    return asyncio.run(async_fetch_rendered_html(url))

def clean_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for script in soup(["script", "style"]):
        script.decompose()
    return soup.get_text(separator=" ", strip=True)

def index_content(data):
    url = data.get("url")
    raw_html = fetch_rendered_html(url)
    title = data.get("title", url)

    if not url or not raw_html:
        logging.warning("Incomplete data received. Skipping.")
        return

    text = clean_text(raw_html)
    print(text)

    writer = ix.writer()
    writer.update_document(title=title, url=url, content=text)
    writer.commit()

    logging.info(f"Indexed: {url} ({len(text)} chars)")


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


def search_query_listener():
    search_query_queue = sqs.get_queue_by_name(QueueName='search_query.fifo')
    search_results_queue = sqs.get_queue_by_name(QueueName='search_results.fifo')
    last_query = None  # To process a query only once per indexer
    while True:
        messages = search_query_queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=10)
        if messages:
            msg = messages[0]
            query_data = json.loads(msg.body)
            query = query_data.get("query", "").strip()
            
            # If this query was already processed, skip processing
            if query == last_query:
                time.sleep(2)
                continue
            last_query = query
            
            logging.info(f"Indexer received search query: {query}")
            
            if query == "/.quit":
                logging.info("Received quit command. Terminating search query listener.")
                break

            qp = MultifieldParser(["title", "content"], schema=ix.schema, group=OrGroup.factory(0.9))
            try:
                q = qp.parse(query)
                with ix.searcher() as searcher:
                    results_obj = searcher.search(q, limit=10)
                    if len(results_obj) > 0:
                        results = {"results": [f"{r['title']} ({r['url']})" for r in results_obj]}
                    else:
                        results = {"results": "No results found."}
            except Exception as e:
                results = {"error": f"Invalid query: {e}"}
            
            search_results_queue.send_message(
                MessageBody=json.dumps(results),
                MessageGroupId=str(uuid.uuid4())
            )
        else:
            time.sleep(3)

def indexer_process():

    shutdown_queue = sqs.get_queue_by_name(QueueName='shutdown.fifo')
    start_time = None

    while True:
        
        shutdown_msgs = shutdown_queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=2)
        if shutdown_msgs:
            shutdown_data = json.loads(shutdown_msgs[0].body)
            if shutdown_data.get("status") == "finished":
                logging.info("Shutdown message received on shutdown.fifo. Exiting indexer process.")
                break

        messages = content_queue.receive_messages(
            MaxNumberOfMessages=5,
            WaitTimeSeconds=10,
            VisibilityTimeout=20
        )
        if messages:
            start_time = None
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
        else:
            if start_time is None:
                start_time = time.time()
            else:
                elapsed = time.time() - start_time
                if elapsed >= 10:
                    # Check if crawler_completion.fifo has a done message
                    crawler_done_queue = sqs.get_queue_by_name(QueueName='crawler_completion.fifo')
                    done_messages = crawler_done_queue.receive_messages(
                        MaxNumberOfMessages=1,
                        WaitTimeSeconds=2
                    )
                    if done_messages:
                        done_data = json.loads(done_messages[0].body)
                        if done_data.get("status") == "done":
                            logging.info("Crawler_completion indicates 'done'. Checking final crawled_content messages.")
                            
                            # Final check: process any remaining messages in crawled_content.fifo
                            final_msgs = content_queue.receive_messages(
                                MaxNumberOfMessages=5,
                                WaitTimeSeconds=3
                            )
                            while final_msgs:
                                for msg in final_msgs:
                                    try:
                                        content_data = json.loads(msg.body)
                                        if isinstance(content_data, list):
                                            for item in content_data:
                                                index_content(item)
                                        else:
                                            index_content(content_data)
                                        msg.delete()
                                    except Exception as e:
                                        logging.error(f"Failed processing final message: {e}")
                                final_msgs = content_queue.receive_messages(
                                    MaxNumberOfMessages=5,
                                    WaitTimeSeconds=3
                                )
                            logging.info("Final crawled_content check complete; exiting indexer loop.")
                            break
            time.sleep(1)


    index_completion_queue = sqs.get_queue_by_name(QueueName='index_completion.fifo')
    status_message = {
        "status": "finished",
    }
    index_completion_queue.send_message(MessageBody=json.dumps(status_message), MessageGroupId=str(uuid.uuid4()))

    search_query_listener()
    
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

        # Delete local files: the whoosh_index directory and the zip archive.
        if os.path.exists(INDEX_DIR):
            shutil.rmtree(INDEX_DIR)
            logging.info(f"Deleted local index directory: {INDEX_DIR}")
        if os.path.exists(zip_path):
            os.remove(zip_path)
            logging.info(f"Deleted local zip archive: {zip_path}")


if __name__ == '__main__':

    logging.info("Indexer started. Listening for content to index...")

    indexer_process()