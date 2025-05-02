from mpi4py import MPI
import logging
import json
import time
import boto3
import os
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import MultifieldParser, OrGroup
from bs4 import BeautifulSoup

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

def search_index():
    print("\n Welcome to the search engine (Boolean operators supported)")
    print("   Example: google AND example")
    print("   Example: python AND NOT example")
    print("   Example: python OR example")
    print("   Use quotes for exact phrases: \"data science\"")

    while True:
        term = input("\n🔍 Enter a search query (or 'exit'): ").strip()
        if term.lower() == "exit":
            break

        qp = MultifieldParser(["title", "content"], schema=ix.schema, group=OrGroup.factory(0.9))
        try:
            q = qp.parse(term)
        except Exception as e:
            print(f"Invalid query: {e}")
            continue

        with ix.searcher() as searcher:
            results = searcher.search(q, limit=10)
            if results:
                print(f"\n Found {len(results)} result(s):")
                for r in results:
                    print(f"- {r['title']} ({r['url']})")
            else:
                print("No results found.")

def indexer_process():
    logging.info("Indexer started. Listening for content to index...")

    while True:
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

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "search":
        search_index()
    else:
        indexer_process()