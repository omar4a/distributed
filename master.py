from mpi4py import MPI
import time
import logging
import boto3
import json
import threading

# Configure logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# AWS SQS setup
sqs = boto3.resource('sqs', region_name='eu-north-1')
toCrawl_queue = sqs.get_queue_by_name(QueueName='Queue1.fifo')
crawled_Queue = sqs.get_queue_by_name(QueueName='crawled_URLs.fifo')
urls_to_crawl_queue = []

MAX_PAGES = 10


def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Master node started with rank {rank} of {size}")

    crawler_nodes = size - 2
    indexer_nodes = 1

    active_crawler_nodes = list(range(1, 1 + crawler_nodes))
    active_indexer_nodes = list(range(1 + crawler_nodes, size))

    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    seed_urls = ["http://example.com", "http://example.org"]
    urls_to_crawl_queue.extend(seed_urls)

    def receive_crawled_urls():
        while True:
            try:
                messages = crawled_Queue.receive_messages(
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=10
                )

                for message in messages:
                    try:
                        crawled_urls = json.loads(message.body)
                        urls_to_crawl_queue.extend(crawled_urls)
                        logging.info(f"Added {len(crawled_urls)} URLs to URLs_To_Crawl queue")
                    except Exception as e:
                        logging.error(f"Error parsing crawled URLs: {e}")
                    message.delete()

            except Exception as e:
                logging.error(f"Error receiving from crawled_URLs queue: {e}")
            time.sleep(1)

    def assign_urls_to_crawlers():
        total_assigned = 0

        while total_assigned < MAX_PAGES:
            if urls_to_crawl_queue:
                url_to_crawl = urls_to_crawl_queue.pop(0)
                toCrawl_queue.send_message(
                    MessageBody=json.dumps({"url": url_to_crawl}),
                    MessageGroupId="urls_to_crawl"
                )
                total_assigned += 1
                logging.info(f"Assigned URL to crawler. Total assigned: {total_assigned}, Remaining: {len(urls_to_crawl_queue)}")
            time.sleep(0.1)

        logging.info("Max pages assigned. Stopping assignment thread.")

    receive_thread = threading.Thread(target=receive_crawled_urls)
    assign_thread = threading.Thread(target=assign_urls_to_crawlers)

    receive_thread.start()
    assign_thread.start()

    receive_thread.join()
    assign_thread.join()

    logging.info("Master: All tasks completed. Exiting.")


if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        master_process()
    elif rank == 1:
        from crawler import crawler_process
        crawler_process()
    elif rank == 2:
        from indexer import indexer_process
        indexer_process()
