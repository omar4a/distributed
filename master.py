import time
import logging
import boto3
import json
import threading
from queue import Queue

# Configure logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# Initialize task queue, database connections, etc. 
sqs = boto3.resource('sqs', region_name = 'eu-north-1')
toCrawl_queue = sqs.get_queue_by_name(QueueName = 'Queue1.fifo')
crawled_Queue = sqs.get_queue_by_name(QueueName = 'crawled_URLs.fifo')
urls_to_crawl_queue = Queue()


def send_task_to_queue(task_data, groupID):
    """
    Sends a crawling task to the SQS queue.
    :param task_data: A dictionary with task details (e.g., URL to crawl).
    """
    # Convert task data into a JSON string
    message_body = json.dumps(task_data)

    # Send message to the queue
    response = toCrawl_queue.send_message(MessageBody=message_body, MessageGroupId = groupID)
    logging.info(f"Task sent to queue with MessageId: {response.get('MessageId')}")

def fetch_task_from_queue():
    """
    Fetches a crawling task from the SQS queue.
    :return: The task data (as a dictionary) or None if no messages are available.
    """
    messages = crawled_Queue.receive_messages(
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

def master_process():

    logging.info(f"Master node started")

    urls_to_crawl_queue.put("http://example.com")
    urls_to_crawl_queue.put("http://example.org")

    def receive_crawled_urls():
        """
        Continuously receives messages from the crawled_URLs queue and adds them to the URLs_To_Crawl queue.
        """
        while True:
            try:
                messages = crawled_Queue.receive_messages(
                    MaxNumberOfMessages=10,  # Adjust batch size
                    WaitTimeSeconds=10       # Long polling
                )
                
                for message in messages:
                    
                    # Process and deserialize crawled URLs
                    try:
                        crawled_urls = json.loads(message.body)
                        for url in crawled_urls:
                            urls_to_crawl_queue.put(url)  # Add to local queue
                        logging.info(f"Added {len(crawled_urls)} URLs to URLs_To_Crawl queue")
                    except Exception as e:
                        logging.error(f"Error parsing crawled URLs: {e}")
                    
                    # Delete message after processing
                    message.delete()
                
            except Exception as e:
                logging.error(f"Error receiving from crawled_URLs queue: {e}")
            time.sleep(1)  # Prevent rapid polling

    def assign_urls_to_crawlers():
        """
        Continuously assigns URLs to crawler nodes and pushes tasks to the URLs_To_Crawl queue.
        """
        task_count = 0
        crawler_tasks_assigned = 0
        
        while True:
            if urls_to_crawl_queue.qsize():
                url_to_crawl = urls_to_crawl_queue.get()  # Get URL (FIFO)
                
                # Assign URL to the queue for crawlers
                toCrawl_queue.send_message(
                    MessageBody=json.dumps({"url": url_to_crawl}),
                    MessageGroupId="urls_to_crawl",  # Group ID for tasks
                )
                
                crawler_tasks_assigned += 1
                logging.info(f"Assigned URL to crawler. Task count: {task_count}, URLs remaining: {urls_to_crawl_queue.qsize()}")
            
            time.sleep(0.1)  # Adjust based on performance needs

    receive_thread = threading.Thread(target=receive_crawled_urls, args=())
    assign_thread = threading.Thread(target=assign_urls_to_crawlers, args=())

    # Start threads
    receive_thread.start()
    assign_thread.start()

if __name__ == '__main__':

    master_process()