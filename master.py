import time
import logging
import boto3
import json
import threading
import uuid
from queue import Queue

# Configure logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# Initialize task queue, database connections, etc. 
sqs = boto3.resource('sqs', region_name = 'eu-north-1')
toCrawl_queue = sqs.get_queue_by_name(QueueName = 'Queue1.fifo')
crawled_Queue = sqs.get_queue_by_name(QueueName = 'crawled_URLs.fifo')
search_results_queue = sqs.get_queue_by_name(QueueName='search_results.fifo')
search_query_queue = sqs.get_queue_by_name(QueueName='search_query.fifo')
urls_to_crawl_queue = Queue()

# Assuming you've created the heartbeat queue beforehand:
heartbeat_queue = sqs.get_queue_by_name(QueueName='master_heartbeat.fifo')

def send_heartbeat():
    """Send a heartbeat message to the master_heartbeat queue."""
    heartbeat_message = {"heartbeat": "alive", "timestamp": int(time.time())}
    heartbeat_queue.send_message(
        MessageBody=json.dumps(heartbeat_message),
        MessageGroupId=str(uuid.uuid4()),
    )

def heartbeat_loop():
    while True:
        send_heartbeat()
        time.sleep(15)  # send heartbeat every 10 seconds


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
    

def prompt_and_delegate_search():
    """
    Continuously prompt for search queries, send them to the indexer,
    and display the returned results. When '/.quit' is entered,
    it initiates the shutdown routine.
    """
    while True:
        search_query = input("Enter a search query (or '/.quit' to exit):\n").strip()
        if search_query == "/.quit":
            logging.info("Shutdown command received. Initiating shutdown routine.")
            send_search_query(search_query)  # Notify the indexer to terminate
            shutdown_routine()
            break
        send_search_query(search_query)
        poll_for_search_results()


def shutdown_routine():
    """
    Sends a shutdown message to shutdown.fifo, waits 3 seconds, purges
    all specified queues, and then exits the master process.
    """
    # Send shutdown message to shutdown.fifo (do not delete this message later)
    shutdown_queue = sqs.get_queue_by_name(QueueName='shutdown.fifo')
    shutdown_msg = {"status": "finished"}
    shutdown_queue.send_message(MessageBody=json.dumps(shutdown_msg), MessageGroupId=str(uuid.uuid4()))
    logging.info("Sent shutdown message to shutdown.fifo.")

    # Wait 3 seconds to allow all nodes to receive the shutdown message
    time.sleep(3)

    # Purge all the specified queues
    client = boto3.client('sqs', region_name='eu-north-1')
    queues_to_purge = [
        'crawled_content.fifo',
        'crawled_URLs.fifo',
        'crawler_completion.fifo',
        'index_completion.fifo',
        'Queue1.fifo',
        'search_query.fifo',
        'search_results.fifo',
        'shutdown.fifo',
        'master_heartbeat.fifo'
    ]
    for queue_name in queues_to_purge:
        q = sqs.get_queue_by_name(QueueName=queue_name)
        client.purge_queue(QueueUrl=q.url)
        logging.info(f"Purged {queue_name}.")

    logging.info("Shutdown routine completed. Exiting master process.")
    exit(0)

def send_search_query(query):
    """
    Sends the user’s search query to the indexer via SQS.
    """
    search_query_queue = sqs.get_queue_by_name(QueueName='search_query.fifo')
    message = {"query": query}
    search_query_queue.send_message(MessageBody=json.dumps(message), MessageGroupId=str(uuid.uuid4()))
    logging.info(f"Sent search query to indexers: {query}")
    
def wait_for_indexing_completion():
    index_completion_queue = sqs.get_queue_by_name(QueueName='index_completion.fifo')
    while True:
        messages = index_completion_queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=10)
        if messages:
            message = messages[0]
            data = json.loads(message.body)
            if data.get("status") == "finished":
                logging.info(f"Indexer finished. Global index available at: {data.get('location')}")
                message.delete()
                break
        time.sleep(1)
    prompt_and_delegate_search()

def poll_for_search_results():
    logging.info("Polling for search results and aggregating responses...")
    
    aggregated_results = []
    timeout = 5
    start_time = time.time()
    result_found = False
    
    while time.time() - start_time < timeout and not result_found:
        messages = search_results_queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=3)
        if messages:
            for msg in messages:
                result_data = json.loads(msg.body)
                aggregated_results.append((msg, result_data))
        time.sleep(1)
    
    # Determine the final result based on aggregated responses.
    final_results = []
    for (_, result_data) in aggregated_results:
        if result_data.get("results") != "No results found.":
            if isinstance(result_data.get("results"), list):
                final_results.extend(result_data["results"])
            else:
                final_results.append(result_data.get("results"))
    if not final_results:
        final_result = {"results": "No results found."}
    else:
        final_result = {"results": final_results}
    
    print("Search Results:")
    if "error" in final_result:
        print(final_result["error"])
    elif isinstance(final_result.get("results"), list):
        for line in final_result.get("results"):
            print(line)
    else:
        print(final_result.get("results"))

    # Iteratively delete all messages currently in search_query.fifo.
    try:
        while True:
            msgs = search_query_queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=2, VisibilityTimeout=10)
            if not msgs:
                break
            for m in msgs:
                m.delete()
    except Exception as e:
        logging.error(f"Error deleting messages from search_query.fifo: {e}")

    # Iteratively delete all messages currently in search_results.fifo.
    try:
        while True:
            msgs = search_results_queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=2, VisibilityTimeout=10)
            if not msgs:
                break
            for m in msgs:
                m.delete()
    except Exception as e:
        logging.error(f"Error deleting messages from search_results.fifo: {e}")


def master_process():

    logging.info(f"Master node started")
    
    # Prompt for seed URLs (enter 1 to 5 URLs, comma-separated)
    seed_input = input("Enter 1-5 seed URLs separated by commas (include http(s)://):\n")
    seeds = [u.strip() for u in seed_input.split(",") if u.strip()]
    if not (1 <= len(seeds) <= 5):
        logging.error("You must provide between 1 and 5 seed URLs.")
        exit(1)

    # Prompt for DEPTH (an integer between 0 and 20)
    try:
        depth = int(input("Enter the crawl depth (0-20):\n"))
        if not (0 <= depth <= 20):
            raise ValueError
    except ValueError:
        logging.error("Depth must be an integer between 0 and 20.")
        exit(1)

    # Place each seed URL in the local queue with additional depth information.
    for seed in seeds:
        # Each task now is a dictionary with the seed URL,
        # the current depth (0 initially), and the maximum depth (as provided)
        seed_task = {"url": seed, "current_depth": 0, "max_depth": depth}
        urls_to_crawl_queue.put(seed_task)

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
                
                toCrawl_queue.send_message(
                    MessageBody=json.dumps(url_to_crawl),
                    MessageGroupId=str(uuid.uuid4()),
                    )
                
                crawler_tasks_assigned += 1
                logging.info(f"Assigned URL to crawler. Task count: {task_count}, URLs remaining: {urls_to_crawl_queue.qsize()}")
            
            time.sleep(0.1)  # Adjust based on performance needs

    receive_thread = threading.Thread(target=receive_crawled_urls, args=())
    assign_thread = threading.Thread(target=assign_urls_to_crawlers, args=())
    index_thread = threading.Thread(target=wait_for_indexing_completion, args=())
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    

    # Start threads
    receive_thread.start()
    assign_thread.start()
    index_thread.start()
    heartbeat_thread.start()

if __name__ == '__main__':

    master_process()