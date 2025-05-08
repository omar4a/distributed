import time
import logging
import boto3
import json
import threading
import uuid
from queue import Queue

def standby_loop():
    heartbeat_queue = sqs.get_queue_by_name(QueueName='master_heartbeat.fifo')
    timeout = 15  # seconds to wait for a heartbeat
    while True:
        messages = heartbeat_queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=timeout)
        if messages:
            # Delete received heartbeat message so the queue doesn’t accumulate stale messages.
            for msg in messages:
                msg.delete()
            logging.info("Heartbeat received. Primary master is alive.")
        else:
            logging.info("No heartbeat received within {} seconds. Taking over as active master.".format(timeout))
            # Promote self to active: call master_process() (or the appropriate takeover function)
            master_process()
            break
        time.sleep(1)

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
        'shutdown.fifo'
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
    search_results_queue = sqs.get_queue_by_name(QueueName='search_results.fifo')
    search_query_queue = sqs.get_queue_by_name(QueueName='search_query.fifo')
    logging.info("Polling for search results and aggregating responses...")
    
    aggregated_results = []
    timeout = 30
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        messages = search_results_queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5)
        if messages:
            for msg in messages:
                result_data = json.loads(msg.body)
                aggregated_results.append((msg, result_data))
        time.sleep(1)
    
    # Determine the final result based on aggregated responses.
    final_result = None
    for (_, result_data) in aggregated_results:
        if result_data.get("results") != "No results found.":
            final_result = result_data
            break
    if final_result is None:
        final_result = {"results": "No results found."}
    
    logging.info(f"Final Aggregated Search Results: {final_result}")
    print("Search Results:")
    if "error" in final_result:
        print(final_result["error"])
    elif isinstance(final_result.get("results"), list):
        for line in final_result.get("results"):
            print(line)
    else:
        print(final_result.get("results"))
    
    # Delete all received search result messages.
    for (msg, _) in aggregated_results:
        msg.delete()
    
    # Also delete the search query message(s) from the search_query queue.
    query_messages = search_query_queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5)
    for qm in query_messages:
        qm.delete()

def master_process():

    logging.info(f"Backup Master node started")

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

    # Start threads
    receive_thread.start()
    assign_thread.start()
    index_thread.start()

if __name__ == '__main__':
    # On the backup master VM, run the standby loop instead of master_process().
    standby_loop()