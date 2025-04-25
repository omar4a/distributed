from mpi4py import MPI
import time
import logging
import boto3
import json
import threading

# Import necessary libraries for task queue, database, etc. (e.g., redis, cloud storage SDKs)

# Configure logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# Initialize task queue, database connections, etc. 
sqs = boto3.resource('sqs', region_name = 'eu-north-1')
toCrawl_queue = sqs.get_queue_by_name(QueueName = 'Queue1.fifo')
crawled_Queue = sqs.get_queue_by_name(QueueName = 'crawled_URLs.fifo')
urls_to_crawl_queue = []


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
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    crawler_nodes = size - 2  # Assuming master and at least one indexer node
    indexer_nodes = 1  # At least one indexer node



    ########################  uncomment if statement and remove the 2 lines under the if statement
    # if crawler_nodes <= 0 or indexer_nodes <= 0:
    #     logging.error(
    #         "Not enough nodes to run crawler and indexer. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
    #     return
    crawler_nodes = 1
    indexer_nodes = 0

    ##################################
    active_crawler_nodes = list(range(1, 1 + crawler_nodes))  # Ranks for crawler nodes (assuming rank 0 is master)
    active_indexer_nodes = list(range(1 + crawler_nodes, size))  # Ranks for indexer nodes

    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    seed_urls = ["http://example.com", "http://example.org"]  # Example seed URLs - replace with actual seed URLs
    urls_to_crawl_queue = seed_urls  # Simple list as initial queue - replace with a distributed queue 

    def receive_crawled_urls(crawled_queue, urls_to_crawl_queue):
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
                        urls_to_crawl_queue.extend(crawled_urls)  # Add to local queue
                        logging.info(f"Added {len(crawled_urls)} URLs to URLs_To_Crawl queue")
                    except Exception as e:
                        logging.error(f"Error parsing crawled URLs: {e}")
                    
                    # Delete message after processing
                    message.delete()
                
            except Exception as e:
                logging.error(f"Error receiving from crawled_URLs queue: {e}")
            time.sleep(1)  # Prevent rapid polling

    def assign_urls_to_crawlers(urls_to_crawl_queue, crawler_nodes, urls_queue):
        """
        Continuously assigns URLs to crawler nodes and pushes tasks to the URLs_To_Crawl queue.
        """
        task_count = 0
        crawler_tasks_assigned = 0
        
        while True:
            if urls_to_crawl_queue:
                url_to_crawl = urls_to_crawl_queue.pop(0)  # Get URL (FIFO)
                
                # Assign URL to the queue for crawlers
                toCrawl_queue.send_message(
                    MessageBody=json.dumps({"url": url_to_crawl}),
                    MessageGroupId="urls_to_crawl",  # Group ID for tasks
                )
                
                crawler_tasks_assigned += 1
                logging.info(f"Assigned URL to crawler. Task count: {task_count}, URLs remaining: {len(urls_to_crawl_queue)}")
            
            time.sleep(0.1)  # Adjust based on performance needs

    receive_thread = threading.Thread(target=receive_crawled_urls, args=(crawled_Queue, urls_to_crawl_queue))
    assign_thread = threading.Thread(target=assign_urls_to_crawlers, args=(urls_to_crawl_queue, crawler_nodes, toCrawl_queue))

    # Start threads
    receive_thread.start()
    assign_thread.start()


    # task_count = 0
    # crawler_tasks_assigned = 0

    # while True:  # Continue as long as there are URLs to crawl or tasks in progress
    #     # Check for completed crawler tasks and results from crawler nodes 
    #     if crawler_tasks_assigned > 0:
    #         if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG,
    #                        status=status):  # Non-blocking check for incoming messages
    #             message_source = status.Get_source()
    #             message_tag = status.Get_tag()
    #             message_data = comm.recv(source=message_source, tag=message_tag)

    #             if message_tag == 1:  # Crawler completed task and sent back extracted URLs
    #                 crawler_tasks_assigned -= 1
    #                 new_urls = message_data  # Assuming message_data is a list of URLs
    #                 if new_urls:
    #                     for url in new_urls:
    #                         send_task_to_queue(url, "url_to_crawl")  # Add newly discovered URLs to the queue
    #                 logging.info(
    #                     f"Master received URLs from Crawler {message_source}, URLs in queue: {len(urls_to_crawl_queue)}, Tasks assigned: {crawler_tasks_assigned}")
    #             elif message_tag == 99:  # Crawler node reports status/heartbeat
    #                 logging.info(f"Crawler {message_source} status: {message_data}")  # Example status message
    #             elif message_tag == 999:  # Crawler node reports error
    #                 logging.error(f"Crawler {message_source} reported error: {message_data}")
    #                 crawler_tasks_assigned -= 1  # Decrement task count even on error, consider re-assigning task in real implementation

    #     # Assign new crawling tasks if there are URLs in the queue and available crawler nodes 
    #     while urls_to_crawl_queue and crawler_tasks_assigned < crawler_nodes:  # Limit tasks to available crawler nodes for simplicity in this skeleton

    #         url_to_crawl = urls_to_crawl_queue.pop(0)  # Get URL from queue (FIFO for simplicity)
    #         available_crawler_rank = active_crawler_nodes[
    #             crawler_tasks_assigned % len(active_crawler_nodes)]  # Simple round-robin assignment
    #         task_id = task_count
    #         task_count += 1
    #         #comm.send(url_to_crawl, dest=available_crawler_rank, tag=0)  # Tag 0 for task assignment
    #         send_task_to_queue(url_to_crawl, "url_to_crawl")
    #         crawler_tasks_assigned += 1

    #         logging.info(
    #             f"Master assigned task {task_id} (crawl {url_to_crawl}) to Crawler {available_crawler_rank}, Tasks assigned: {crawler_tasks_assigned}")
    #         time.sleep(0.1)  # Small delay to prevent overwhelming master in this example
    #         time.sleep(1)  # Master node's main loop sleep - adjust as needed
    #         logging.info("Master node finished URL distribution. Waiting for crawlers to complete...")
    #         # In a real system, you would have more sophisticated shutdown and result aggregation logic 

    #     print("Master Node Finished.")

    # After finishing tasks, send shutdown signal to crawlers and indexers
    for crawler_rank in active_crawler_nodes:
        comm.send(None, dest=crawler_rank, tag=0)
    for indexer_rank in active_indexer_nodes:
        comm.send(None, dest=indexer_rank, tag=2)

    logging.info("Master: All tasks completed. Shutdown signals sent.")

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
