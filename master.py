from mpi4py import MPI
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    crawler_nodes = size - 2
    indexer_nodes = 1

    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Not enough nodes to run crawler and indexer. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return

    active_crawler_nodes = list(range(1, 1 + crawler_nodes))
    active_indexer_nodes = list(range(1 + crawler_nodes, size))

    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    seed_urls = ["http://example.com", "http://example.org"]
    urls_to_crawl_queue = seed_urls

    task_count = 0
    crawler_tasks_assigned = 0
    max_tasks = 5

    while task_count < max_tasks and (urls_to_crawl_queue or crawler_tasks_assigned > 0):
        if crawler_tasks_assigned > 0:
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                message_source = status.Get_source()
                message_tag = status.Get_tag()
                message_data = comm.recv(source=message_source, tag=message_tag)

                if message_tag == 1:
                    crawler_tasks_assigned -= 1
                    new_urls = message_data
                    if new_urls:
                        urls_to_crawl_queue.extend(new_urls)
                    logging.info(f"Master received URLs from Crawler {message_source}, URLs in queue: {len(urls_to_crawl_queue)}, Tasks assigned: {crawler_tasks_assigned}")
                elif message_tag == 99:
                    logging.info(f"Node {message_source} status: {message_data}")
                elif message_tag == 999:
                    logging.error(f"Node {message_source} reported error: {message_data}")
                    crawler_tasks_assigned -= 1

        while task_count < max_tasks and urls_to_crawl_queue and crawler_tasks_assigned < crawler_nodes:
            url_to_crawl = urls_to_crawl_queue.pop(0)
            available_crawler_rank = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)]
            task_id = task_count
            task_count += 1
            comm.send(url_to_crawl, dest=available_crawler_rank, tag=0)
            crawler_tasks_assigned += 1

            logging.info(f"Master assigned task {task_id} (crawl {url_to_crawl}) to Crawler {available_crawler_rank}, Tasks assigned: {crawler_tasks_assigned}")
            time.sleep(0.1)
            time.sleep(1)
            logging.info("Master node finished URL distribution. Waiting for crawlers to complete...")
            print("Master Node Finished.")

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
        from crawler_node import crawler_process
        crawler_process()
    elif rank == 2:
        from Indexer import indexer_process
        indexer_process()
