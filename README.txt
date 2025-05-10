To run the distributed web-crawling system and begin a crawling session, please do the following:

- Launch the cloud VMs with your favorite cloud provider.
- Install python3, pip, and git.
- Clone the GitHub repo: https://github.com/omar4a/distributed.git
- Run pip install requirements.txt
- To run a crawler node: python3 crawler.py
- To run an indexer node: python3 indexer.py
- To run a master backup node: python3 master_backup.py
- To run the master node and begin the crawling session: python3 master.py
- You can run the nodes in any order. If you run the crawlers and indexers first, they will wait for a master node to provide seed URLs and crawling parameters.
- When you run the master node, you will be prompted for seed URLs and depth parameters.
- You can add more crawler and indexer nodes during a session. Simply follow the steps above.
- When the crawling session is done, the master's CLI will prompt you for a search query. You can enter search queries and view search results.
- If you want to exit, provide "/.quit" as the search query, all the nodes will shut down.

Note: You must either have access to the existing SQS queues, or create new SQS queues with these names, and provide access to them to your VMs:
crawled_content.fifo
crawled_URLs.fifo
crawler_completion.fifo
index_completion.fifo
master_heartbeat.fifo
Queue1.fifo
search_query.fifo
search_results.fifo
shutdown.fifo