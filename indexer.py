from mpi4py import MPI
import logging
import json
import time
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

inverted_index = defaultdict(set)

INDEX_FILE = "index.json"

def process_text(text, source_url):
    words = text.lower().split()
    for word in words:
        if word.isalpha():
            inverted_index[word].add(source_url)

def save_index_to_file():
    with open(INDEX_FILE, "w") as f:
        json.dump({k: list(v) for k, v in inverted_index.items()}, f, indent=2)

def load_index_from_file():
    try:
        with open(INDEX_FILE, "r") as f:
            data = json.load(f)
            for word, urls in data.items():
                inverted_index[word] = set(urls)
    except FileNotFoundError:
        pass

def search_index():
    if not inverted_index:
        print(" Index is empty. Run crawler and indexer first.")
        return

    while True:
        word = input("\nEnter a keyword to search (or type exit to quit): ").strip().lower()
        if word == "exit":
            print(" Goodbye!")
            break
        elif word:
            if word in inverted_index:
                print(f" Found '{word}' in {len(inverted_index[word])} URL(s):")
                for url in inverted_index[word]:
                    print(f"  - {url}")
            else:
                print(f"🔍 Keyword '{word}' not found in the index.")

def indexer_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Indexer node started with rank {rank} of {size}")
    load_index_from_file()

    while True:
        status = MPI.Status()
        content_to_index = comm.recv(source=MPI.ANY_SOURCE, tag=2, status=status)
        source_rank = status.Get_source()

        if not content_to_index:
            logging.info(f"Indexer {rank} received shutdown signal. Exiting.")
            save_index_to_file()
            break

        try:
            source_url = content_to_index.get("url")
            text = content_to_index.get("text")

            if source_url and text:
                logging.info(f"Indexer {rank} indexing content from {source_url}")
                process_text(text, source_url)
                save_index_to_file()
                logging.info(f"Indexer {rank} saved index.json successfully.")
                comm.send(f"Indexer {rank} - Indexed content from {source_url}", dest=0, tag=99)
            else:
                logging.warning("Indexer received incomplete data")

        except Exception as e:
            logging.error(f"Indexer {rank} error indexing content: {e}")
            comm.send(f"Indexer {rank} - Error indexing: {e}", dest=0, tag=999)

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "search":
        load_index_from_file()
        search_index()
    else:
        indexer_process()
