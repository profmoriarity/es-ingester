import argparse
import json
import jsonlines
import os
import sys
import threading
import yaml
from elasticsearch import Elasticsearch
import time

class ESIngester:
    def __init__(self, es_host, username, password, index_name, verbose, parent=None):
        self.es = Elasticsearch([es_host], http_auth=(username, password))
        self.index_name = index_name
        self.verbose = verbose
        self.lock = threading.Lock()
        self.total_documents = 0
        self.parent_data = {}

        # Parse the parent flag if provided
        if parent:
            try:
                key, value = parent.split(":", 1)
                self.parent_data[key] = value
            except ValueError:
                print("Invalid format for -parent. Use key:value format.")
                sys.exit(1)

    def ingest_jsonl(self, lines):
        self.total_documents = len(lines)
        for index, line in enumerate(lines):
            doc = json.loads(line)

            # Add parent data if provided
            if self.parent_data:
                doc.update(self.parent_data)

            self.es.index(index=self.index_name, document=doc)
            if self.verbose:
                self.print_progress(index + 1, self.total_documents)

    def ingest_json(self, data_key, json_data):
        documents = self.extract_documents(data_key, json_data)
        self.total_documents = len(documents)
        threads = []

        for index, doc in enumerate(documents):
            # Add parent data if provided
            if self.parent_data:
                doc.update(self.parent_data)

            thread = threading.Thread(target=self.index_document, args=(doc, index))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def index_document(self, doc, index):
        self.es.index(index=self.index_name, document=doc)
        if self.verbose:
            self.print_progress(index + 1, self.total_documents)

    def extract_documents(self, data_key, json_data):
        keys = data_key.split("->")
        documents = json_data

        for key in keys:
            if isinstance(documents, list):
                documents = documents
            else:
                documents = documents.get(key)

        return documents if isinstance(documents, list) else [documents]

    def print_progress(self, processed, total):
        with self.lock:
            print(f"\rProcessed {processed}/{total} documents...", end='', flush=True)

def load_config():
    config_path = os.path.expanduser("~/.es_ingester_config.yaml")
    if os.path.exists(config_path):
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    return None

def save_config(es_host, username, password):
    config = {
        'es_host': es_host,
        'username': username,
        'password': password,
    }
    config_path = os.path.expanduser("~/.es_ingester_config.yaml")
    with open(config_path, 'w') as config_file:
        yaml.dump(config, config_file)

def main():
    parser = argparse.ArgumentParser(description='Ingest data into Elasticsearch')
    parser.add_argument('-es_host', help='Elasticsearch host URL')
    parser.add_argument('-username', help='Elasticsearch username')
    parser.add_argument('-password', help='Elasticsearch password')
    parser.add_argument('-indexname', required=True, help='Index name to use')
    parser.add_argument('-threads', type=int, default=20, help='Number of threads for ingestion')
    parser.add_argument('-json', help='Key for JSON extraction (e.g., "result")')
    parser.add_argument('-jsonl', action='store_true', help='Indicates that stdin contains newline-separated JSON documents')
    parser.add_argument('-verbose', action='store_true', help='Show progress of document ingestion')
    parser.add_argument('-parent', help='Add a key-value pair to each document in the format key:value')

    args = parser.parse_args()

    # Load configuration from file if not provided via command line
    config = load_config()
    if config:
        es_host = args.es_host if args.es_host else config.get('es_host')
        username = args.username if args.username else config.get('username')
        password = args.password if args.password else config.get('password')
    else:
        es_host = args.es_host
        username = args.username
        password = args.password

    # Check if credentials are provided
    if not es_host or not username or not password:
        print("Error: Elasticsearch host, username, and password must be provided either via command line or config file.")
        sys.exit(1)

    # Save config to YAML file if not already present
    save_config(es_host, username, password)

    # Initialize ingester with parent data if provided
    ingester = ESIngester(es_host, username, password, args.indexname, args.verbose, parent=args.parent)

    # Read from stdin based on the input type specified
    if args.jsonl:
        # Handle newline-separated JSON objects
        lines = sys.stdin.read().strip().splitlines()
        ingester.ingest_jsonl(lines)
    elif args.json:
        # Handle JSON input
        json_data = json.load(sys.stdin)
        ingester.ingest_json(args.json, json_data)
    else:
        print("No input specified. Use -json or -jsonl.")
        sys.exit(1)

    if args.verbose:
        print("\nIngestion complete.")

if __name__ == '__main__':
    main()
