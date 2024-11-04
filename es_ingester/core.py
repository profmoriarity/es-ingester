import argparse
import json
import jsonlines
import os
import sys
import threading
import yaml
from elasticsearch import Elasticsearch

class ESIngester:
    def __init__(self, es_host, username, password, index_name):
        self.es = Elasticsearch([es_host], http_auth=(username, password))
        self.index_name = index_name

    def ingest_jsonl(self, lines):
        for line in lines:
            doc = json.loads(line)
            self.es.index(index=self.index_name, document=doc)

    def ingest_json(self, data_key, json_data):
        documents = self.extract_documents(data_key, json_data)
        threads = []

        for doc in documents:
            thread = threading.Thread(target=self.index_document, args=(doc,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def index_document(self, doc):
        self.es.index(index=self.index_name, document=doc)

    def extract_documents(self, data_key, json_data):
        keys = data_key.split("->")
        documents = json_data

        for key in keys:
            if isinstance(documents, list):
                documents = documents
            else:
                documents = documents.get(key)

        return documents if isinstance(documents, list) else [documents]

def save_config(es_host, username, password):
    config = {
        'es_host': es_host,
        'username': username,
        'password': password,
    }
    config_path = os.path.expanduser("~/.es_ingester_config.yaml")
    with open(config_path, 'w') as config_file:
        yaml.dump(config, config_file)

def load_config():
    config_path = os.path.expanduser("~/.es_ingester_config.yaml")
    if os.path.exists(config_path):
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    return None

def main():
    parser = argparse.ArgumentParser(description='Ingest data into Elasticsearch')
    parser.add_argument('-es_host', required=True, help='Elasticsearch host URL')
    parser.add_argument('-username', required=True, help='Elasticsearch username')
    parser.add_argument('-password', required=True, help='Elasticsearch password')
    parser.add_argument('-indexname', required=True, help='Index name to use')
    parser.add_argument('-threads', type=int, default=20, help='Number of threads for ingestion')
    parser.add_argument('-json', help='Key for JSON extraction (e.g., "result")')

    args = parser.parse_args()

    # Save config to YAML file
    save_config(args.es_host, args.username, args.password)

    ingester = ESIngester(args.es_host, args.username, args.password, args.indexname)

    # Read from stdin
    if args.json is None:
        with jsonlines.Reader(sys.stdin) as reader:
            ingester.ingest_jsonl(reader)
    else:
        json_data = json.load(sys.stdin)
        ingester.ingest_json(args.json, json_data)

if __name__ == '__main__':
    main()
