# 📦 ES Ingester

**ES Ingester** is a Python CLI tool that ingests JSON or JSONL data directly into an Elasticsearch index. It supports multithreading, dynamic JSON extraction, configuration persistence, and optional metadata tagging.

---

## 🚀 Features
- **Flexible Input**: Accepts JSON and JSONL data from stdin.
- **Dynamic JSON Key Extraction**: Supports nested keys (e.g., `-json 'data->0->result'`).
- **Multithreaded Ingestion**: Speed up ingestion with customizable thread count.
- **Configuration Persistence**: Saves Elasticsearch credentials to a config file for easy reuse.
- **Parent Field Addition**: Optional `-parent` flag allows adding key-value metadata to each document.
- **Verbose Mode**: Track progress in real-time.

---

## 🔧 Installation

```bash
pip install es-ingester
```

## ⚙️ Configuration

If *.es_ingester_config.yaml* already exists in your home directory and contains valid credentials, ES Ingester will use it automatically. The configuration file will be generated automatically the first time credentials are provided.

Example `.es_ingester_config.yaml`

```
# ~/.es_ingester_config.yaml

es_host: "http://localhost:9200"
username: "your_username"
password: "your_password"
```

## 🛠️ Usage

Ingest JSONL data with saved configuration:

```
cat data.jsonl | es-ingester -indexname 'my_index' -jsonl
```

```
usage: es_ingester [-h] [-es_host ES_HOST] [-username USERNAME] [-password PASSWORD] -indexname INDEXNAME [-threads THREADS] [-json JSON] [-jsonl] [-verbose] [-parent PARENT] [-print PRINT]

Ingest data into Elasticsearch

options:
  -h, --help            show this help message and exit
  -es_host ES_HOST      Elasticsearch host URL
  -username USERNAME    Elasticsearch username
  -password PASSWORD    Elasticsearch password
  -indexname INDEXNAME  Index name to use
  -threads THREADS      Number of threads for ingestion
  -json JSON            Key for JSON extraction (e.g., "result")
  -jsonl                Indicates that stdin contains newline-separated JSON documents
  -verbose              Show progress of document ingestion
  -parent PARENT        Add a key-value pair to each document in the format key:value
  -print PRINT          Specify a key name to print from each document during ingestion
  ```

### Specify JSON Key for Nested Arrays
Extract nested JSON data by specifying a key path:

```
cat data.json | es-ingester -indexname 'my_index' -json 'data->0->result'
```


### Add Metadata with Parent Field

Add a *domain* field with the value *example.com* to each document:
```
cat data.jsonl | es-ingester -indexname 'my_index' -jsonl -parent 'domain:example.com'
```


### Full Command with Verbose Output
Ingest JSONL data with a specific host, user, and password, and show progress:

```
cat data.jsonl | es-ingester -es_host 'http://localhost:9200' -username 'user' -password 'pass' -indexname 'my_index' -jsonl -verbose
```

- JSONL vs JSON: Use -jsonl for newline-separated JSON objects or -json to specify a nested key for JSON arrays.
- Configuration Persistence: If ~/.es_ingester_config.yaml exists, it will be used by default.
- Parent Field: Adding metadata with -parent is optional. Use key:value format (e.g., -parent 'source:api').