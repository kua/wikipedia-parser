# wikipedia-parser

## Configuration

Copy `config.example.yml` to `config.yml` (or pass a custom path via `-config`) and adjust the values if needed.

```yaml
dump_base_url: https://dumps.wikimedia.org/kiwix/zim/wikipedia
kafka_broker:
  - localhost:9092
kafka_topic: wikipedia-pages
work_dir: ./data
status_file: ./data/status.log
sink: kafka # use "stdout" to log messages instead of writing to Kafka
http_addr: :8080
```

The exporter keeps the names of processed dumps in `status_file`. Restarting the service will skip entries already recorded there. Set `sink` to `stdout` to print each processed page to the console (decoded into a human readable line) instead of sending it to Kafka.

Run the service with:

```
go run ./cmd/wikiparser -config config.yml
```

## HTTP API

All control endpoints accept both `GET` and `POST` requests:

* `/start` — start or resume the exporter.
* `/pause` — finish the current file and pause processing.
* `/abort` — stop the current run and reset progress.
* `/metrics` — fetch the current progress metrics.
* `/list` — view processed and pending dump URLs for the active run.
* `/list/all` — on demand discovery of every dump file currently available for download.
