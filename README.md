# wikipedia-parser

## Configuration

Copy `config.example.yml` to `config.yml` (or pass a custom path via `-config`) and adjust the values if needed.

```yaml
dump_base_url: https://dumps.wikimedia.org/
kafka_broker: localhost:9092
kafka_topic: wikipedia-pages
work_dir: ./data
status_file: ./data/status.log
sink: kafka # use "stdout" to log messages instead of writing to Kafka
http_addr: :8080
mediawiki_api_url: http://localhost:8080/api.php
max_render_inflight: 4
```

The exporter keeps the names of processed dumps in `status_file`. Restarting the service will skip entries already recorded there. Set `sink` to `stdout` to print each processed page to the console (decoded into a human readable line) instead of sending it to Kafka.

`mediawiki_api_url` must point to a MediaWiki API endpoint (for example `/api.php`) that can render wikitext into HTML. `max_render_inflight` controls how many pages are rendered concurrently by the exporter.

Run the service with:

```
go run ./cmd/wikiparser -config config.yml
```

## Docker Compose

The repository contains a `docker-compose.yml` that starts the exporter alongside a MediaWiki instance configured for HTML rendering. The default configuration file used by the compose stack is `config.docker.yml`.

Build and start the stack:

```
docker compose up -d
```

Verify that both services are running:

```
curl http://localhost:8080/api.php?action=parse\&format=json\&formatversion=2\&contentmodel=wikitext\&text=Hello
curl http://localhost:8081/metrics
```

Once the MediaWiki service responds, trigger a dump export by calling `/start` on the exporter container:

```
curl -X POST http://localhost:8081/start
```

## HTTP API

All control endpoints accept both `GET` and `POST` requests:

* `/start` — start or resume the exporter.
* `/pause` — finish the current file and pause processing.
* `/abort` — stop the current run and reset progress.
* `/metrics` — fetch the current progress metrics.
* `/list` — view processed and pending dump URLs for the active run.
* `/list/all` — on demand discovery of every dump file currently available for download.
