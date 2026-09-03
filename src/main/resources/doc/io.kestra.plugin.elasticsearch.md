# How to use the Elasticsearch plugin

Query, index, and bulk-ingest documents across Elasticsearch clusters from Kestra flows.

## Authentication

Configure the cluster connection on each task via `connection.hosts` (a list of `scheme://host:port` endpoints, e.g. `https://localhost:9200` — the scheme is required). For secured clusters, set `connection.basicAuth.username` and `connection.basicAuth.password` for HTTP basic auth, or pass an API key via `connection.headers` as a `"Authorization: ApiKey <token>"` entry. For self-signed certificates, set `trustAllSsl: true` — use only in non-production environments. Store credentials in [secrets](https://kestra.io/docs/concepts/secret).

## Common properties

Set `hosts` and any auth properties on each task.

## Tasks

`Search` runs a query DSL request against an index and returns matching documents. For large result sets that exceed a single page, use `Scroll` instead — it pages through all matching documents using the Elasticsearch scroll API and streams them to Kestra internal storage. `Esql` runs an ES|QL query and is the right choice for SQL-style analytics and aggregations on Elasticsearch data.

For writes, `Put` indexes or replaces a single document by ID. `Bulk` replays an Elasticsearch bulk-format file (NDJSON or ION) from Kestra internal storage, applying its index/create/update/delete operations in batched requests — use it to replay a prepared bulk payload. `Load` reads ION records from a Kestra internal storage file and bulk-indexes them (index operations only), making it the natural follow-on after a download or transform step.

`Get` retrieves a single document by ID. `Request` sends a raw HTTP request to any Elasticsearch REST API endpoint — use it for operations not covered by a dedicated task.
