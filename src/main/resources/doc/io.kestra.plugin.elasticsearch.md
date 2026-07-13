# How to use the Elasticsearch plugin

Query, index, and bulk-ingest documents across Elasticsearch clusters from Kestra flows.

## Authentication

Configure the cluster connection on each task via `hosts` (a list of `host:port` entries). For secured clusters, set `basicAuth.username` and `basicAuth.password` for HTTP basic auth, or pass an API key via `headers` as a `"Authorization: ApiKey <token>"` entry. For self-signed certificates, set `trustAllSsl: true` — use only in non-production environments. Store credentials in [secrets](https://kestra.io/docs/concepts/secret).

## Common properties

Set `hosts` and any auth properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) if all tasks in a flow target the same cluster.

## Tasks

`Search` runs a query DSL request against an index and returns matching documents. For large result sets that exceed a single page, use `Scroll` instead — it pages through all matching documents using the Elasticsearch scroll API and streams them to Kestra internal storage. `Esql` runs an ES|QL query and is the right choice for SQL-style analytics and aggregations on Elasticsearch data.

For writes, `Put` indexes or replaces a single document by ID. `Bulk` performs batched index, update, and delete operations in a single request — use it for any multi-document write to avoid per-document round trips. `Load` reads records from a Kestra internal storage file and bulk-indexes them, making it the natural follow-on after a download or transform step.

`Get` retrieves a single document by ID. `Request` sends a raw HTTP request to any Elasticsearch REST API endpoint — use it for operations not covered by a dedicated task.
