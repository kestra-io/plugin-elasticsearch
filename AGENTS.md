# Kestra Elasticsearch Plugin

## What

- Provides plugin components under `io.kestra.plugin.elasticsearch`.
- Includes classes such as `ElasticsearchConnection`, `Request`, `ElasticsearchService`, `Load`.

## Why

- What user problem does this solve? Teams need to query and write to Elasticsearch clusters for search, indexing, bulk ingestion, and ES|QL from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Elasticsearch steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Elasticsearch.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `elasticsearch`

Infrastructure dependencies (Docker Compose services):

- `elasticsearch`

### Key Plugin Classes

- `io.kestra.plugin.elasticsearch.Bulk`
- `io.kestra.plugin.elasticsearch.Esql`
- `io.kestra.plugin.elasticsearch.Get`
- `io.kestra.plugin.elasticsearch.Load`
- `io.kestra.plugin.elasticsearch.Put`
- `io.kestra.plugin.elasticsearch.Request`
- `io.kestra.plugin.elasticsearch.Scroll`
- `io.kestra.plugin.elasticsearch.Search`

### Project Structure

```
plugin-elasticsearch/
├── src/main/java/io/kestra/plugin/elasticsearch/model/
├── src/test/java/io/kestra/plugin/elasticsearch/model/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
