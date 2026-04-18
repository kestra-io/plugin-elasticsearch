# Kestra Elasticsearch Plugin

## What

- Provides plugin components under `io.kestra.plugin.elasticsearch`.
- Includes classes such as `ElasticsearchConnection`, `Request`, `ElasticsearchService`, `Load`.

## Why

- This plugin integrates Kestra with Elasticsearch.
- It provides tasks that query and write to Elasticsearch clusters for search, indexing, bulk ingestion, and ES|QL.

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
