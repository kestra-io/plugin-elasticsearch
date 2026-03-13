# Kestra Elasticsearch Plugin

## What

Connect Elasticsearch search and analytics engine to Kestra workflows. Exposes 8 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Elasticsearch, allowing orchestration of Elasticsearch-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
