#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

if [[ "${KESTRA_ELASTICSEARCH_USE_DOCKER_COMPOSE:-false}" == "true" ]]; then
    docker compose -f "${repo_root}/docker-compose-ci.yml" up -d
    sleep 30
else
    docker compose -f "${repo_root}/docker-compose-ci.yml" down >/dev/null 2>&1 || true
    echo "Skipping docker-compose Elasticsearch setup; tests use Testcontainers."
fi
