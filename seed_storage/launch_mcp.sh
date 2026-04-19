#!/usr/bin/env bash
# Launch the seed-storage MCP server with credentials resolved from ant-keeper's postgres.
# Used by Claude Code's MCP server config in ~/.claude/settings.json.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PG_DSN="${PG_DSN:-postgresql://taskman:postgres@127.0.0.1:30433/task_manager}"

# Resolve encrypted credentials from ant-keeper's postgres and decrypt them.
# ant-keeper uses Fernet encryption with TOKEN_ENCRYPTION_KEY from k8s secret.
ENCRYPTION_KEY=$(kubectl --kubeconfig /opt/shared/k3s/kubeconfig.yaml \
  get secret ant-keeper-secrets -n ant-keeper \
  -o jsonpath='{.data.TOKEN_ENCRYPTION_KEY}' | base64 -d)

# Query credentials and decrypt
eval "$(cd "$SCRIPT_DIR" && uv run python3 -c "
import os, sys
os.environ['PG_DSN'] = '$PG_DSN'
os.environ['TOKEN_ENCRYPTION_KEY'] = '$ENCRYPTION_KEY'

from cryptography.fernet import Fernet
import psycopg2

f = Fernet(os.environ['TOKEN_ENCRYPTION_KEY'].encode())
conn = psycopg2.connect('$PG_DSN')
cur = conn.cursor()

for cred_id, env_name in [('anthropic', 'ANTHROPIC_API_KEY'), ('gemini', 'GEMINI_API_KEY')]:
    cur.execute('SELECT encrypted_value FROM credentials WHERE credential_id = %s LIMIT 1', (cred_id,))
    row = cur.fetchone()
    if row:
        raw = row[0]
        if isinstance(raw, memoryview):
            raw = bytes(raw)
        val = f.decrypt(raw).decode()
        print(f'export {env_name}=\"{val}\"')

conn.close()
")"

export NEO4J_URI="${NEO4J_URI:-bolt://127.0.0.1:30687}"
export NEO4J_USER="${NEO4J_USER:-neo4j}"
export NEO4J_PASSWORD="${NEO4J_PASSWORD:-seedstorage2026}"
export PG_DSN="$PG_DSN"

cd "$SCRIPT_DIR"
exec uv run python -m seed_storage.mcp_server
