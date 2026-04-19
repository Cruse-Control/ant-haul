#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Setting up seed-storage..."

# Python venv
if [ ! -d .venv ]; then
    echo "    Creating virtualenv..."
    python3 -m venv .venv
fi
source .venv/bin/activate

echo "    Installing Python dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Verify Neo4j is reachable
echo "    Checking Neo4j connectivity..."
if curl -sf http://127.0.0.1:30474/ > /dev/null 2>&1; then
    echo "    Neo4j is up (http://127.0.0.1:30474)"
else
    echo "    WARNING: Neo4j not reachable at 127.0.0.1:30474"
    echo "    Deploy with: kubectl apply -f k8s/neo4j.yaml"
fi

# Initialize schema if Neo4j is up
if curl -sf http://127.0.0.1:30474/ > /dev/null 2>&1; then
    echo "    Initializing Neo4j schema (indexes + constraints)..."
    python3 -c "
import asyncio
from seed_storage.graph import init_schema, close
async def main():
    await init_schema()
    await close()
    print('    Schema initialized.')
asyncio.run(main())
"
fi

echo ""
echo "==> Done. To run the API:"
echo "    source .venv/bin/activate"
echo "    export GEMINI_API_KEY=<your-key>"
echo "    python -m seed_storage.api"
