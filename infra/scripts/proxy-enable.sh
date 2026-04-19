#!/usr/bin/env bash
# Configure proxy_target for a proxy-enabled credential in ant-keeper.
#
# MUST be run BEFORE registering the seed-storage daemon with ant-keeper.
# Required for env-mode proxy-enabled credentials: openai, github-pat
#
# If the daemon is registered first, ant-keeper will fail on deploy with:
#   "Credential '<name>' missing proxy_target"
#
# Usage:
#   ./infra/scripts/proxy-enable.sh <credential-name> <upstream-url>
#
# Required credentials (run once per deployment environment):
#   ./infra/scripts/proxy-enable.sh openai https://api.openai.com
#   ./infra/scripts/proxy-enable.sh github-pat https://api.github.com

set -euo pipefail

ANTKEEPER_API="${ANTKEEPER_API:-http://localhost:7070}"

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <credential-name> <upstream-url>" >&2
    echo "" >&2
    echo "Required for seed-storage deploy:" >&2
    echo "  $0 openai https://api.openai.com" >&2
    echo "  $0 github-pat https://api.github.com" >&2
    exit 1
fi

CREDENTIAL_NAME="$1"
UPSTREAM_URL="$2"

echo "Configuring proxy_target for credential '${CREDENTIAL_NAME}' → ${UPSTREAM_URL}"

curl -sf -X PATCH \
    "${ANTKEEPER_API}/api/credentials/${CREDENTIAL_NAME}" \
    -H "Content-Type: application/json" \
    -d "{\"proxy_target\": \"${UPSTREAM_URL}\"}"

echo ""
echo "Done. Credential '${CREDENTIAL_NAME}' proxy_target set to '${UPSTREAM_URL}'"
