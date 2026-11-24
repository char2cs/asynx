#!/usr/bin/env bash
# ap-install.sh -> installs the `ap` binary pinned in .ap.yaml into $GOBIN-ish dir
# (default ~/.local/bin) so the lefthook pre-commit gate can run it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(grep '^version:' "$ROOT/.ap.yaml" | head -1 | awk '{print $2}')"
DEST="${AP_INSTALL_DIR:-$HOME/.local/bin}"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$DEST"
curl -fsSL "https://github.com/rabbytesoftware/agent-protection/releases/download/${VERSION}/ap_${OS}_${ARCH}.tar.gz" \
  -o "$TMP/ap.tar.gz"
tar -xzf "$TMP/ap.tar.gz" -C "$TMP" ap
install -m 0755 "$TMP/ap" "$DEST/ap"
echo "installed ap ${VERSION} to $DEST/ap"
