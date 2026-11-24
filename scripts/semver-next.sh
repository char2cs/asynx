#!/usr/bin/env bash
# semver-next.sh v1.2.3 patch|minor|major -> prints the next tag.
set -euo pipefail

LATEST="${1:?usage: semver-next.sh <latest-tag> <patch|minor|major>}"
BUMP="${2:?usage: semver-next.sh <latest-tag> <patch|minor|major>}"

VERSION="${LATEST#v}"
IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION"

case "$BUMP" in
  major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
  minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
  patch) PATCH=$((PATCH + 1)) ;;
  *) echo "unknown bump type: $BUMP" >&2; exit 1 ;;
esac

echo "v${MAJOR}.${MINOR}.${PATCH}"
