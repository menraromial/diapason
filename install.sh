#!/bin/sh
# Installs the diapason binary from its GitHub releases.
#
#   curl -fsSL https://raw.githubusercontent.com/menraromial/diapason/main/install.sh | sh
#
# Environment:
#   DIAPASON_VERSION      release tag to install, e.g. v0.1.0 (default: the latest)
#   DIAPASON_INSTALL_DIR  where to put the binary (default: $HOME/.local/bin)
#   DIAPASON_BASE_URL     where the archives are (default: the GitHub release)
set -eu

REPO="menraromial/diapason"
VERSION="${DIAPASON_VERSION:-latest}"
INSTALL_DIR="${DIAPASON_INSTALL_DIR:-$HOME/.local/bin}"

say() { printf 'diapason-install: %s\n' "$*" >&2; }
fail() { say "$*"; exit 1; }

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64 | Linux-amd64) target=x86_64-unknown-linux-musl ;;
    Linux-aarch64 | Linux-arm64) target=aarch64-unknown-linux-musl ;;
    Darwin-x86_64) target=x86_64-apple-darwin ;;
    Darwin-arm64) target=aarch64-apple-darwin ;;
    *) fail "no prebuilt binary for $(uname -s) $(uname -m); build from source with: cargo install --git https://github.com/$REPO diapason-cli" ;;
esac

if [ -n "${DIAPASON_BASE_URL:-}" ]; then
    base="$DIAPASON_BASE_URL"
elif [ "$VERSION" = latest ]; then
    base="https://github.com/$REPO/releases/latest/download"
else
    base="https://github.com/$REPO/releases/download/$VERSION"
fi
asset="diapason-$target.tar.gz"

if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -qO "$2" "$1"; }
else
    fail "curl or wget is needed"
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

say "downloading $base/$asset"
fetch "$base/$asset" "$tmp/$asset" || fail "download failed: $base/$asset"
fetch "$base/$asset.sha256" "$tmp/$asset.sha256" || fail "download failed: $base/$asset.sha256"

expected=$(cut -d ' ' -f 1 "$tmp/$asset.sha256")
if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$tmp/$asset" | cut -d ' ' -f 1)
elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$tmp/$asset" | cut -d ' ' -f 1)
else
    fail "sha256sum or shasum is needed to check the download"
fi
[ "$expected" = "$actual" ] || fail "checksum mismatch for $asset: expected $expected, got $actual"

tar xzf "$tmp/$asset" -C "$tmp"
mkdir -p "$INSTALL_DIR"
cp "$tmp/diapason-$target/diapason" "$INSTALL_DIR/diapason.new"
chmod 755 "$INSTALL_DIR/diapason.new"
mv "$INSTALL_DIR/diapason.new" "$INSTALL_DIR/diapason"

say "installed $("$INSTALL_DIR/diapason" --version) in $INSTALL_DIR"
case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *) say "add $INSTALL_DIR to your PATH, e.g.: export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac
