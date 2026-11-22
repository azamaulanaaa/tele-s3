#!/usr/bin/env bash
# Set the crate version in Cargo.toml and Cargo.lock.
set -euo pipefail

version="${1:?usage: bump-cargo-version.sh <version>}"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

awk -v version="$version" '
  /^\[/ { in_package = ($0 == "[package]") }
  in_package && /^version = / && !done {
    print "version = \"" version "\""
    done = 1
    next
  }
  { print }
' Cargo.toml >"$tmp"
mv "$tmp" Cargo.toml

awk -v version="$version" '
  /^name = "tele-s3"$/ {
    print
    getline
    print "version = \"" version "\""
    next
  }
  { print }
' Cargo.lock >"$tmp"
mv "$tmp" Cargo.lock
