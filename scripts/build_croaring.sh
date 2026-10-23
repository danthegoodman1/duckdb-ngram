#!/usr/bin/env bash
# Install CRoaring, which DuckLake links, under build/deps/croaring for a build
# configured with NGRAM_DUCKLAKE=1 (see extension_config.cmake).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
deps="$root/build/deps"
version="v4.6.1"

if [ -f "$deps/croaring/lib/cmake/roaring/roaring-config.cmake" ]; then
    exit 0
fi
mkdir -p "$deps"
rm -rf "$deps/croaring-src" "$deps/croaring-build"
git clone --quiet --depth 1 --branch "$version" https://github.com/RoaringBitmap/CRoaring.git "$deps/croaring-src"
cmake -S "$deps/croaring-src" -B "$deps/croaring-build" -DCMAKE_BUILD_TYPE=Release -DENABLE_ROARING_TESTS=OFF \
    -DROARING_BUILD_STATIC=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX="$deps/croaring"
cmake --build "$deps/croaring-build" --parallel
cmake --install "$deps/croaring-build"
rm -rf "$deps/croaring-src" "$deps/croaring-build"
