#!/bin/bash
# A stock DuckDB v1.5.5 CLI installs the loadable artifact from a repository
# in DuckDB's layout and uses it across three processes. Run from the
# repository root after a release build:
#
#   bash docs/review/2026-09-09/install_check.sh /path/to/stock/duckdb /path/to/scratch
#
# The recorded transcript is install_check.log beside this script.
set -u
STOCK=${1:?stock duckdb CLI}
SCRATCH=${2:?scratch directory}
REPO="$SCRATCH/repo"
DB="$SCRATCH/install-test.db"
EXTDIR="$SCRATCH/extdir"
rm -rf "$REPO" "$DB" "$DB.wal" "$EXTDIR"
mkdir -p "$REPO/v1.5.5/linux_amd64" "$EXTDIR"
gzip -c build/release/extension/ngram/ngram.duckdb_extension > "$REPO/v1.5.5/linux_amd64/ngram.duckdb_extension.gz"
echo "artifact sha256: $(sha256sum build/release/extension/ngram/ngram.duckdb_extension | cut -c1-64)"
echo "stock cli: $("$STOCK" --version) sha256 $(sha256sum "$STOCK" | cut -c1-64)"
run() { "$STOCK" -unsigned -csv "$DB"; }
echo "== process 1: install, load, build, search"
run <<EOF
SET extension_directory='$EXTDIR';
SET custom_extension_repository='$REPO';
INSTALL ngram;
LOAD ngram;
SELECT extension_name, loaded, installed, install_mode FROM duckdb_extensions() WHERE extension_name = 'ngram';
CREATE TABLE docs AS SELECT i AS id, 'row ' || i || CASE WHEN i % 7 = 0 THEN ' needle' ELSE '' END AS s FROM range(50000) t(i);
PRAGMA create_ngram_index('docs', 's');
SELECT count(*) FROM ngram_search('docs', 'needle');
SELECT count(*) FROM docs WHERE contains(s, 'needle');
CHECKPOINT;
EOF
echo "== process 2: reopen, status, covered update, tail insert, refresh, drop"
run <<EOF
SET extension_directory='$EXTDIR';
LOAD ngram;
SELECT table_name, column_name, format_version, status FROM ngram_indexes();
UPDATE docs SET s = s || ' needle' WHERE id = 1;
INSERT INTO docs VALUES (50000, 'tail needle');
SELECT count(*) FROM ngram_search('docs', 'needle');
SELECT count(*) FROM docs WHERE contains(s, 'needle');
PRAGMA ngram_refresh('docs');
SELECT count(*) FROM ngram_search('docs', 'needle');
SELECT hwm_rowid, remaining_tail, stale_reason FROM ngram_index_stats('docs');
SET VARIABLE ref = (SELECT index_ref FROM ngram_indexes());
PRAGMA drop_ngram_index(getvariable('ref'));
SELECT count(*) FROM ngram_indexes();
SELECT count(*) FROM duckdb_tables() WHERE schema_name = '__ngram' AND table_name <> 'registry';
SELECT count(*) FROM duckdb_indexes();
SELECT count(*) FROM docs;
EOF
echo "== process 3: extension-free read of the same file"
"$STOCK" -csv "$DB" <<EOF
SELECT count(*) FROM docs WHERE contains(s, 'needle');
EOF
echo "INSTALL_CHECK_DONE"
