"""Process, registry and oracle helpers shared by the Python drivers.

Every driver runs the duckdb CLI one script at a time, reads CSV back, finds
the corpus index through the public listing, and compares an accelerated
answer with a brute-force one. Those pieces live here so that the format
lookup, the storage digests and the multiset oracle exist once; each driver
keeps its own corpus generation, so its seeds reproduce the same runs.
"""

import csv
import os
import re
import subprocess

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DUCKDB = os.path.join(REPO, "build", "release", "duckdb")
FORMAT_VERSION = "5"
UUID_V4 = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
LISTING_COLUMNS = 8


def sql_quote(text):
    return "'" + text.replace("'", "''") + "'"


def storage_table(ref, part):
    """The qualified name of one storage table of the index `ref`."""
    return "__ngram.%s_%s" % (part, ref.replace("-", ""))


def multiset_mismatch(left, right):
    """A count of the rows on which two queries disagree as multisets: rows one
    side returns more often than the other, in either direction. A duplicated
    accelerated row counts, which a set comparison would hide."""
    return ("SELECT count(*) FROM ((%s EXCEPT ALL %s) UNION ALL (%s EXCEPT ALL %s))"
            % (left, right, right, left))


def index_state_sql(ref):
    """The facts a half-applied maintenance operation would disagree on: the
    mark and every segment row's identity and bounds."""
    return ("SELECT (SELECT hwm_rowid FROM __ngram.registry WHERE index_id = %s::UUID), "
            "s.n, s.rows, s.hash_sum, s.hash_xor FROM (SELECT count(*) AS n, "
            "coalesce(sum(rowid_count), 0) AS rows, "
            "coalesce(sum(hash(gram_key, segment_no, generation, rowid_count, min_rowid, max_rowid))"
            "::VARCHAR, '0') AS hash_sum, "
            "coalesce(bit_xor(hash(gram_key, segment_no, generation, rowid_count, min_rowid, max_rowid))"
            "::VARCHAR, '0') AS hash_xor FROM %s) s;" % (sql_quote(ref), storage_table(ref, "segments")))


def postings_digest_sql(ref):
    """The decoded index itself: every (gram_key, rowid) posting, summarised so
    two databases can be compared without materialising both."""
    return ("SELECT count(*), coalesce(sum(hash(gram_key || ':' || r))::VARCHAR, '0') "
            "FROM ngram_unpack_postings((SELECT gram_key, segment_no, postings FROM %s));"
            % storage_table(ref, "segments"))


class Cli:
    """One duckdb process per script. Reopening the database on every call is
    part of what the drivers test: checkpoint on close and WAL replay."""

    def __init__(self, binary=DEFAULT_DUCKDB, timeout=900):
        self.binary = binary
        self.timeout = timeout

    def run(self, db_path, script, allow_error=False, timeout=None, attached=True):
        """(exit code, stdout, stderr) of the script in CSV mode without headers."""
        proc = subprocess.run([self.binary] + ([db_path] if attached else []),
                              input=".headers off\n.mode csv\n" + script,
                              capture_output=True, text=True, timeout=timeout or self.timeout)
        if proc.returncode != 0 and not allow_error:
            raise RuntimeError("duckdb failed on %s:\n%s\n--- script ---\n%s"
                               % (db_path, proc.stderr[-4000:], script[:2000]))
        return proc.returncode, proc.stdout, proc.stderr

    def rows(self, db_path, script):
        return list(csv.reader(self.run(db_path, script)[1].splitlines()))

    def last_line(self, db_path, script):
        lines = [line for line in self.run(db_path, script)[1].strip().splitlines() if line]
        return lines[-1] if lines else None

    def index_ref(self, db_path, table="corpus", column="s", statuses=("READY",)):
        """The reference of the one current-format index on table.column in the
        database's default catalog, from the public listing."""
        catalog = self.rows(db_path, "SELECT current_database();")[0][0]
        rows = [row for row in self.rows(db_path, "PRAGMA ngram_indexes;")
                if len(row) == LISTING_COLUMNS and row[0] == catalog
                and row[2:5] == ["main", table, column] and row[5] == FORMAT_VERSION and row[6] in statuses]
        if len(rows) != 1:
            raise RuntimeError("expected one format-%s %s.%s index in %s, found %d"
                               % (FORMAT_VERSION, table, column, ", ".join(statuses), len(rows)))
        ref = rows[0][1]
        if not UUID_V4.fullmatch(ref):
            raise RuntimeError("public %s.%s index id is not a canonical UUIDv4" % (table, column))
        return ref

    def index_state(self, db_path, ref):
        return self.last_line(db_path, index_state_sql(ref))

    def postings_digest(self, db_path, ref):
        return self.last_line(db_path, postings_digest_sql(ref))
