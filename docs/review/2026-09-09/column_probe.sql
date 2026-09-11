SET threads=1;
SET memory_limit='48GB';
SET ngram_max_candidate_fraction=1;
.timer on
SELECT count(*) FROM ngram_search('wide', 'parliamentary');
SELECT count(*) FROM ngram_search('wide', 'parliamentary');
SELECT max(c1) FROM ngram_search('wide', 'parliamentary');
SELECT max(c1) FROM ngram_search('wide', 'parliamentary');
SELECT max(c2) FROM ngram_search('wide', 'parliamentary');
SELECT max(c2) FROM ngram_search('wide', 'parliamentary');
SELECT max(c3) FROM ngram_search('wide', 'parliamentary');
SELECT max(c3) FROM ngram_search('wide', 'parliamentary');
SELECT max(c5) FROM ngram_search('wide', 'parliamentary');
SELECT max(c5) FROM ngram_search('wide', 'parliamentary');
SELECT max(id) FROM ngram_search('wide', 'parliamentary');
SELECT max(id) FROM ngram_search('wide', 'parliamentary');
.timer off
SELECT column_name, compression, count(*) FROM pragma_storage_info('wide') WHERE segment_type <> 'VALIDITY' GROUP BY 1, 2 ORDER BY 1, 2;
