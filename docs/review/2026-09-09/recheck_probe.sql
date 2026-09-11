SET threads=1;
SET memory_limit='48GB';
SET ngram_max_candidate_fraction=1;
SET ngram_auto_accelerate=true;
.timer on
SELECT count(*) FROM clustered WHERE s ILIKE '%history%';
SELECT count(*) FROM clustered WHERE s ILIKE '%history%';
SELECT count(*) FROM clustered WHERE s ILIKE '%history%';
SELECT count(*) FROM ngram_search('clustered', 'history');
SELECT count(*) FROM ngram_search('clustered', 'history');
SELECT count(*) FROM ngram_search('clustered', 'history');
SELECT count(*) FROM docs WHERE s ILIKE '%history%';
SELECT count(*) FROM docs WHERE s ILIKE '%history%';
SELECT count(*) FROM ngram_search('docs', 'history');
SELECT count(*) FROM ngram_search('docs', 'history');
.timer off
EXPLAIN (ANALYZE, FORMAT JSON) SELECT count(*) FROM clustered WHERE s ILIKE '%history%';
