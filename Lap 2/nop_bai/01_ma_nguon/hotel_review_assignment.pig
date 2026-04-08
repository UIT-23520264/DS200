-- Pig script for assignments.ipynb (Bai 1 -> Bai 5)

SET default_parallel 1;

raw = LOAD '/workspace/hotel-review.csv' USING PigStorage(';')
    AS (rid:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '/workspace/stopwords.txt' USING PigStorage('\n')
    AS (word:chararray);

normalized = FOREACH raw GENERATE
    rid,
    LOWER(review) AS review_lc,
    UPPER(category) AS category,
    UPPER(aspect) AS aspect,
    LOWER(sentiment) AS sentiment;

-- Bai 1: lowercase + tokenize + remove stopwords
tokenized = FOREACH normalized GENERATE
    rid,
    category,
    aspect,
    sentiment,
    FLATTEN(TOKENIZE(review_lc)) AS word;

non_empty = FILTER tokenized BY word IS NOT NULL AND SIZE(TRIM(word)) > 0;
joined_stop = JOIN non_empty BY word LEFT OUTER, stopwords BY word;
filtered_words = FILTER joined_stop BY stopwords::word IS NULL;

words = FOREACH filtered_words GENERATE
    non_empty::rid AS rid,
    non_empty::category AS category,
    non_empty::aspect AS aspect,
    non_empty::sentiment AS sentiment,
    non_empty::word AS word;

-- Bai 2.1: word frequency > 500
word_group = GROUP words BY word;
word_freq = FOREACH word_group GENERATE group AS word, COUNT(words) AS freq;
word_freq_gt_500 = FILTER word_freq BY freq > 500;

-- Bai 2.2: comment count by category
cat_group = GROUP raw BY category;
count_by_category = FOREACH cat_group {
    rid_only = FOREACH raw GENERATE rid;
    rid_distinct = DISTINCT rid_only;
    GENERATE
        group AS category,
        COUNT(raw) AS total_rows,
        COUNT(rid_distinct) AS distinct_comments;
};

-- Bai 2.3: comment count by aspect
aspect_group = GROUP raw BY aspect;
count_by_aspect = FOREACH aspect_group {
    rid_only = FOREACH raw GENERATE rid;
    rid_distinct = DISTINCT rid_only;
    GENERATE
        group AS aspect,
        COUNT(raw) AS total_rows,
        COUNT(rid_distinct) AS distinct_comments;
};

-- Bai 3: aspect with most negative and most positive
neg_rows = FILTER normalized BY sentiment == 'negative';
pos_rows = FILTER normalized BY sentiment == 'positive';

neg_aspect_group = GROUP neg_rows BY aspect;
neg_aspect_count = FOREACH neg_aspect_group GENERATE group AS aspect, COUNT(neg_rows) AS neg_count;
neg_aspect_sorted = ORDER neg_aspect_count BY neg_count DESC, aspect ASC;
most_negative_aspect = LIMIT neg_aspect_sorted 1;

pos_aspect_group = GROUP pos_rows BY aspect;
pos_aspect_count = FOREACH pos_aspect_group GENERATE group AS aspect, COUNT(pos_rows) AS pos_count;
pos_aspect_sorted = ORDER pos_aspect_count BY pos_count DESC, aspect ASC;
most_positive_aspect = LIMIT pos_aspect_sorted 1;

-- Pre-aggregate word counts by category + sentiment
cat_sent_word_group = GROUP words BY (category, sentiment, word);
cat_sent_word_count = FOREACH cat_sent_word_group GENERATE
    group.category AS category,
    group.sentiment AS sentiment,
    group.word AS word,
    COUNT(words) AS cnt;

-- Bai 4.1: top 5 positive words by category
pos_counts = FILTER cat_sent_word_count BY sentiment == 'positive';
pos_counts_by_category = GROUP pos_counts BY category;

top5_positive_words_by_category = FOREACH pos_counts_by_category {
    ordered = ORDER pos_counts BY cnt DESC, word ASC;
    top5 = LIMIT ordered 5;
    GENERATE group AS category, FLATTEN(top5.(word, cnt));
};

-- Bai 4.2: top 5 negative words by category
neg_counts = FILTER cat_sent_word_count BY sentiment == 'negative';
neg_counts_by_category = GROUP neg_counts BY category;

top5_negative_words_by_category = FOREACH neg_counts_by_category {
    ordered = ORDER neg_counts BY cnt DESC, word ASC;
    top5 = LIMIT ordered 5;
    GENERATE group AS category, FLATTEN(top5.(word, cnt));
};

-- Bai 5: top 5 related words by category (highest frequency)
cat_word_group = GROUP words BY (category, word);
cat_word_count = FOREACH cat_word_group GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(words) AS cnt;

cat_word_by_category = GROUP cat_word_count BY category;

top5_related_words_by_category = FOREACH cat_word_by_category {
    ordered = ORDER cat_word_count BY cnt DESC, word ASC;
    top5 = LIMIT ordered 5;
    GENERATE group AS category, FLATTEN(top5.(word, cnt));
};

-- Store outputs to /workspace so host machine can access via ./workspace
rmf '/workspace/output/bai1_words';
STORE words INTO '/workspace/output/bai1_words' USING PigStorage('\t');

rmf '/workspace/output/bai2_word_freq_gt_500';
STORE word_freq_gt_500 INTO '/workspace/output/bai2_word_freq_gt_500' USING PigStorage('\t');

rmf '/workspace/output/bai2_count_by_category';
STORE count_by_category INTO '/workspace/output/bai2_count_by_category' USING PigStorage('\t');

rmf '/workspace/output/bai2_count_by_aspect';
STORE count_by_aspect INTO '/workspace/output/bai2_count_by_aspect' USING PigStorage('\t');

rmf '/workspace/output/bai3_most_negative_aspect';
STORE most_negative_aspect INTO '/workspace/output/bai3_most_negative_aspect' USING PigStorage('\t');

rmf '/workspace/output/bai3_most_positive_aspect';
STORE most_positive_aspect INTO '/workspace/output/bai3_most_positive_aspect' USING PigStorage('\t');

rmf '/workspace/output/bai4_top5_positive_words_by_category';
STORE top5_positive_words_by_category INTO '/workspace/output/bai4_top5_positive_words_by_category' USING PigStorage('\t');

rmf '/workspace/output/bai4_top5_negative_words_by_category';
STORE top5_negative_words_by_category INTO '/workspace/output/bai4_top5_negative_words_by_category' USING PigStorage('\t');

rmf '/workspace/output/bai5_top5_related_words_by_category';
STORE top5_related_words_by_category INTO '/workspace/output/bai5_top5_related_words_by_category' USING PigStorage('\t');
