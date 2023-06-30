-- TSV input (video_id, title, channel_id, channel_title, published_at, trending_at, view_count, likes, dislikes, comment_count, tags, description, comments)
videos = LOAD 'hdfs://cm:9000/uhadoop2023/proyects/lostilines/youtube_kaggle_clean_dataset.tsv' USING PigStorage('\t') AS (video_id:chararray, title:chararray, channel_id:chararray, channel_title:chararray, published_at:chararray, trending_at:chararray, view_count:chararray, likes:chararray, dislikes:chararray, comment_count:chararray, tags:chararray, description:chararray, comments:chararray);

-- stopwords
stopwords = LOAD 'hdfs://cm:9000/uhadoop2023/proyects/lostilines/stopwords.txt' USING PigStorage('\n') AS (stopword:chararray);

-- Merge title, desc and tags columns as text per year for keyword count
yearText = FOREACH videos GENERATE video_id as video, SUBSTRING(published_at, 0, 4) as year, CONCAT(title, description, tags) as text;

-- split into words by year to perform word count
wordsPerYear = FOREACH yearText GENERATE year, FLATTEN(TOKENIZE(LOWER(text))) as word, video;

joined = JOIN wordsPerYear BY word LEFT OUTER, stopwords BY stopword;

filteredStopWords = FILTER joined BY stopword is null;

filteredWords = FILTER filteredStopWords by (word matches '[\\p{L}]+');

withoutSW = FOREACH filteredWords GENERATE year, word, video;

groupedCount = FOREACH (GROUP withoutSW BY (year, word)) GENERATE group.year as year, group.word as word, COUNT(withoutSW.video) as count;

grouped = GROUP groupedCount BY year;

keywords = FOREACH grouped {
    ordered = ORDER groupedCount BY count DESC;
    limited = LIMIT ordered 10;
    GENERATE FLATTEN(limited);
};

STORE keywords INTO '/uhadoop2023/proyects/lostilines/output/';