-------------------------
-- KeywordsCount
-- TSV input (video_id, title, channel_id, channel_title, published_at, trending_at, view_count, likes, dislikes, comment_count, tags, description, comments)
videos = LOAD 'hdfs://cm:9000/uhadoop2023/proyects/lostilines/youtube_kaggle_sample_dataset.tsv' USING PigStorage('\t') AS (video_id:chararray, title:chararray, channel_id:chararray, channel_title:chararray, published_at:chararray, trending_at:chararray, view_count:chararray, likes:chararray, dislikes:chararray, comment_count:chararray, tags:chararray, description:chararray, comments:chararray);

-- stopwords
stopwords = LOAD 'hdfs://cm:9000/uhadoop2023/proyects/lostilines/stopwords.txt' USING PigStorage('\t') AS (stopword:chararray);

-- Merge title, desc and tags columns as text per year for keyword count
yearText = FOREACH videos GENERATE SUBSTRING(published_at, 0, 4) as year, CONCAT(title, description, tags) as text;

-- split into words by year to perform word count
wordsPerYear = FOREACH yearText GENERATE year, FLATTEN(TOKENIZE(text)) as word;

-- perform word count
keyWordCount = FOREACH (GROUP wordsPerYear BY (year, word)) GENERATE group.year as year, LOWER(group.word) as word, COUNT(wordsPerYear) as count;

-- filter stopwords
joined = JOIN keyWordCount BY word LEFT OUTER, stopwords BY stopword;

filteredStopWords = FILTER joined BY stopword is null;

-- filter special characters and punctuation
filteredWords = FILTER filteredStopWords by (word matches '[\\p{L}]+'); 

groupedData = GROUP filteredWords BY year;

keywords = FOREACH groupedData {
    sorted = ORDER filteredWords BY count DESC;
    limited = LIMIT sorted 5;
    GENERATE FLATTEN(limited);
};

STORE keywords INTO '/uhadoop2023/proyects/lostilines/output/';