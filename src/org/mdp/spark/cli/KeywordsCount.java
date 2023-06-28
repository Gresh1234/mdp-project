package org.mdp.spark.cli;

import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

import scala.Tuple2;

/**
 * Count and get Keywords for youtube videos per year
 */
public class KeywordsCount {
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new KeywordsCount().run(args[0], args[1]);
	}

	/**
	 * The task body
	 */
	public void run(String inputFilePath, String outputFilePath) {
		/*
		 * Initialises a Spark context with the name of the application
		 *   and the (default) master settings.
		 */
		SparkConf conf = new SparkConf()
				.setAppName(KeywordsCount.class.getName());
		JavaSparkContext context = new JavaSparkContext(conf);

		String[] stopwords_arr = new String[] {"this", "is", "a", "an", "or", "but", "and", "to", "-"};
		List<String> stopwords = Arrays.asList(stopwords_arr);

		/*
		 * Load the first RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> inputRDD = context.textFile(inputFilePath);

		// Turn the RDD to JavaPairRDD with the date as key and text as value 
		JavaPairRDD<String, String> yearTextPairRDD = inputRDD.mapToPair(
			line -> new Tuple2<String, String> (
				line.split("\t")[4].substring(0, 4),
				line.split("\t")[1] + " " + line.split("\t")[11] + " " + line.split("\t")[10]
			)
		);

		// Group RDD by year
		JavaPairRDD<String, Iterable<String>> groupedRDD = yearTextPairRDD.groupByKey(); 


		JavaRDD<Tuple2<String, List<String>>> keywordsByYearRDD = groupedRDD.map(yearTexts -> {
            String year = yearTexts._1;
            Iterable<String> textList = yearTexts._2;

            // Combine all text values into a single string
            StringBuilder combinedText = new StringBuilder();
            for (String text : textList) {
                combinedText.append(text).append(" ");
            }
			String text = combinedText.toString();
            // // Split the combined text into words
            String[] words = text.trim().toLowerCase().split(" ");

            // Perform word frequency analysis
            Counter<String> counter = new Counter<>();
            for (String word : words) {
				if (!stopwords.contains(word)) {
					counter.incrementCount(word+"sw");
				}
				else {
					counter.incrementCount(word);
				}
            }

            // Get the most common words
            List<String> mostUsedWords = counter.mostCommon(5);

            return new Tuple2<String, List<String>>(year, mostUsedWords);
        });
		
		keywordsByYearRDD.saveAsTextFile(outputFilePath);
		
		context.close();
	}
}

class Counter<T> {
        private final java.util.Map<T, Integer> map = new java.util.HashMap<>();

        public void incrementCount(T item) {
            map.put(item, map.getOrDefault(item, 0) + 1);
        }

        public List<T> mostCommon(int n) {
            return map.entrySet().stream()
                    .sorted(Comparator.comparingInt(java.util.Map.Entry<T, Integer>::getValue).reversed())
                    .limit(n)
                    .map(java.util.Map.Entry::getKey)
                    .collect(java.util.stream.Collectors.toList());
        }
    }
