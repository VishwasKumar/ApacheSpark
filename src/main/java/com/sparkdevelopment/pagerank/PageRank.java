package com.sparkdevelopment.pagerank;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import scala.Tuple2;
import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

public final class PageRank {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    private static class addValues implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank").setMaster("local[4]");;
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> data = javaSparkContext.textFile("resources/pageRankInput.0", 1);

        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<String, Iterable<String>> links = data.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) {
                        String[] parts = SPACES.split(s);
                        return new Tuple2<String, String>(parts[0], parts[1]);
                    }
                }).distinct().groupByKey().cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
            @Override
            public Double call(Iterable<String> rs) {
                return 1.0;
            }
        });

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < Integer.parseInt(String.valueOf(10)); current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                        @Override
                        public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
                            int urlCount = Iterables.size(s._1);
                            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
                            for (String n : s._1) {
                                results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
                            }
                            return results;
                        }
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(new addValues()).mapValues(new Function<Double, Double>() {
                @Override
                public Double call(Double sum) {
                    return 0.15 + sum * 0.85;
                }
            });
        }

        // Collects all URL ranks and save them to local.
        ranks.saveAsTextFile("output/pageRankOutput");
        javaSparkContext.stop();
    }
}