package com.sparkdevelopment.kmeansClustering;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class KMeansClustering {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("K Means Clustering");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> data = javaSparkContext.textFile("resources/kmeansInput.txt");
        JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
            @Override
            public Vector call(String s) throws Exception {
                String[] stringArray = s.split(" ");
                double[] values = new double[stringArray.length];
                for (int i = 0; i < stringArray.length; i++)
                    values[i] = Double.parseDouble(stringArray[i]);
                return Vectors.dense(values);
            }
        }).cache();

        // Cluster the data into two classes using KMeansClustering
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
    }
}
