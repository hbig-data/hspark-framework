package org.apache.spark.examples.ml;

import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * An example demonstrating Gaussian Mixture Model.
 * Run with
 * <pre>
 * bin/run-example ml.JavaGaussianMixtureExample
 * </pre>
 */
public class JavaGaussianMixtureExample {

    public static void main(String[] args) {

        // Creates a SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaGaussianMixtureExample")
                .getOrCreate();


        // Loads data
        Dataset<Row> dataset = spark.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");

        // Trains a GaussianMixture model
        GaussianMixture gmm = new GaussianMixture().setK(2);
        GaussianMixtureModel model = gmm.fit(dataset);

        // Output the parameters of the mixture model
        for (int i = 0; i < model.getK(); i++) {
            System.out.printf("weight=%f\nmu=%s\nsigma=\n%s\n", model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
        }


        spark.stop();
    }
}
