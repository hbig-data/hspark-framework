package com.hspark.job.ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/13 17:54.
 */
public class SparkMLExample {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "textTest", "", new String[]{});

        JavaRDD<String> textFile = sc.textFile("hdfs://...");

        JavaRDD<Row> data = textFile.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                return RowFactory.create(line);
            }
        });



        // Every record of this DataFrame contains the label and
        // features represented by a vector.
        SQLContext sqlContext = new SQLContext(sc);
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        DataFrame df = sqlContext.createDataFrame(data, schema);

        // Set parameters for the algorithm.
        // Here, we limit the number of iterations to 10.
        LogisticRegression lr = new LogisticRegression().setMaxIter(10);

        // Fit the model to the data.
        LogisticRegressionModel model = lr.fit(df);

        // Inspect the model: get the feature weights.
        Vector weights = model.weights();

        // Given a dataset, predict each point's label, and show the results.
        model.transform(df).show();
    }
}
