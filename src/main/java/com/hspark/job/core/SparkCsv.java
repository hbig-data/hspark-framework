package com.hspark.job.core;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Spark 解析 CSV  文件
 *
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/8/22 11:48.
 */
public class SparkCsv implements Serializable {

    public static void main(String[] args) {

        JavaSparkContext javaSparkContext = new JavaSparkContext("local[1]", "sparkCSV", "", "");

        SQLContext sqlContext = new SQLContext(javaSparkContext.sc());

        StructType customSchema = new StructType(new StructField[]{
                new StructField("year", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("make", DataTypes.StringType, true, Metadata.empty()),
                new StructField("model", DataTypes.StringType, true, Metadata.empty()),
                new StructField("comment", DataTypes.StringType, true, Metadata.empty()),
                new StructField("blank", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> load = sqlContext.read().format("com.databricks.spark.csv")
                .schema(customSchema)
                .option("inferSchema", "true")
                .option("header", "true")
                .load("cars.csv");


        load.select("year", "model").write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("newcars.csv");

    }
}
