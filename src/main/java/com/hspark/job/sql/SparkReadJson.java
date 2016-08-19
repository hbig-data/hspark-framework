package com.hspark.job.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Spark  读取 JSON数据
 *
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/7 16:29.
 */
public class SparkReadJson {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("readJson");
        conf.set("spark.sql.dialect", "sql");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        SQLContext sc = new SQLContext(sparkContext);
        String path = "file:///E:/test/spark-sql.json";

        //指定数据格式加载数据
        Dataset<Row> df = sc.read().format("json").load(path);

        df.registerTempTable("test");

        //直接对文件使用 SQL
        df = sc.sql("select * from test");
        df.show();
        df.select("name", "age").write().parquet("file:///E:/test/jsonTest.parquet");

        sparkContext.close();


    }

}
