package com.hspark.job.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * <pre>
 * User:        Ryan
 * Date:        2017/6/5
 * Email:       liuwei412552703@163.com
 * Version      V1.0
 * Discription:
 */
public class SparkSocketWriteData {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("write date append");
        sparkConf.setMaster("local[2]");

        final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(jsc);

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(jsc, Duration.apply(3000));
        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("192.168.1.107", 19099);


        final StructType structType = new StructType();
        structType.add("word", DataTypes.StringType, true);
        structType.add("sum", DataTypes.IntegerType, true);

        dStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> javaRDD) throws Exception {
                Dataset<Row> dataFrame = sqlContext.createDataFrame(javaRDD, String.class);
                dataFrame.write().mode(SaveMode.Append).save("E:/ttt/");
            }
        });


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();


        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                jsc.close();
            }
        }));

    }
}
