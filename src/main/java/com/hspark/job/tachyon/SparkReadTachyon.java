package com.hspark.job.tachyon;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/8/19 16:23.
 */
public class SparkReadTachyon implements Serializable{


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("Tachyon");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> javaRDD = sc.textFile("alluxio://192.168.1.116:19998/LICENSE");

        JavaRDD<String> map = javaRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1 + "--------" + v1;
            }
        });

        map.saveAsTextFile("alluxio://192.168.1.116:19998/result");


    }
}
