package com.hspark.job.video;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/9/13 9:27.
 */
public class SparkReadVideo implements Serializable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("VideoInput").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hc = new org.apache.hadoop.conf.Configuration();
        JavaPairRDD<Text, HBMat> video = sc.newAPIHadoopFile("data/bike.avi", VideoInputFormat.class, Text.class, HBMat.class, hc);

        video.foreach(new VoidFunction<Tuple2<Text, HBMat>>() {
            @Override
            public void call(Tuple2<Text, HBMat> tuple) throws Exception {
                HBMat image = (HBMat) tuple._2;
                System.out.print(image.getBmat().dump());
            }
        });

        System.out.print(video.count());


        sc.close();
    }
}
