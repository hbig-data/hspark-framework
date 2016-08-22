package com.hspark.job.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.io.Serializable;


public class SparkStreamingFlume implements Serializable {


    public static void main(String[] args) throws InterruptedException {

        startJob();

    }

    public static void startJob() throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingFlumeTest");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(60));

        JavaReceiverInputDStream lines = FlumeUtils.createStream(jsc,"192.168.102.39", 9999);

/*
        lines.count();

        lines.count().map(new Function<Long, String>() {
            public String call(Long in) {
                return "Received " + in + " flume events.";
            }
        }).print();
*/

        // 注意此处输入的event类型是SparkFlumeEvent类型。


        lines.mapToPair(new PairFunction<SparkFlumeEvent, String, Integer>() {
            public Tuple2<String,Integer> call(SparkFlumeEvent event) throws Exception {
                return new Tuple2<String, Integer>(new String(event.event().getBody().array()), 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).print();

        /*JavaDStream<String> words = lines.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {

            public Iterable<String> call(SparkFlumeEvent event) throws Exception {

                String line = new String(event.event().getBody().array());

                return Arrays.asList(line.split(" "));

            }

        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String word) throws Exception {

                return new Tuple2<String, Integer>(word, 1);

            }

        });

        JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1 + v2;

            }

        });
        wordsCount.print();
        */
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
