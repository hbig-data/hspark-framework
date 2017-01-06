package com.hspark.job.streaming;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/5 16:49.
 */
public class HelloSparkStreaming {
    public static final Logger log = LoggerFactory.getLogger(HelloSparkStreaming.class);


    private static final Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args) throws InterruptedException {

        JavaStreamingContext jssc = new JavaStreamingContext("local[1]", "JavaNetworkWordCount", new Duration(10000));

        //使用updateStateByKey()函数需要设置checkpoint
        jssc.checkpoint(".");

        //打开本地的端口9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.1.107", 9999);
        //按行输入，以空格分隔
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(SPACE.split(line)).iterator();
            }
        });


        //每个单词形成pair，如（word，1）
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {

                System.out.println("word  :" + word);

                return new Tuple2<>(word, 1);
            }
        });

        //统计并更新每个单词的历史出现次数
        JavaPairDStream<String, Integer> counts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newSum = state.or(0);
                for (Integer i : values) {
                    newSum += i;
                }
                return Optional.of(newSum);

            }
        });


        System.out.println("=================" + counts);
        log.info("{}", counts);
        counts.print();


        jssc.start();


        jssc.awaitTermination();

        jssc.close();
    }
}
