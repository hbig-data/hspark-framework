package com.hspark.job.core;

import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author Rayn on 2016/7/2.
 * @email liuwei412552703@163.com.
 */
public class HelloSparkJob implements Callable<String>, Serializable {

    public static final Logger log = LoggerFactory.getLogger(HelloSparkJob.class);

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public String call() throws Exception {
        SparkConf conf = new SparkConf().setAppName("HelloSpark").setMaster("local[1]").setSparkHome("E:/openSource/spark-1.4.1-bin-hadoop2.4");

        List<String> data = new ArrayList<String>();
        data.add("test-1");
        data.add("frist-2");
        data.add("seconds-3");


        JavaSparkContext context = new JavaSparkContext(conf);

        final Accumulator<Integer> accumulator = context.accumulator(0);

        JavaRDD<Integer> map = context.parallelize(data).map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                String[] split = s.split("-");

                accumulator.add(1);

                return Integer.valueOf(split[1]);
            }
        });

        log.info("当前 Count : {}", accumulator.localValue());


        JavaRDD<Integer> persist = map.persist(StorageLevel.DISK_ONLY());


        persist.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                log.info("当前值:{}", integer);
            }
        });

        log.info("缓存结束。");

        map.saveAsObjectFile("file:///E:/test/spark/result1");

        context.close();

        return "";
    }

    public static void main(String[] args) throws Exception {
        HelloSparkJob job = new HelloSparkJob();
        job.call();
    }
}
