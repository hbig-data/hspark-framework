package com.hspark.job.core;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * @author Rayn on 2016/7/2.
 * @email liuwei412552703@163.com.
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        final Log log = LogFactory.getLog(WordCount.class);
        if (args.length == 0) {
            args = new String[]{"/user/hadoop/test.txt"};
        }

        JavaSparkContext ctx = new JavaSparkContext("local", "WordCount", "", new String[]{});

        long time = System.currentTimeMillis();
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        System.out.println("lines:" + (System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                List<String> words = new ArrayList<String>();
                if(StringUtils.isNotBlank(s)) {
                    StringTokenizer tokenizer = new StringTokenizer(s);
                    while (tokenizer.hasMoreTokens()) {
                        words.add(StringUtils.trim(tokenizer.nextToken().replaceAll("\\W", "")));
                    }
                    return words.iterator();
                } else {
                    return Collections.EMPTY_LIST.iterator();
                }
            }
        });

        System.out.println("words:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        System.out.println("====one!!!!:" + ones.count());
//        System.out.println("====three!!!!:" + three.count());
        System.out.println("ones:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        System.out.println("counts:" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();
        List<Tuple2<String, Integer>> output = counts.collect();
        System.out.println("counts:" + (System.currentTimeMillis() - time));
        for (Tuple2 tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        System.exit(0);
    }
}
