package com.hspark.job.sql;

import com.hspark.job.vo.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;


/**
 * @author Rayn on 2016/7/2.
 * @email liuwei412552703@163.com.
 */
public class TextSQLLocalTest implements Serializable {
	private static final long serialVersionUID = 1L;



	public final static Log log = LogFactory.getLog(TextSQLLocalTest.class);



    public void start() throws IOException {
        //初始化sparkContext，这里必须在jars参数里面放上Hbase的jar，
        // 否则会报unread block data异常
        JavaSparkContext sc = new JavaSparkContext(
        		"local",
        		"textTest",
                "",
                new String[]{  }
        );



        //使用HBaseConfiguration.create()生成Configuration
        // 必须在项目classpath下放上hadoop以及hbase的配置文件。
        Configuration conf = new Configuration();
        //setInputPaths(conf,new Path("/user/hadoop/nie.txt"));

        JavaRDD<String> graphRDD = sc.textFile("/user/hadoop/test.txt", 1);
//        List<String> lines = FileUtils.readLines(new File(("./nie.txt")), "UTF-8");
//
//        JavaRDD<String> graphRDD = sc.parallelize(lines);


        System.out.println("====" + graphRDD.count());
        JavaRDD<Pair> pairJavaRDD = graphRDD.flatMap(new FlatMapFunction<String, Pair>() {
            @Override
            public Iterable<Pair> call(String s) throws Exception {
                List<Pair> strs = new LinkedList<Pair>();
                StringTokenizer tokenizer = new StringTokenizer(s);
                while (tokenizer.hasMoreTokens()) {
                    String word = tokenizer.nextToken();
                    if (org.apache.commons.lang.StringUtils.isNotBlank(word)) {
                        strs.add(new Pair(word, 1));
                    }
                }
                return strs;

            }
        });

        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.createDataFrame(pairJavaRDD, Pair.class);
        dataFrame.registerTempTable("pair");

        // SQL can be run over RDDs that have been registered as tables.
        DataFrame teenagers = sqlContext.sql("SELECT text, count(*) as c FROM pair group by text order by c DESC limit 20");

        Row[] collect = teenagers.collect();
        for (Row row : collect) {
            System.out.println(row.get(0) + "    " + row.get(1));
        }

        System.out.println("===================================");

        //schemaPeople2.registerTempTable("pair2");
        teenagers = sqlContext.sql("SELECT text, count(*) as c " +
                "FROM pair group by text order by c DESC limit 20");
        collect = teenagers.collect();
        for (Row row : collect) {
            System.out.println(row.get(0) + "    " + row.get(1));
        }

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    /**
     * spark如果计算没写在main里面,实现的类必须继承Serializable接口，<br>
     * </>否则会报 Task not serializable: java.io.NotSerializableException 异常
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        new TextSQLLocalTest().start();
    }
}
