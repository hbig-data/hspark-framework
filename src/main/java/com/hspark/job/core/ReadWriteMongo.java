package com.hspark.job.core;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.bson.BSONObject;
import scala.Tuple2;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/24 16:41.
 */
public class ReadWriteMongo {

    public static void main(String[] args) {

        Configuration hadoopconf = new Configuration();
        MongoConfig config = new MongoConfig(hadoopconf);

        config.setInputURI("mongodb://127.0.0.1:27017/liu.spark");
        config.setQuery("{apMac:{$in:[\"00:21:26:00:10:A3\",\"00:21:26:00:14:C3\"]}}");
        config.setOutputURI("mongodb://127.0.0.1:27017/liu.spark");

        config.setOutputKey(Text.class);
        config.setOutputValue(BSONWritable.class);

        config.setInputFormat(MongoInputFormat.class);
        config.setOutputFormat(MongoOutputFormat.class);

        JavaSparkContext jsc = new JavaSparkContext("local[2]", "test");
        JavaPairRDD<Object, BSONObject> rdd = jsc.newAPIHadoopRDD(hadoopconf, MongoInputFormat.class, Object.class, BSONObject.class);


        JavaRDD<String> javaRDD = jsc.textFile("hdfs:///data/all/part-11111");

        SQLContext sqlContext = new SQLContext(jsc);
        Dataset<Row> dataset = sqlContext.jsonRDD(javaRDD);
        dataset.registerTempTable("tt");
        Dataset<Row> dataset1 = sqlContext.sql("select sum(badreview),sum(complaints) from tt group by badreview, complaints");



        JavaPairRDD<Object, BSONObject> mapToPair = dataset1.toJavaRDD().mapToPair(new PairFunction<Row, Object, BSONObject>() {
            public Tuple2<Object, BSONObject> call(Row row) throws Exception {
                BSONObject bson = new BasicDBObject();

                bson.put("first", row.get(0) + "");
                bson.put("second", row.get(1) + "");

                return new Tuple2<Object, BSONObject>(null, bson);
            }
        });

        mapToPair.saveAsNewAPIHadoopFile("hdfs:///sparktestdata", Object.class, Object.class, MongoOutputFormat.class, hadoopconf);


    }
}
