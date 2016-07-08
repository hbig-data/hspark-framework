package com.hspark.job.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Spark 读取HBase中的数据
 *
 * @author Rayn on 2016/7/7.
 * @email liuwei412552703@163.com.
 */
public class SparkReadHBase implements Serializable {

    private static final String COLUMN_FAMLILY = "info";
    private static final String COLUMN_NAME = "time";


    public static void main(String[] args) {
        SparkConf config = new SparkConf().setAppName("ReadHBase").setMaster("local[1]").set("spark.executor.memory", "500m");
        JavaSparkContext jsc = new JavaSparkContext(config);

        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(COLUMN_FAMLILY));
        scan.addColumn(Bytes.toBytes(COLUMN_FAMLILY), Bytes.toBytes(COLUMN_NAME));

        try {
            String tableName = "scantest";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);

            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);

            JavaPairRDD<ImmutableBytesWritable, Result> myRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            myRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, Tuple2>() {
                @Override
                public Tuple2 call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {

                    byte[] o = v1._2().getValue(Bytes.toBytes(COLUMN_FAMLILY), Bytes.toBytes(COLUMN_NAME));
                    if (o != null) {
                        return new Tuple2<Integer, Integer>(Bytes.toInt(o), 1);
                    }
                    return null;
                }
            });

            System.out.println(myRDD.count());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
