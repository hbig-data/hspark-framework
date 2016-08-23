package com.hspark.job.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * mysql -h 10.0.200.11 -P 3306 -utest -ptest
 */
public class HbaseSparkJob implements Serializable {


    protected transient static String HOST = null;
    protected transient static String USERNAME = null;
    protected transient static String PORT = null;
    protected transient static String PASSWORD = null;
    protected transient static String DB = null;
    protected transient static String HBASE_TO_MYSQL_TABLENAME = null;
    protected transient static String HBASE_TABLENAME = null;
    protected transient static String HBASE_ZOOKEEPER_QUORUM = null;

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("HbaseSparkJob");

        HOST = args[0];
        PORT = args[1];
        DB = args[2];
        USERNAME = args[3];
        PASSWORD = args[4];

        HBASE_ZOOKEEPER_QUORUM = args[5];
        HBASE_TABLENAME = args[6];
        HBASE_TO_MYSQL_TABLENAME = args[7];

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final SQLContext sqlContext = new SQLContext(sc);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));


        conf.set(TableInputFormat.INPUT_TABLE, HBASE_TABLENAME);
        ClientProtos.Scan proto = null;
        try {
            proto = ProtobufUtil.toScan(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String ScanToString = Base64.encodeBytes(proto.toByteArray());
        conf.set(TableInputFormat.SCAN, ScanToString);

        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        System.out.println(" ----------------------------------------- start  ----------------------------------------- ");

        JavaRDD<Row> rowRDD = myRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, String[]>() {
            @Override
            public Iterator<Tuple2<String, String[]>> call(Tuple2<ImmutableBytesWritable, Result> line) throws Exception {
                List<Tuple2<String, String[]>> results = new LinkedList<Tuple2<String, String[]>>();

                Result result = line._2;
                if (result != null && !result.isEmpty()) {
                    String[] data = new String[2];

                    for (Cell kv : result.listCells()) {

                        String key = Bytes.toString(kv.getQualifier());
                        String weight = Bytes.toString(kv.getValue());
                        data[0] = weight;
                        data[1] = "1";

                        results.add(new Tuple2<String, String[]>(key, data));
                    }
                }
                return results.iterator();
            }
        }).reduceByKey(new Function2<String[], String[], String[]>() {

            public String[] call(String[] data0, String[] data1) throws Exception {
                String[] data = new String[2];
                data[0] = String.valueOf(Double.parseDouble(data0[0]) + Double.parseDouble(data1[0]));
                data[1] = String.valueOf(Integer.parseInt(data0[1]) + Integer.parseInt(data1[1]));
                return data;
            }
        }).map(new Function<Tuple2<String, String[]>, Row>() {

            private String splitChar = "#";

            public Row call(Tuple2<String, String[]> line) throws Exception {
                String key = line._1;
                String[] keys = key.split(splitChar, 2);
                String lableId = keys[0];

                String keyId = keys.length == 2 ? keys[1] : "#N/A";

                String[] values = line._2;

                int userCount = Integer.valueOf(values[1]);

                BigDecimal b = new BigDecimal(Double.valueOf(values[0]) / userCount);
                double weight = b.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();

                return RowFactory.create(lableId, keyId, weight, userCount);
            }
        });


        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("labelId", DataTypes.StringType, false),
                DataTypes.createStructField("keywordId", DataTypes.StringType, false),
                DataTypes.createStructField("weight", DataTypes.DoubleType, true),
                DataTypes.createStructField("userCount", DataTypes.IntegerType, true)
        });

        Dataset<Row> personDataFrame = sqlContext.createDataFrame(rowRDD, schema);

        Properties prop = new Properties();
        prop.put("user", USERNAME);
        prop.put("password", PASSWORD);

        personDataFrame.write().mode("append").jdbc("jdbc:mysql://" + HOST + ":" + PORT + "/" + DB, HBASE_TO_MYSQL_TABLENAME, prop);

        System.out.println("------------------------------------   success  -------------------------------------");
        sc.stop();
    }
}