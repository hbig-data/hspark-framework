package com.hspark.job.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/25 16:46.
 */
public class SparkSchemaToRead {

    private static final Logger LOG = LoggerFactory.getLogger(SparkSchemaToRead.class);

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().master("local[1]").appName("SparkSession").config("spark.sql.warehouse.dir", "file:///f:/").getOrCreate();



        //定义schema
        StructType struct = new StructType();
        struct.add("partner_code", DataTypes.StringType, true);
        struct.add("items", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true);


        SQLContext sqlContext = new SQLContext(sparkSession);

        Dataset<Row> dataset = sparkSession.read().schema(struct).json("file:///f:/test");
        dataset.select("partner_code").show();

        long count = dataset.count();
        LOG.info("合计中数据 : {}", count);


    }
}
