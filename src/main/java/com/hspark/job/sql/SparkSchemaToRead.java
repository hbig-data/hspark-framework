package com.hspark.job.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
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

        SparkSession sparkSession = SparkSession.builder().master("local[1]").appName("SparkSession").config("spark.sql.warehouse.dir", "file:///").getOrCreate();


        //定义schema
        StructType struct = new StructType();
        struct.add("partner_code", DataTypes.StringType, true);
        struct.add("app_name", DataTypes.StringType, true);
        struct.add("person_info", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType,true));
        struct.add("items", DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true), true);


        Dataset<Row> dataset = sparkSession.read().schema(struct).json("file:///f:/test");
//        Dataset<Row> dataset = sparkSession.read().json("file:///f:/test");
        dataset.printSchema();
        dataset.show();

        dataset.registerTempTable("tt");

        sparkSession.sql("select partner_code from tt").show();

        long count = dataset.count();
        LOG.info("合计中数据 : {}", count);


    }
}
