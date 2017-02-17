package com.hspark.job.carbondata;

import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.CarbonContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/2/17 16:23.
 */
public class SparkCarbondata {


    public static void main(String[] args) {

        JavaSparkContext jsc = new JavaSparkContext("local[2]", "CarbondataDemo");

        CarbonContext carbonContext = new CarbonContext(jsc.sc(), "F:/spark-temp/");

        SparkSession spark = SparkSession.builder().master("local")
                .appName("SparkSessionExample")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "F:/spark-temp")
                .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=$metastoredb;create=true")
                .getOrCreate();

        CarbonProperties.getInstance().addProperty("carbon.kettle.home", "$rootPath/processing/carbonplugins").addProperty("carbon.storelocation", "F:/spark-temp");

        // Create table
        Dataset<Row> dataset = spark.sql("CREATE TABLE carbon_table( " +
                "shortField short, " +
                "intField int, " +
                "bigintField long, " +
                "doubleField double, " +
                "stringField string, " +
                "timestampField timestamp, " +
                "decimalField decimal(18,2), " +
                "dateField date, " +
                "charField char(5)" +
                ") USING org.apache.spark.sql.CarbonSource " +
                "OPTIONS('DICTIONARY_INCLUDE'='dateField, charField', " +
                "'dbName'='default', 'tableName'='carbon_table')");


        /**
         * 第二种方式
         */
        StructType customSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("c1", DataTypes.StringType, true),
                DataTypes.createStructField("c2", DataTypes.StringType, true),
                DataTypes.createStructField("number", DataTypes.IntegerType, true)});

        // Reads carbondata to dataframe
        Dataset<Row> dataset1 = spark.read().format("carbondata")
                .schema(customSchema)
                .option("tableName", "carbon_table")
                .load();


    }
}
