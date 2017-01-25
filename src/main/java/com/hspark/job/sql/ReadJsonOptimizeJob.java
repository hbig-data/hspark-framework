package com.hspark.job.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2016/12/30 9:26.
 */
public class ReadJsonOptimizeJob implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ReadJsonOptimizeJob.class);


    public static void main(String[] args) {


        JavaSparkContext sparkContext = new JavaSparkContext("local[1]", "ReadJsonOptimizeJob");

        SQLContext sqlContext = new SQLContext(sparkContext);


        String schemaString = "uname as realName,upass as password,age as sexage";
        Dataset<Row> json = sqlContext.read().schema(JsonSchemaBuilder.getJsonSchema(schemaString))
                .load("file:///E:/myWorkSpace/hspark-framework/data/person.parquet");

        Dataset<Row> rowDataset = json.selectExpr(JsonSchemaBuilder.columnSplitPattern().split(schemaString));

        List<Row> rows = rowDataset.collectAsList();
        for (Row row : rows) {
            LOG.info("{}", row.mkString());
        }


    }

}
