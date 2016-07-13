package com.hspark.job.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/13 17:44.
 */
public class SparkTextSearch implements Serializable {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                "local",
                "textTest",
                "",
                new String[]{}
        );

        SQLContext sqlContext = new SQLContext(sc);


        // Creates a DataFrame having a single column named "line"
        JavaRDD<String> textFile = sc.textFile("hdfs://...");
        JavaRDD<Row> rowRDD = textFile.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                return RowFactory.create(line);
            }
        });


        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("line", DataTypes.StringType, true));


        StructType schema = DataTypes.createStructType(fields);

        DataFrame df = sqlContext.createDataFrame(rowRDD, schema);
        DataFrame errors = df.filter(new Column("line").like("%ERROR%"));

        // Counts all the errors
        errors.count();

        // Counts errors mentioning MySQL
        errors.filter(new Column("line").like("%MySQL%")).count();

        // Fetches the MySQL errors as an array of strings
        errors.filter(new Column("line").like("%MySQL%")).collect();

    }
}
