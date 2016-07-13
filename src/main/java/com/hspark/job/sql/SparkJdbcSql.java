package com.hspark.job.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/13 17:50.
 */
public class SparkJdbcSql {

    private static final String username = "root";
    private static final String password = "123456";

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                "local",
                "textTest",
                "",
                new String[]{}
        );

        SQLContext sqlContext = new SQLContext(sc);


        // Creates a DataFrame based on a table named "people"
        // stored in a MySQL database.
        String url = "jdbc:mysql://127.0.0.1:3306/test";
        DataFrame df = sqlContext.read().format("jdbc")
                .option("url", url)
                .option("user", username)
                .option("password", password)
                .option("dbtable", "people").load();

        // Looks the schema of this DataFrame.
        df.printSchema();

        // Counts people by age
        DataFrame countsByAge = df.groupBy("age").count();
        countsByAge.show();

        // Saves countsByAge to S3 in the JSON format.
        countsByAge.write().format("json").save("s3a://...");






    }
}
