package com.hspark.job.graphx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark Graphx 示例
 *
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/11/28 16:20.
 */
public class SparkGraphxExample {

    private static final Logger LOG = LoggerFactory.getLogger(SparkGraphxExample.class);

    public static void main(String[] args) {

        final SparkConf spackConf = new SparkConf().setMaster("local[2]").setAppName("SparkGraphExample");
        final JavaSparkContext sparkContext = new JavaSparkContext(spackConf);


        final Graph<Object, Object> graph = GraphLoader.edgeListFile(sparkContext.sc(), "", true, 10, StorageLevel.DISK_ONLY(), StorageLevel.DISK_ONLY());
        final VertexRDD<Object> vertices = graph.vertices();
        LOG.info("总定点数：{}", vertices.count());

        final EdgeRDD<Object> edges = graph.edges();
        LOG.info("总的边数为：{}", edges.count());


        final RDD<EdgeTriplet<Object, Object>> triplets = graph.triplets();



    }
}
