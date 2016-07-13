package com.hspark.job.graphx;

import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.impl.EdgePartition;
import org.apache.spark.graphx.impl.EdgePartitionBuilder;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Function2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/13 11:16.
 */
public class SparkGraphxExample {

    public static void main(String[] args) {
        SparkContext sc = new SparkContext("local[1]", "spark_grpahx", "");

        Graph<Object, Object> objectGraph = GraphLoader.edgeListFile(sc, "", false, 10, StorageLevel.MEMORY_AND_DISK(), StorageLevel.MEMORY_ONLY());

        EdgePartitionBuilder<String, String> partitionBuilder = new EdgePartitionBuilder<>(10, new ClassTag<String>(), new ClassTag<String>());






    }
}
