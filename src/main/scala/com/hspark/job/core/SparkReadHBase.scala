package com.hspark.job.core

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Rayn on 2016/7/7.
  * @email liuwei412552703@163.com.
  */
object SparkReadHBase {
  def main(args: Array[String]) {

    val sc: SparkContext = new SparkContext("local[1]", "HBaseTest", "")

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, args(1))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.count()

    System.exit(0)
  }
}
