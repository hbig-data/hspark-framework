package com.hspark.job.core

/**
  * SparkSession实质上是SQLContext和HiveContext的组合（未来可能还会加上StreamingContext），
  * 所以在SQLContext和HiveContext上可用的API在SparkSession上同样是可以使用的。
  * SparkSession内部封装了sparkContext，所以计算实际上是由sparkContext完成的。
  *
  * @author Rayn on 2016/7/7.
  * @email liuwei412552703@163.com.
  */
object DefaultSparkSession {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local[1]").appName("spark session example").getOrCreate()

    sparkSession.enableHiveSupport()



  }
}
