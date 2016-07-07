package com.hspark.job.core

/**
  *
  * @author Rayn on 2016/7/7.
  * @email liuwei412552703@163.com.
  */
object SparkSessionReadCsv {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.master("local[1]").appName("spark session example").getOrCreate()
    val df = sparkSession.read.option("header","true").csv("src/main/resources/sales.csv")

    df.show()

  }
}
