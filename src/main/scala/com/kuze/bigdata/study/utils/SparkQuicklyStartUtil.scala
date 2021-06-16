package com.kuze.bigdata.study.utils

import org.apache.spark.sql.SparkSession

object SparkQuicklyStartUtil {

  def getDefaultSparkSession() ={
    val spark = SparkSession
      .builder
      .appName("test")
      .master("local[3]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

}
