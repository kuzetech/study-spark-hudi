package com.kuze.bigdata.study.shizhan.utils

import com.kuze.bigdata.study.shizhan.constants.ApplicationConstants
import org.apache.spark.sql.SparkSession

object SparkQuicklyStartUtil {

  def getDefaultSparkSession(): SparkSession ={
    val spark = SparkSession
      .builder
      .appName(ApplicationConstants.APPLICATION_NAME)
      .master(ApplicationConstants.APPLICATION_MASTER)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}
