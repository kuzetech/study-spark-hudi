package com.kuze.bigdata.study

import com.kuze.bigdata.study.utils.SparkQuicklyStartUtil
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions._

object L1WriteDemo {

  val TABLE_NAME = "hudi_cow_table"
  val BASE_PATH = "file:///tmp/" + TABLE_NAME
  val PARTITION_PATH = BASE_PATH + "/*/*/*/*"
  val META_KEY = "_test"

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val inserts = convertToStringList(new DataGenerator().generateInserts(100))
    val df: DataFrame = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      //option(KEYGENERATOR_CLASS_OPT_KEY, "org.apache.hudi.keygen.NonpartitionedKeyGenerator").
      option(META_KEY, "test").
      option(HoodieWriteConfig.TABLE_NAME, this.TABLE_NAME).
      mode(SaveMode.Overwrite).
      save(BASE_PATH);

    spark.stop()

  }

}
