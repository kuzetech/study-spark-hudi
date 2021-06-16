package com.kuze.bigdata.study

import com.kuze.bigdata.study.utils.SparkQuicklyStartUtil
import com.xmfunny.turbine.L1WriteDemo.{BASE_PATH, META_KEY}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.{DataGenerator, convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConversions._

object L4DeleteDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val roViewDF = spark.
      read.
      format("hudi").
      load(L1WriteDemo.PARTITION_PATH)

    roViewDF.createOrReplaceTempView(L1WriteDemo.TABLE_NAME)

    val ds = spark.sql(s"select uuid, partitionpath, ts from ${L1WriteDemo.TABLE_NAME}").limit(2)

    val deletes = new DataGenerator().generateDeletes(ds.collectAsList())

    val deleteDF = spark.read.json(spark.sparkContext.parallelize(deletes, 2))

    deleteDF.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(HoodieWriteConfig.DELETE_PARALLELISM,"2").
      // 标志该操作是删除
      option(DataSourceWriteOptions.OPERATION_OPT_KEY,"delete").
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(HoodieWriteConfig.TABLE_NAME, L1WriteDemo.TABLE_NAME).
      mode(SaveMode.Append).
      save(BASE_PATH)

    val roAfterDeleteViewDF = spark.
      read.
      format("hudi").
      load(L1WriteDemo.PARTITION_PATH)

    roAfterDeleteViewDF.createOrReplaceTempView("test")

    println(spark.sql(s"select * from test").count())

    spark.stop()

  }

}
