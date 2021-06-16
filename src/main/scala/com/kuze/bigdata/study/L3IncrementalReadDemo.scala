package com.kuze.bigdata.study

import com.kuze.bigdata.study.utils.SparkQuicklyStartUtil
import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

object L3IncrementalReadDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val input = spark.read
      .format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, "000")
      .load(L1WriteDemo.PARTITION_PATH)

    input.createOrReplaceTempView(L1WriteDemo.TABLE_NAME)

    spark.sql(s"select * from ${L1WriteDemo.TABLE_NAME}").show(false)

    spark.stop()

  }

}
