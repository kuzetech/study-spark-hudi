package com.kuze.bigdata.study.shizhan.utils

import com.kuze.bigdata.study.shizhan.domain.TableMeta
import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

object HudiReadUtil {

  def incrementalRead(spark: SparkSession,tableMeta: TableMeta, beginCommitTiming: String, endCommitTiming: String) = {
    if(beginCommitTiming.endsWith(endCommitTiming)){
      println("没有数据需要读取,程序自动退出")
      System.exit(0)
    }
    val result = spark.read.format("hudi").
      option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginCommitTiming).
      option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, endCommitTiming).
      load(tableMeta.partitionPath)
    println(s"本次从源表消费了${result.count()}条数据")
    result
  }

  def readAll(spark: SparkSession,tableMeta: TableMeta) = {
    spark.read.format("hudi")
        .load(tableMeta.partitionPath)
        .createOrReplaceTempView("tmp_result")

    spark.sql("select * from tmp_result").show(false)
  }

}
