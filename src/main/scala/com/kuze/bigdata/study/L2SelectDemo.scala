package com.kuze.bigdata.study

import com.kuze.bigdata.study.utils.SparkQuicklyStartUtil
import org.apache.spark.sql.SparkSession

object L2SelectDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val roViewDF = spark.
      read.
      format("org.apache.hudi").
      load(L1WriteDemo.PARTITION_PATH)
    //load(basePath) 如果使用 "/partitionKey=partitionValue" 文件夹命名格式，Spark将自动识别分区信息

    roViewDF.createOrReplaceTempView("test")
    spark.sql("select a.*,b.* from test a left join test b on a.driver = b.driver").show(false)
    //spark.sql("select fare, begin_lon, begin_lat, ts from hudi_cow_table where fare > 20.0").show(false)
    //spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_cow_table").show()

    spark.stop()
  }
}
