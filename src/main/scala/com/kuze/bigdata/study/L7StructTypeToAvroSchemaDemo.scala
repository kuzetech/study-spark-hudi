package com.kuze.bigdata.study

import com.kuze.bigdata.study.utils.SparkQuicklyStartUtil
import org.apache.hudi.AvroConversionUtils

object L7StructTypeToAvroSchemaDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val df = spark.
      read.
      format("org.apache.hudi").
      load(L1WriteDemo.PARTITION_PATH)

    val schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema,"a1","a2")

    println(schema.toString(true))

  }

}
