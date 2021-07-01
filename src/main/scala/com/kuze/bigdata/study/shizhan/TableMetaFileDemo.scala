package com.kuze.bigdata.study.shizhan

import com.xmfunny.hudi.domain.TableMeta
import com.xmfunny.hudi.utils.SparkQuicklyStartUtil

object TableMetaFileDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val meta = TableMeta(spark,"/tmp/user")

    println(meta.tableName)
  }

}
