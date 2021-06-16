package com.kuze.bigdata.study

import com.kuze.bigdata.study.utils.SparkQuicklyStartUtil
import com.xmfunny.hudi.domain.TableMeta

object L8MergeIntoDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val loginTM = TableMeta(spark,"file:///tmp/login")

    loginTM.readAll()

    spark.stop()


  }

}
