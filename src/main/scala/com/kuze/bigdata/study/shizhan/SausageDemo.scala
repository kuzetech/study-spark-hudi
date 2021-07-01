package com.kuze.bigdata.study.shizhan

import com.xmfunny.hudi.delta.MergeBuilder
import com.xmfunny.hudi.domain.TableMeta
import com.xmfunny.hudi.utils.SparkQuicklyStartUtil
import org.apache.spark.sql.SparkSession

object SausageDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val loginTableMeta = new TableMeta(
      "login",
      "file:///tmp/login",
      "file:///tmp/login/*",
      "ts",
      "pid",
      TableMeta.NO_PARTITION_FIELD)

    val rechageTableMeta = new TableMeta(
      "recharge",
      "file:///tmp/recharge",
      "file:///tmp/recharge/*",
      "ts",
      "pid",
      TableMeta.NO_PARTITION_FIELD)

    val userTableMeta = new TableMeta(
      "user",
      "file:///tmp/user",
      "file:///tmp/user/*",
      "ts",
      "pid",
      TableMeta.NO_PARTITION_FIELD)

    prepareLoginData(spark, loginTableMeta)
    prepareRechargeData(spark, rechageTableMeta)

    MergeBuilder
      .forPath(spark,loginTableMeta,"login")
      .merge(userTableMeta,"user","login.pid = user.pid")
      .whenNotMatched(Map(
        "pid" -> "login.pid",
        "ts" -> "login.ts"
      ))
      .whenMatched(Map(
        "ts" -> "login.ts"
      ))
      .execute(2)

    println(s"最终从数据表中查询所有数据如下：")
    HudiReadUtil.readAll(spark,userTableMeta)

    MergeBuilder
      .forPath(spark,rechageTableMeta,"recharge")
      .merge(userTableMeta,"user","recharge.pid = user.pid")
      .whenNotMatched(Map(
        "pid" -> "recharge.pid",
        "money" -> "recharge.money",
        "ts" -> "recharge.ts"
      ))
      .whenMatched(Map(
        "money" -> "recharge.money"
      ))
      .execute(2)

    println(s"最终从数据表中查询所有数据如下：")
    HudiReadUtil.readAll(spark,userTableMeta)

    spark.stop()
  }

  private def prepareLoginData(spark: SparkSession, sourceTableMeta: TableMeta) = {
    val inserts = convertToStringList(SausageDataGenerator.generateLoginData(), SausageDataGenerator.loginSchema)
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    HudiWriteUtil.overwriteData(df,sourceTableMeta)
  }

  private def prepareRechargeData(spark: SparkSession, sourceTableMeta: TableMeta) = {
    val inserts = convertToStringList(SausageDataGenerator.generateRechargeData(), SausageDataGenerator.rechargeSchema)
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    HudiWriteUtil.overwriteData(df,sourceTableMeta)
  }

}
