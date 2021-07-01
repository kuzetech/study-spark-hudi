package com.kuze.bigdata.study.shizhan

import com.xmfunny.hudi.delta.MergeBuilder
import com.xmfunny.hudi.domain.TableMeta
import com.xmfunny.hudi.utils.SparkQuicklyStartUtil
import org.apache.spark.sql.SparkSession

object AggregationDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val sourceTableMeta = new TableMeta(
      "order",
      "file:///tmp/order",
      "file:///tmp/order/*/*/*/*",
      "ts",
      "uuid",
      "partitionpath")

    prepareData(spark, sourceTableMeta)

    val targetTableMeta = new TableMeta(
      "fare",
      "file:///tmp/fare",
      "file:///tmp/fare/*",
      "ts",
      "driver",
      TableMeta.NO_PARTITION_FIELD)

    MergeBuilder
      .forPath(spark,sourceTableMeta,"source")
      .aggSQL(
        "select driver, sum(fare) as fare, max(ts) as ts from source group by driver",
        "driver",
        MergeBuilder.SEARCH_NO_PARTITION_FIELD
      )
      .merge(targetTableMeta,"target","source.driver = target.driver")
      .whenNotMatched(Map(
        "driver" -> "source.driver",
        "fare" -> "source.fare",
        "ts" -> "source.ts"
      ))
      .whenMatched(Map(
        "fare" -> "(source.fare + target.fare)",
        "ts" -> "source.ts"
      ))
      .execute(3)

    println(s"最终从数据表中查询所有数据如下：")
    HudiReadUtil.readAll(spark,targetTableMeta)

    spark.stop()
  }

  private def prepareData(spark: SparkSession, sourceTableMeta: TableMeta) = {
    val inserts = convertToStringList(new DataGenerator().generateInserts(5))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    HudiWriteUtil.overwriteData(df,sourceTableMeta)
  }
}
