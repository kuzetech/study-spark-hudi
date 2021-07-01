package com.kuze.bigdata.study.shizhan

import com.xmfunny.hudi.delta.MergeBuilder
import com.xmfunny.hudi.domain.TableMeta
import com.xmfunny.hudi.utils.SparkQuicklyStartUtil
import org.apache.spark.sql.SparkSession

object MergeBuildDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    //定义增量表元数据
    val sourceTableMeta = new TableMeta(
      "source",
      "file:///tmp/source",
      "file:///tmp/source/*/*/*/*",
      "ts",
      "uuid",
      "partitionpath")

    //为增量表生成测试数据
    prepareData(spark, sourceTableMeta)

    //定义存量表元数据
    val targetTableMeta = new TableMeta(
      "target",
      "file:///tmp/target",
      "file:///tmp/target/*/*/*/*",
      "ts",
      "driver",
      "partitionpath")

    MergeBuilder

      // 引入增量表，并为增量数据注册名为 source 的 TempView
      .forPath(spark,sourceTableMeta,"source")

      // 如果增量数据需要聚合后再写入，可以使用该方法
      // aggSQL案例 ： "select driver, sum(fare) as fare, max(ts) as ts from source group by driver"
      // 聚合后要写入存量表需要指定新的 key 和 partition 字段，也可以传入 keyFunc 和 partitionFunc 对其字段值进行处理，不做处理传null
      // 特别注意使用了该方法后，聚合后的数据将会替代 source TempView
      //.aggSQL(aggSQL, keyFieldName, keyFunc, partitionFieldName, partitionFunc)

      // 引入存量表，并为存量数据注册名为 target 的 TempView
      // 并指定 增量TempView 和 存量TempView 的关联条件
      // 特别注意：条件中指定字段需要带上 TempView 的名字
      .merge(targetTableMeta,"target","source.driver = target.driver")

      // 当 增量表 和 存量表 的主键不一致时，需要从 增量数据 中指定一个字段关联到 存量表的主键，允许插入函数做一定处理
      .keyBy("driver", null)

      // 当 增量表 和 存量表 的分区字段不一致时，需要从 增量数据 中指定一个字段关联到 存量表的分区，允许插入函数做一定处理
      // .partitionBy("partitionpath", null)

      .whenNotMatched(Map(
        "driver" -> "source.driver",
        "fare" -> "source.fare",
        "partitionpath" -> "source.partitionpath",
        "ts" -> "source.ts"
      ))

      .whenMatched(Map(
        "fare" -> "(source.fare + target.fare)",
        "ts" -> "source.ts"
      ))

      // 需要指定 搜索存量数据时的并行度
      .execute(2)

    println(s"最终从数据表中查询所有数据如下：")
    HudiReadUtil.readAll(spark,targetTableMeta)

    spark.stop()
  }

  private def prepareData(spark: SparkSession, sourceTableMeta: TableMeta) = {
    val inserts = convertToStringList(new DataGenerator().generateInserts(2))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    HudiWriteUtil.overwriteData(df,sourceTableMeta)
  }
}
