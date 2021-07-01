package com.kuze.bigdata.study.shizhan

import com.xmfunny.hudi.domain.TableMeta
import com.xmfunny.hudi.utils.{IpUtil, SparkQuicklyStartUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

object IpToAddressDemo {

  //读取项目中的配置配置文件
  val IP_ADDRESS_RELATION_FILE_PATH = "ip.txt"

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val sourceTableMeta = new TableMeta(
      "ip",
      "file:///tmp/ip",
      "file:///tmp/ip/*/*/*/*",
      "ts",
      "uuid",
      "partitionpath")

    prepareData(spark, sourceTableMeta)

    val targetTableMeta = new TableMeta(
      "address",
      "file:///tmp/address",
      "file:///tmp/address/*/*/*/*",
      "ts",
      "driver",
      "partitionpath")

    val lastCommitTiming = new HoodieTableMetaService(spark, sourceTableMeta).getLastCommitTiming()

    val consumerMetaMap = new HoodieTableMetaService(spark,targetTableMeta).getLastConsumerTimingMap()

    val lastConsumerTiming = HoodieTableMetaService.getLastConsumerTimingFromMap(consumerMetaMap, sourceTableMeta.tableName)

    // 从源 hudi 表中读取数据
    val incViewDF = HudiReadUtil.incrementalRead(spark,sourceTableMeta,lastConsumerTiming,lastCommitTiming)

    // 读取IP和地址匹配规则，并广播
    val sc = spark.sparkContext
    val ipAddressRelations = IpUtil.readIPAddressRules(sc, IP_ADDRESS_RELATION_FILE_PATH)
    val ipAddressRelationsRef = sc.broadcast(ipAddressRelations)

    // 注册转换 udf
    import org.apache.spark.sql.functions.udf
    val ipToAddressFuction = (ip: String) => {
      val ipAddressRelation = IpUtil.getAddress(ip, ipAddressRelationsRef.value)
      ipAddressRelation.country
    }
    val ipToAddressUDF: UserDefinedFunction = udf(ipToAddressFuction)

    // 增加一列
    val withAddressDF = incViewDF.withColumn("address", ipToAddressUDF(incViewDF("rider")))

    val consumerMeta = HoodieTableMetaService.encodeConsumerString(consumerMetaMap, Map(sourceTableMeta.tableName -> lastCommitTiming))

    HudiWriteUtil.overwriteDataWithConsumerMeta(withAddressDF,targetTableMeta,consumerMeta)

    HudiReadUtil.readAll(spark,targetTableMeta)

    spark.stop()
  }

  private def prepareData(spark: SparkSession, sourceTableMeta: TableMeta) = {
    val inserts = convertToStringList(new DataGenerator().generateInserts(5))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    HudiWriteUtil.overwriteData(df,sourceTableMeta)
  }
}
