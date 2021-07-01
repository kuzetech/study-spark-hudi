package com.kuze.bigdata.study.shizhan.performance

import com.xmfunny.hudi.UberOrderGenerator.DataGenerator
import com.xmfunny.hudi.UberOrderGenerator.convertToStringList
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import com.kuze.bigdata.study.shizhan.constants.ApplicationConstants
import com.kuze.bigdata.study.shizhan.domain.TableMeta
import com.kuze.bigdata.study.shizhan.service.HoodieReadClientService
import com.kuze.bigdata.study.shizhan.utils.HudiWriteUtil
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.common.model.HoodieKey
import org.apache.hudi.config.{HoodieStorageConfig, HoodieWriteConfig}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable

object Search100KeyDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName(ApplicationConstants.APPLICATION_NAME)
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val sourceTableMeta = new TableMeta(
      "read_client_performance",
      "file:///tmp/read_client_performance",
      "file:///tmp/read_client_performance/*/*",
      "ts",
      "uuid",
      "partitionpath")

    val totalArray = Array(10000,20000,40000,70000,100000,500000,1000000,2000000)
    val partitionCountArray = Array(1, 2, 4, 7, 10, 50, 100, 200)

    val map = mutable.HashMap[String,Long]()

    for ( total <- totalArray){
      for (count <- partitionCountArray){
        // 生成avro json 数据，参数为生成的数量
        val inserts = convertToStringList(new DataGenerator().generateInserts(total, count))

        val df = spark.read.json(spark.sparkContext.parallelize(inserts, 3))

        val configs = getQuickstartWriteConfigs
        configs.put(HoodieStorageConfig.PARQUET_FILE_MAX_BYTES, String.valueOf(20 * 1024 * 1024))

        HudiWriteUtil.overwriteDataWithConfig(df,sourceTableMeta,configs.toMap)

        val keySetSize = 1
        val baseNum = total / keySetSize

        val array = new Array[HoodieKey](keySetSize)

        for( i <- 0 until keySetSize ){
          val key = i * baseNum
          val partitionIndex = key / (total / count)
          array.update(i,new HoodieKey(key.toString, "part"+partitionIndex))
        }

        val beginTime = System.currentTimeMillis()
        HoodieReadClientService.searchKeys(spark, sourceTableMeta, array.toList, 3)
        val endTime = System.currentTimeMillis()
        map.put(total + "," + count, endTime - beginTime)
      }
    }

    map.foreach(println(_))

    spark.stop()
  }

}
