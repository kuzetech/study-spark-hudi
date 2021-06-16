package com.kuze.bigdata.study

import com.kuze.bigdata.study.utils.SparkQuicklyStartUtil
import org.apache.hudi.client.HoodieReadClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.HoodieKey
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}

object L4HoodieReadClientDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val jsc = new JavaSparkContext(spark.sparkContext)

    val sparkEngineContext = new HoodieSparkEngineContext(jsc)
    val client = new HoodieReadClient(sparkEngineContext, L1WriteDemo.BASE_PATH, spark.sqlContext)

    // 当key重复还是会查出多条
    // 当key不存在就不会反悔
    val key1 = new HoodieKey("529fd4cd-c921-47ad-9550-94667203838f", null)
    val key2 = new HoodieKey("222", "americas/united_states/san_francisco")
    val paraResource1: RDD[HoodieKey] = spark.sparkContext.parallelize(List(key1))
    val rdd = JavaRDD.fromRDD(paraResource1)
    try {
      val searchResult: Dataset[Row] = client.readROView(rdd, 2)
      searchResult.show(false)
    } catch {
      case ex: AnalysisException => {
        if(ex.getMessage().contains("Unable to infer schema for Parquet")){
          System.err.println("找不到文件")
        }else{
          ex.printStackTrace()
        }
      }
    }
  }

}
