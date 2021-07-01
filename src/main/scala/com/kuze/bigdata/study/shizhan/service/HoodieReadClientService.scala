package com.kuze.bigdata.study.shizhan.service

import com.kuze.bigdata.study.shizhan.domain.TableMeta
import org.apache.hudi.client.HoodieReadClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{HoodieKey, OverwriteWithLatestAvroPayload}
import org.apache.hudi.exception.TableNotFoundException
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object HoodieReadClientService {

  def searchKeys(session: SparkSession, targetTableMeta: TableMeta, data: List[HoodieKey], parallelism: Int): Dataset[Row] = {
    val rdd = session.sparkContext.parallelize(data)
    searchKeys(session,targetTableMeta,rdd,parallelism)
  }

  def searchKeys(session: SparkSession, targetTableMeta: TableMeta, rdd: RDD[HoodieKey], parallelism: Int): Dataset[Row] = {
    val jsc = new JavaSparkContext(session.sparkContext)
    val sparkEngineContext = new HoodieSparkEngineContext(jsc)
    var client: HoodieReadClient[OverwriteWithLatestAvroPayload] = null
    try {
      client = new HoodieReadClient[OverwriteWithLatestAvroPayload](sparkEngineContext, targetTableMeta.bathPath, session.sqlContext)
    } catch {
      case e: TableNotFoundException => return null
    }
    val javaRDD = JavaRDD.fromRDD(rdd)
    val searchResult = client.readROView(javaRDD, parallelism)
    val resultCount = searchResult.count()
    val searchCount = if (searchResult == null || resultCount == 0) 0 else resultCount
    println(s"本次搜索结果总共${searchCount}条")
    searchResult
  }



}
