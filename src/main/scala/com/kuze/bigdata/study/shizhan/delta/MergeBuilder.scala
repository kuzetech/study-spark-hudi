package com.kuze.bigdata.study.shizhan.delta

import com.kuze.bigdata.study.shizhan.domain.TableMeta
import com.kuze.bigdata.study.shizhan.service.{HoodieReadClientService, HoodieTableMetaService}
import com.kuze.bigdata.study.shizhan.utils.{HudiReadUtil, HudiWriteUtil, StringUtil}
import org.apache.hudi.common.model.HoodieKey
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class MergeBuilder (
                     val spark: SparkSession,
                     val sourceTableMeta: TableMeta,
                     val sourceAlias: String) extends Serializable {

  private var targetTableMeta: TableMeta = _
  private var targetAlias: String = _
  private var joinCondition: String = _

  private var aggSQL: String = _

  private var updateOpt: Map[String, String] = _
  private var insertOpt: Map[String, String] = _

  private var keyFieldName: String = _
  private var keyTransformFunc: (String) => String = _

  private var partitionFieldName: String = _
  private var partitionTransformFunc: (String) => String = _

  /**
    * 适用：源数据需要先聚合后再写入
    * 参数说明：聚合后需要重新指定key和分区规则
    * @param sql 聚合语句
    * @param keyFieldName 选取聚合后数据集的某一个字段作为主键字段
    * @param keyFunc  对选取作为key的字段进行预处理，处理结果作为主键值
    * @param partitionFieldName 选取聚合后数据集的某一个字段作为分区字段
    * @param partitionFunc  对选取作为分区的字段进行预处理，处理完的值作为分区值
    * @return
    */
  def aggSQL(sql: String, keyFieldName: String, keyFunc: (String) => String, partitionFieldName: String, partitionFunc: (String) => String): MergeBuilder = {
    this.aggSQL = sql
    this.keyFieldName = keyFieldName
    this.keyTransformFunc = keyFunc
    this.partitionFieldName = partitionFieldName
    this.partitionTransformFunc = partitionFunc
    this
  }

  def aggSQL(sql: String, keyFieldName: String, partitionFieldName: String): MergeBuilder = {
    aggSQL(sql, keyFieldName, null, partitionFieldName, null)
    this
  }

  /**
    * 适用：从要写入的数据中指定主键规则
    * @param keyFieldName
    * @param func
    * @return
    */
  def keyBy(keyFieldName: String, func: (String) => String): MergeBuilder = {
    this.keyFieldName = keyFieldName
    this.keyTransformFunc = func
    this
  }

  def keyBy(keyFieldName: String): MergeBuilder = {
    keyBy(keyFieldName, null)
    this
  }

  /**
    * 适用：从要写入的数据中指定分区规则
    * @param partitionFieldName
    * @param func
    * @return
    */
  def partitionBy(partitionFieldName: String, func: (String) => String): MergeBuilder = {
    this.partitionFieldName = partitionFieldName
    this.partitionTransformFunc = func
    this
  }

  def partitionBy(partitionFieldName: String): MergeBuilder = {
    partitionBy(partitionFieldName, null)
    this
  }

  /**
    * 引入存量表，并指定增量数据和存量数据的关联方式
    * @param targetTableMeta 存量表元数据
    * @param targetAlias 为增量数据关联的存量数据创建的 TempView 名字
    * @param joinCondition 指定 增量TempView 和 存量TempView 关联关系
    * @return
    */
  def merge(targetTableMeta: TableMeta, targetAlias: String, joinCondition: String): MergeBuilder = {
    this.targetTableMeta = targetTableMeta
    this.targetAlias = targetAlias
    this.joinCondition = joinCondition
    this
  }

  /**
    * 当增量数据已经存在存量表中时，进行更新
    * 需要指定指定更新规则
    * @param set key表示写入后的字段名，value表示写入的值
    * @return
    */
  def whenMatched(set: Map[String, String]): MergeBuilder = {
    this.updateOpt = set
    this
  }

  /**
    * 当增量数据不存在存量表中时，需要插入数据
    * 如果不指定任何写入规则，默认为 select *
    * @param set key表示写入后的字段名，value表示写入的值
    * @return
    */
  def whenNotMatched(set: Map[String, String]): MergeBuilder = {
    this.insertOpt = set
    this
  }

  /**
    * 构建完相关参数，需要调用该方法真正执行计划
    * @param parallelism HoodieReadClient查询任务并行度
    */
  def execute(parallelism: Int) = {

    checkParameters()

    val consumerMetaMap = new HoodieTableMetaService(spark,targetTableMeta).getLastConsumerTimingMap()

    val lastConsumerTime = HoodieTableMetaService.getLastConsumerTimingFromMap(consumerMetaMap, sourceTableMeta.tableName)

    val sourceLastCommitTime = new HoodieTableMetaService(spark,sourceTableMeta).getLastCommitTiming()

    val sourceDF = HudiReadUtil.incrementalRead(spark,sourceTableMeta,lastConsumerTime,sourceLastCommitTime)

    sourceDF.createOrReplaceTempView(sourceAlias)

    var processDF = sourceDF

    if(StringUtil.noEmpty(aggSQL)){
      val aggDF = spark.sql(aggSQL)
      processDF = aggDF
      println(s"数据经过了预聚合，结果如下")
      aggDF.createOrReplaceTempView(sourceAlias)
    }else{
      println(s"数据未经过预聚合，结果如下")
    }

    processDF.show(false)

    val distinctHoodieKeyRDD = processDF.rdd.map(item => {
      val keyFieldValue = getSearchKeyValue(item)
      val partitionFieldValue = getSearchPartitionValue(item)
      (keyFieldValue,partitionFieldValue)
    }).distinct().map(tuple => {new HoodieKey(tuple._1,tuple._2)})

    val searchResult: Dataset[Row] = HoodieReadClientService.searchKeys(spark, targetTableMeta, distinctHoodieKeyRDD,parallelism)

    //生成记录消费进度字符串
    val consumerMeta = HoodieTableMetaService.encodeConsumerString(consumerMetaMap,Map(sourceTableMeta.tableName -> sourceLastCommitTime))

    if(searchResult == null){
      println(s"没有查询到数据，所以'全部新增'")

      val allInsertSQl = generateAllInsertSQL(processDF.columns)
      val insertDF = spark.sql(allInsertSQl)
      println(s"一共新增${insertDF.count()}条数据，插入数据如下：")
      insertDF.show(false)
      HudiWriteUtil.writeDataWithConsumerMeta(insertDF,targetTableMeta,consumerMeta)

    }else if(searchResult.count() == processDF.count()){
      println(s"查询到的数据条目和输入条目数量一致，所以'全部更新',查询到的数据如下：")
      searchResult.show(false)

      searchResult.createOrReplaceTempView(targetAlias)
      val updateSQl = generateUpdateSQL(searchResult)
      val updateDF = spark.sql(updateSQl)
      println(s"一共更新${updateDF.count()}条数据，插入数据如下：")
      updateDF.show(false)
      HudiWriteUtil.writeDataWithConsumerMeta(updateDF,targetTableMeta,consumerMeta)

    }else{
      println(s"输入数据同时存在更新和新增,查询到的数据如下：")
      searchResult.show(false)

      searchResult.createOrReplaceTempView(targetAlias)
      val insertSQl = generateInsertSQL(processDF.columns)
      val insertDF = spark.sql(insertSQl)
      println(s"一共新增${insertDF.count()}条数据，插入数据如下：")
      insertDF.show(false)
      HudiWriteUtil.writeData(insertDF,targetTableMeta)

      val updateSQl: String = generateUpdateSQL(searchResult)
      println(s"生成的更新语句为：${updateSQl}")
      val updateDF = spark.sql(updateSQl)
      println(s"一共更新${updateDF.count()}条数据，插入数据如下：")
      updateDF.show(false)
      HudiWriteUtil.writeDataWithConsumerMeta(updateDF,targetTableMeta,consumerMeta)
    }
  }

  /**
    * 新数据需要通过 HoodieKey 查找对应的 存量数据，需要确认 key 和 partition
    * 当前部分仅确认 partition ，该 partition 需要指定源数据的某一个字段
    * 通过传入 partitionFieldName 函数可以对该字段做一定的处理
    *
    * @param item
    * @return
    */
  private def getSearchPartitionValue(item: Row): String = {
    val partitionFieldName = if (StringUtil.isEmpty(this.partitionFieldName)) sourceTableMeta.partitionFieldName else this.partitionFieldName
    if(partitionFieldName.equals(MergeBuilder.SEARCH_NO_PARTITION_FIELD)){
      return MergeBuilder.SEARCH_NO_PARTITION_FIELD
    }
    item.getAs[String](partitionFieldName)
  }

  /**
    * 新数据需要通过 HoodieKey 查找对应的 存量数据，需要确认 key 和 partition
    * 当前部分仅确认 key ，该 key 需要指定源数据的某一个字段
    * 通过传入 keyTransformFunc 函数可以对该字段做一定的处理
    *
    * @param item
    * @return
    */
  private def getSearchKeyValue(item: Row) = {
    val keyFieldName = if (StringUtil.isEmpty(this.keyFieldName)) sourceTableMeta.keyFieldName else this.keyFieldName
    var keyFieldValue = item.getAs[String](keyFieldName)
    if (keyTransformFunc != null) {
      keyFieldValue = keyTransformFunc(keyFieldValue)
    }
    keyFieldValue
  }

  private def generateInsertSQL(columns: Array[String]) = {
    val insertSelectStr = generateInsertSelectStr(columns)
    val insertSQl = insertSelectStr + s" from ${sourceAlias} left join ${targetAlias} on ${joinCondition} where ${targetAlias}.${targetTableMeta.keyFieldName} is null"
    println(s"生成的插入语句为：${insertSQl}")
    insertSQl
  }

  private def generateUpdateSQL(searchResult: Dataset[Row]) = {
    val updateSelectStr = generateUpdateSelectStr(searchResult.columns)
    val updateSQl = updateSelectStr + s" from ${sourceAlias} inner join ${targetAlias} on ${joinCondition}"
    println(s"生成的更新语句为：${updateSQl}")
    updateSQl
  }

  private def generateAllInsertSQL(columns: Array[String]) = {
    val insertSelectStr = generateInsertSelectStr(columns)
    val insertSQl = insertSelectStr + s" from ${sourceAlias}"
    println(s"生成的全数据插入语句为：${insertSQl}")
    insertSQl
  }

  /**
    * 新数据需要 update 时，生成 select 语句
    * 结合 更新规则字段 + 未更新字段
    * @param columns
    * @return
    */
  private def generateUpdateSelectStr(columns: Array[String]) = {
    val noChangeCol = columns.filter(col => {
      updateOpt.get(col).isEmpty && !col.contains("_hoodie_")
    })
    val builder = StringBuilder.newBuilder
    builder.append("select")
    noChangeCol.foreach(col => {
      builder.append(s" ${targetAlias}.${col},")
    })
    updateOpt.foreach(opt => {
      builder.append(s" ${opt._2} as ${opt._1},")
    })
    builder.deleteCharAt(builder.length - 1)
    builder.append(MergeBuilder.BLANK_STRING)
    builder.toString()
  }

  /**
    * 新数据存在 insert 时，生成 select 语句
    * 如果 insert 时 不指定插入规则，默认为 select *
    * @return
    */
  private def generateInsertSelectStr(columns: Array[String]) = {
    val builder = StringBuilder.newBuilder
    builder.append("select")
    if (insertOpt == null || insertOpt.isEmpty) {
      columns.filter(!_.contains("_hoodie_")).foreach(col=>{
        builder.append(s" ${sourceAlias}.${col},")
      })
    } else {
      insertOpt.foreach(opt => {
        builder.append(s" ${opt._2} as ${opt._1},")
      })
    }
    builder.deleteCharAt(builder.length - 1)
    builder.append(MergeBuilder.BLANK_STRING)
    builder.toString()
  }

  /**
    * 检查相关参数是否非空
    */
  private def checkParameters() = {
    if(this.spark == null){
      throw new RuntimeException("请初始化 spark 环境")
    }

    if(StringUtil.isEmpty(this.sourceAlias)){
      throw new RuntimeException("请为源表指定别名")
    }

    if(StringUtil.isEmpty(this.targetAlias)){
      throw new RuntimeException("请为写入表指定别名")
    }

    if(StringUtil.isEmpty(this.joinCondition)){
      throw new RuntimeException("请指定 join 条件")
    }

    if(updateOpt == null || updateOpt.size == 0){
      throw new RuntimeException("请指定 match 时的数据更新规则")
    }

    if(StringUtil.noEmpty(aggSQL)){
      if(StringUtil.isEmpty(keyFieldName)){
        throw new RuntimeException("聚合模式下，需要指定新的 match key field")
      }
      if(partitionFieldName == null){
        throw new RuntimeException("聚合模式下，需要指定新的 match partition field")
      }
    }
  }
}

object MergeBuilder {

  val BLANK_STRING = " "
  // 如果要写入的表没有分区，指定 partitionFieldName 可以使用该参数
  val SEARCH_NO_PARTITION_FIELD = ""

  /**
    * 引入源表
    * @param spark
    * @param sourceTableMeta 增量表元数据
    * @param sourceAlias 为增量数据创建的 TempView 名字
    * @return
    */
  def forPath(spark: SparkSession, sourceTableMeta: TableMeta, sourceAlias: String): MergeBuilder = {
    new MergeBuilder(spark, sourceTableMeta, sourceAlias)
  }

}
