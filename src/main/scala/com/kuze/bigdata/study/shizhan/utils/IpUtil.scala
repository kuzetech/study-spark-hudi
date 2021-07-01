package com.kuze.bigdata.study.shizhan.utils

import com.kuze.bigdata.study.shizhan.domain.IpAddressRelation
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object IpUtil {

  //正则校验ip
  def verifyIP(ipAddress: String) = {
    import java.util.regex.Pattern
    val ip = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}"
    val pattern = Pattern.compile(ip)
    val matcher = pattern.matcher(ipAddress)
    matcher.matches
  }

  def readIPAddressRules(sc: SparkContext, path: String): Array[IpAddressRelation] = {

    val infoLines: RDD[String] = sc.textFile(path)

    val iPSectionInfoRDD: RDD[IpAddressRelation] = infoLines.map(line => {
      val fields = line.split("[|]")
      val iPSectionInfo = new IpAddressRelation
      iPSectionInfo.setStartNum(fields(0).toLong)
      iPSectionInfo.setEndNum(fields(1).toLong)
      iPSectionInfo.setCountry(fields(2))
      iPSectionInfo.setProvince(fields(3))
      iPSectionInfo.setCity(fields(4))
      iPSectionInfo
    })

    val iPSectionInfoSortRdd: RDD[IpAddressRelation] = iPSectionInfoRDD.sortBy(_.getStartNum)

    val iPSectionRules: Array[IpAddressRelation] = iPSectionInfoSortRdd.collect()
    println(s"IP匹配地址文件包含${iPSectionRules.size}行数据")
    iPSectionRules
  }

  def getAddress(ip: String, IpAddressRelations: Array[IpAddressRelation]): IpAddressRelation = {
    val ipNum = ip2Long(ip)
    val index = binarySearch(IpAddressRelations, ipNum)
    if (index != -1) {
      return IpAddressRelations(index)
    }
    new IpAddressRelation
  }

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
  def binarySearch(IpAddressRelations: Array[IpAddressRelation], ip: Long): Int = {
    var low = 0
    var high = IpAddressRelations.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= IpAddressRelations(middle).getStartNum) && (ip <= IpAddressRelations(middle).getEndNum))
        return middle
      if (ip < IpAddressRelations(middle).getStartNum)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

}
