package com.hainiu.spark.config

object MyConfig {
  // xpath配置文件存放地址
  // final val XPATH_INFO_DIR: String = "/Users/leohe/Data/input/xpath_cache_file"
  final val XPATH_INFO_DIR: String = "hdfs://ns1/user/hadoop/xpath_cache_file"

  // 更新xpath配置文件时间
  final val UPDATE_XPATH_INTERVAL: Long = 10000L

  // hbase 表名
  final val HBASE_TABLE_NAME: String = "context_extract"

  // mysql 配置（存报表的）
  final val MYSQL_CONFIG: Map[String, String] = Map("url" -> "jdbc:mysql://nn1.hadoop/hainiucrawler", "username" -> "hainiu", "password" -> "12345678")
  // final val MYSQL_CONFIG: Map[String, String] = Map("url" -> "jdbc:mysql://nn1.hadoop/hainiu_crawler", "username" -> "hainiu_work", "password" -> "jhUJErmgaztqonzT")

  // redis 配置（存xpath）
  final val REDIS_CONFIG: Map[String, String] = Map("host" -> "redis.hadoop", "port" -> "6379", "timeout" -> "10000")
  // final val REDIS_CONFIG: Map[String, String] = Map("host" -> "nn1.hadoop", "port" -> "6379", "timeout" -> "10000")

  // kafka 的组名
  final val KAFKA_GROUP:String = "qingniu8"

  // kafka 的 topic
  final val KAFKA_TOPIC:String = "hainiu_qingniu"

  // kafka 的 broker 地址
  final val KAFKA_BROKER:String = "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092"

  // kafka 的 offset 读取策略
  final val KAFKA_OFFSET_POSITION:String = "earliest"

  // ES 的 host
  final val ES_HOST:String = "s1.hadoop"

  // ES 的端口
  final val ES_PORT:String = "9200"

  // ES 的 index 和 type
  final val ES_INDEX_TYPE:String = "hainiu_spark/news"

  //ZK 配置（存 offset ）
  final val ZOOKEEPER:String = "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181"

  // spark的多目录输入和输出
  //  final val URL_FILE_DIR: String = "/Users/leohe/Data/input/hainiu_cralwer/*/*"  （"/Users/leohe/Data/input/hainiu_cralwer/201710/31"）
  //  多目录输出，例子：https://blog.csdn.net/lw_ghy/article/details/51477100

}
