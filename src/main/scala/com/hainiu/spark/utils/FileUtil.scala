package com.hainiu.spark.utils

/**
  * 读取配置文件
  */
import java.io.{BufferedReader, InputStream, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object FileUtil {
  private val conf: Configuration = new Configuration()
//  private val fs: FileSystem = FileSystem.get(conf)

  // 读进来
  def getInputStream(path: Path): InputStream = {
    val fileSystem = FileSystem.get(conf)
    fileSystem.open(path)
  }

  // 拿到里面的记录
  def getBufferedReader(path: Path): BufferedReader = {
    val inputStream = getInputStream(path)
    new BufferedReader(new InputStreamReader(inputStream))
  }
}
