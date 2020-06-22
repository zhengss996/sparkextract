package com.hainiu.spark.broadcast

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.HashMap

class MapAccumulator extends AccumulatorV2[(Any, Any), HashMap[Any, Any]]{

  private var map = new HashMap[Any, Any]()

  override def isZero: Boolean = map.isEmpty

  // 重置，给另外一个空的
  override def reset(): Unit = {
    map = new HashMap[Any, Any]()
  }

  // 拷贝一个副本，用于累加   task -> driver
  override def copy(): AccumulatorV2[(Any, Any), HashMap[Any, Any]] = {
    val accumulator = new MapAccumulator()
    accumulator.synchronized(map.foreach(accumulator.map += _))
    accumulator
  }

  // 取值累加
  override def add(v: (Any, Any)): Unit = {
    var value: Long = map.getOrElse(v._1,0L).asInstanceOf[Long]
    value += v._2.asInstanceOf[Long]
    map += v._1 -> value
  }

  // driver端进行合并
  override def merge(other: AccumulatorV2[(Any, Any), HashMap[Any, Any]]): Unit = {
    other match {
      case o: MapAccumulator => o.map.foreach(f => {
        var value: Long = map.getOrElse(f._1,0L).asInstanceOf[Long]
        value += f._2.asInstanceOf[Long]
        map += f._1 -> value
      })
    }
  }

  // 最后取值的返回值
  override def value: HashMap[Any, Any] = map
}
