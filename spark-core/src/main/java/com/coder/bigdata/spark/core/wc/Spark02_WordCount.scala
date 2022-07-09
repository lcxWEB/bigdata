package com.coder.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: lichunxia 
  * @create: 7/9/22 12:59 PM
  */
object Spark02_WordCount {

  def main(args: Array[String]): Unit = {
    // Application
    // Spark框架
    // 1、建立与Spark框架的连接
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // 2、执行业务操作
    val lines: RDD[String] = sc.textFile("data")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    val worldCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        list.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      }
    }

    val array: Array[(String, Int)] = worldCount.collect()
    array.foreach(println)
    // 3、关闭连接
    sc.stop()
  }

}