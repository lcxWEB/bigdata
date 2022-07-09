package com.coder.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: lichunxia 
  * @create: 7/9/22 12:59 PM
  */
object Spark03_WordCount {

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
    // Spark将分组与聚合使用一个功能实现
    // reduceByKey:相同的Key的数据，可以对value进行reduce聚合
    // scala至简原则
    val worldCount: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
//    val worldCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => {x + y})
//    val worldCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => x + y)
    val array: Array[(String, Int)] = worldCount.collect()
    array.foreach(println)
    // 3、关闭连接
    sc.stop()
  }

}
