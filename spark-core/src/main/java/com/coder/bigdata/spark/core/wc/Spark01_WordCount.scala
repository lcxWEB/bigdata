package com.coder.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: lichunxia 
  * @create: 7/9/22 12:59 PM
  */
object Spark01_WordCount {

  def main(args: Array[String]): Unit = {
    // Application
    // Spark框架
    // 1、建立与Spark框架的连接
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // 2、执行业务操作
    // 2.1 读取文件，获取一行一行的数据
    val lines: RDD[String] = sc.textFile("data")
    // 2.2 将一行一行拆分成一个一个单词（分词）
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 2.3 将单词进行分组，进行统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    // 2.4 将分组后的数据进行转换
    val worldCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 2.5 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = worldCount.collect()
    array.foreach(println)
    // 3、关闭连接
    sc.stop()
  }

}
