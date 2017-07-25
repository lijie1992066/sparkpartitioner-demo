package cn.lijie.cn.lijie

import java.net.URI

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/25 
  * Time: 10:39  
  */
class xy {

}

object xy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("urlLocal").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("src/main/file/*.log1").map(x => {
      val s = x.split("\t")
      (s(1), 1.toLong)
    })

    val rdd2 = rdd1.combineByKey(x => x, (a: Long, b: Long) => a + b, (c: Long, d: Long) => c + d)

    val rdd3 = rdd2.map(x => {
      val host = new URI(x._1).getHost
      (host, (x._1, x._2))
    })

    val rdd4 = rdd3.map(_._1).distinct

    //    println(rdd4.collect().toBuffer)

    //    val rdd5 = rdd3.partitionBy(new HostPartitioner(rdd4.collect())).sortBy(_._2._2, false).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\out01")
    val rdd5 = rdd3.partitionBy(new HostPartitioner(rdd4.collect())).mapPartitions(x=>{x.take(3)}).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\out01")

    sc.stop()
  }
}

class HostPartitioner(val arr: Array[String]) extends Partitioner {

  var map = new mutable.HashMap[String, Int]

  init

  def init: Unit = {
    var count = 0
    for (x <- arr) {
      map += (x -> count)
      count += 1
    }
  }

  override def numPartitions: Int = arr.length

  override def getPartition(key: Any): Int = map.getOrElse(key.toString, 0)
}