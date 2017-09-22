package cn.lijie

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/25 
  * Time: 10:21  
  */
class UrlLocal {

}

object UrlLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("urlLocal").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("src/main/file/*.log").map(x => {
      //手机号  时间   基站    type
      val split = x.split(",")
      val mobile = split(0)
      val theType = split(3)
      val theTime = split(1)
      val time = if (theType == "1") -theTime.toLong else theTime.toLong
      val local = split(2)
      ((mobile, local), time)
    })

    val rdd2 = rdd1.aggregateByKey(0.toLong)(_ + _, _ + _).map {
      x => {
        (x._1._2, (x._1._1, x._2))
      }
    }

    val rdd3 = sc.textFile("src/main/file/*.txt").map(x => {
      val split = x.split(",")
      (split(0), (split(1), split(2)))
    })

    val rdd4 = rdd2.join(rdd3).map(x => {
      //local mobile time jd wd
      (x._1, x._2._1._1, x._2._1._2, x._2._2._1, x._2._2._2)
    })

    val res = rdd4.sortBy(_._3, false).take(5)

    println(res.toBuffer)

  }
}
