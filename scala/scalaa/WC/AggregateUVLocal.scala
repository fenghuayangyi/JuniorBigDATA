package scalaa.WC

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Set

object AggregateUVLocal {
  def main(args: Array[String]): Unit = {
    val wordFile = "file:///D:/test/input/labeled_id.txt";
    val sparkSession = SparkSession.builder().appName("AggregateUVLocal")
      .config("spark.some.config.option", "some-value").getOrCreate()
    val input = sparkSession.read.textFile(wordFile)
    import sparkSession.implicits._
    val lines = input.map(line => {
      val fileds: Array[String] = line.split(",")
      val uid: String = fileds(0)
      val session: String = fileds(1)
      val id: String = fileds(2)
      (uid, session, id)
    }).toDF("uid", "session", "id")
    val tmp = lines.collect();
    val rdd1 = lines.rdd.distinct().map(x => ((x(0),x(2)),x(1)))
    val rdd2 = rdd1.aggregateByKey(Set[String]())((set,valu)=>{set.+(valu.toString) },(set1, set2) => {set1 union set2})
    val rdd3 = rdd2.map(t =>(t._1._1,t._1._2,t._2.size))
    val output = rdd3.saveAsTextFile("file:///D:/test/outputUV")
  }
}
