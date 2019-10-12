package scalaa.WC

import org.apache.spark.{SparkConf, SparkContext}
//-Dspark.master=local
object WrodCountLocal {
  def main(args: Array[String]): Unit = {
    val wordFile = "file:///D:/test/input/test.txt";
    val conf = new SparkConf().setAppName("WordCount");
    val sc = new SparkContext(conf)
    val input = sc.textFile(wordFile, 2).cache()
    //    val lines = input.flatMap(line=>line.split(" "))
    val lines = input.flatMap(_.split(" "))
    //val count = lines.map(word => (word,1)).reduceByKey{case (x,y)=>x+y}
    val count = lines.map((_,1)).aggregateByKey(0)(_+_,_+_);
    val output = count.saveAsTextFile("file:///D:/test/output1")
  }
}
