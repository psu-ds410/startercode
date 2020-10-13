import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object WordCount {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/ds410/warandpeace") //in hdfs
    val words = input.flatMap(_.split(" "))
    val kv = words.map(word => (word,1))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts.saveAsTextFile("result") //in your hdfs home directory
  }
}
