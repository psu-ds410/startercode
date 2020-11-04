// This is a scala script for experimenting with more advanced
// RDD operations. To load it in the spark-shell, type
// :load practice_script.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

def showIt[A](myrdd: RDD[A], n: Int = 5): Unit = {
    val piece = myrdd.take(n) // send n records from rdd to dirver program
    //piece is now an array, not an RDD since we performed an action
    piece match {
       case s: Array[Array[String]] => s.foreach{x => println(x.mkString("[", ", ", "]"))} 
       case _ => piece.foreach{println(_)} //display each entry, one per line
    }

}

def warAndPeaceRDD(sc: SparkContext):RDD[String] = {
    sc.textFile("/ds410/warandpeace")
}

def facebookRDD(sc: SparkContext):RDD[(Int, Int)] = {
    val fb = sc.textFile("/ds410/facebook")
    val mapped = fb.map{x => x.split("\t")}
    val cleaned = mapped.map{x => (x(0).toInt, x(1).toInt)}
    cleaned
}

def mapExample(sc: SparkContext):Unit = {
    val wap = warAndPeaceRDD(sc)
    val mapped = wap.map{x => x.split("\\W+")} //regular expression
    showIt(mapped)
}


def filterExample1(sc: SparkContext):Unit = {
    val wap = warAndPeaceRDD(sc)
    val mapped = wap.map{x => x.split("\\W+")}  
    val filtered = mapped.filter{x => !(x.length == 1 && x(0).length == 0)}
    showIt(filtered)
}

def filterExample2(sc: SparkContext):Unit = {
    val fbRDD = facebookRDD(sc)
    val filtered = fbRDD.filter{case (a,b) => a % 2 == 0 && b % 2 == 0}
    //same as
    //val filtered = fbRDD.filter{x => x._1 % 2 == 0 && x._2 % 2 == 0}
    //but we used pattern matching to make it more readable
    showIt(filtered)
}


def flatMapExample(sc: SparkContext):Unit = {
    val wap = warAndPeaceRDD(sc)
    val flat = wap.flatMap{x => x.split("\\W+")} //regular expression
    showIt(flat)
}


def rbkExample(sc: SparkContext):Unit = {
    val wap = warAndPeaceRDD(sc)
    val flat = wap.flatMap{x => x.split("\\W+")} //regular expression
    val withOne = flat.map{x => (x,1)}
    val wordcount = withOne.reduceByKey((accum, x) => accum + x)
    showIt(wordcount)

}

def joinExample(sc: SparkContext):Unit = {
    val fb = facebookRDD(sc)
    val fb2 = facebookRDD(sc)
    val joined = fb.join(fb2)
    showIt(joined)
}

def aggregateByKeyExample(sc: SparkContext):Unit = {
    val fb = facebookRDD(sc)
    //we want to compute, for each node, the number of neighbors and total sum of the neighbor ids
    val comb = fb.aggregateByKey((0, 0))({ 
                                           case ((thecount, thesum), neighbor) => (thecount+1, thesum + neighbor)
                                         },
                                         {
                                           case ((count1, sum1), (count2, sum2)) => (count1 + count2, sum1+sum2)
                                         })
    showIt(comb)

}

def aggregateExample(sc: SparkContext):Unit = {
    val fb = facebookRDD(sc)
    //we want to compute the total number of neighbors and total sum of the neighbor ids
    val agg = fb.aggregate((0, 0))(
                                    { 
                                      case ((thecount, thesum), (node,neighbor)) => (thecount+1, thesum + neighbor)
                                    },
                                    {
                                      case ((count1, sum1), (count2, sum2)) => (count1 + count2, sum1+sum2)
                                    }
                                  )
    println(s"Total Number of Neighbors is: ${agg._1} and their sum is: ${agg._2}")

}


def countFacebook(sc: SparkContext):Unit = {
    val numElements = facebookRDD(sc).count()
    println(s"Number of elements is ${numElements}")
}

def reduceExample(sc: SparkContext):Unit = {
    val wap = warAndPeaceRDD(sc)
    val flat = wap.flatMap{x => x.split("\\W+")} //regular expression
    val lengths = flat.map{x => x.length}
    val tot = lengths.reduce{case (accum, x) => accum + x}
    println(s"Total length of all words is ${tot}")
}



