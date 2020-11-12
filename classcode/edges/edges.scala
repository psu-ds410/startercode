import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


object Ex {

    def getFB(sc: SparkContext): RDD[(Int, Int)] = {
        val fb = sc.textFile("/ds410/facebook/")
        val splitted = fb.map(x => x.split("\t"))
        val kvrdd = splitted.map(x => (x(0).toInt, x(1).toInt)) //key value rdd, each element is a tuple
        kvrdd
    }


    def isAdjList(sc: SparkContext): Boolean = {
        // returns true if the facebook dataset is an adjacencly list
        // and false otherwise
        val fb = sc.textFile("/ds410/facebook/")
        val splitted = fb.map{x => x.split("\t")}
        val lengths = splitted.map{x => x.length}
        val longrows = lengths.filter{x => x > 2}
        val numberOfLongRows = longrows.count()
        numberOfLongRows != 0 // true if adjacency list, false otherwise  
    }

    def isRedundant(sc: SparkContext): Boolean = {
        val kvrdd = getFB(sc)
        // if (a,b) appears in the facebook rdd, does (b,a) also appear in it? if so, it is redundant
        val flipped = kvrdd.map{case (a,b) => (b,a)} //flipped version of the kv rdd
        // common rdd contains contains all edges from kvrdd whose flipped version is also in kvrdd
        val common = flipped.intersection(kvrdd) 
        common.count() != 0
    }

    def convertFromRedundant(sc: SparkContext): RDD[(Int, Int)] = {
        // take redundant rdd and make it non-redundant
        val kvrdd = getFB(sc)
        val ordered = kvrdd.map{case (a,b) => if(a < b) (a,b) else (b,a)}
        val nonRedundant = ordered.distinct()
        nonRedundant
    }

    def toRedundant(sc: SparkContext): RDD[(Int, Int)] = {
        val kvrdd = getFB(sc)
        val flipped = kvrdd.map{case (a,b) => (b, a)}
        val sparky = kvrdd.union(flipped)
        sparky
    }


    def countTriangles(sc: SparkContext) = {
        val kvrdd = getFB(sc)
        val kvrdd2 = getFB(sc)
        // each element of the rdd has the form (a, (b,c)) meaning that
        // there were edges (a,b) and (a, c) in the original RDD
        // so the graph has a path that looks like b -> a -> c
        val joined = kvrdd.join(kvrdd2)
        // remove tuples that look like (a, (b, b))
        val filtered = joined.filter{case (a, (b, c)) => b != c}        
        // next step: we have rdds where the elements look like (a, (b,c))
        // now we need to check if (b,c) was in the original rdd
        filtered.take(5)
       // (1, 2) join with itself -> (1, (2, 2))  X
       // (1, 3)                     (1, (2, 3))  -> tells us that (1, 2) and (1, 3) were in the rdd 2->1-> 3
       // (2, 1)                     (1, (3, 2))  -> tells us that 3 -> 1 -> 2
       // (2, 3)                     (1, (3, 3))  X
       // (3, 2)                     ...
       // (3, 1)
       // (1, 4)
       // (4, 1)
       //    filtered look this:
       //    (1, (2, 3))
       //    (1, (3, 2))
       //    (1, (2, 4))
       //    (1, (4, 2))
       //    (1, (3, 4))
       //    (1, (4, 3))
       //    (2, (1, 3))
       //    (2, (3, 1))
       //    (3, (1, 2))
       //    (3, (2, 1))
    }
}
