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
        //val kvrdd =  toyGraph(sc)  
        //val kvrdd2 =  toyGraph(sc)
        val kvrdd = getFB(sc)
        val kvrdd2 = getFB(sc) 
        // each element of the rdd has the form (a, (b,c)) meaning that
        // there were edges (a,b) and (a, c) in the original RDD
        // so the graph has a path that looks like b -> a -> c
        val joined = kvrdd.join(kvrdd2)
        // remove tuples that look like (a, (b, b)) because they will not be part of the triangle
        // we also know there is a redundancy: (1, (2,3)) is a potential triangle (and a real triangle if (2,3) is in the fb rdd)
        // we also know that (1, (3,2)) is the same potential triangle (same three nodes involved)
        // we only need one of these (1, (3,2)) or (1, (2,3)) because if (2,3) is in the fb dataset (completing the triangle)
        //   then so would (3,2).
        val filtered = joined.filter{case (a, (b, c)) => b < c}        
        // next step: we have rdds where the elements look like (a, (b,c))
        // now we need to check if (b,c) was in the original rdd
        // we are going to do a join. We want to match the value in (1, (2,3)) to the entire record (2,3) in the fb rdd, but join only allows us to match keys
        val swapped = filtered.map{ case (a,b)  => (b, a)} // turns (1, (2,3)) into ((2,3), 1)
        val fbReadyForJoin =  kvrdd.map{x => (x, -1)} // turns (2, 3) into ((2,3), -1), we don't care about the value, we just the key
        // now we are ready to do the join
        val almostThere = swapped.join(fbReadyForJoin)
        // If (1, (2,3)) is in filtered and (2,3) is in kvrdd, then we have a triangle. But if this true, 
        // we should also have (2, (1,3)) in filtered (because (2, 1) will join with (2, 3), and then (1,3) is in kvrdd, and this is the same triangle
        // and we should also have (3, (1, 2)) in filtered and (1,2) in kvrdd
        almostThere.count() / 3   // so we divide by 3 to avoid triple counting, note when dividing 2 integers, scala does integer division
                                 // but this is ok, because we are triple counting triangles
                                 // this is the return value
        // (1, 2) join with itself -> (1, (2, 2))  
        // (1, 3)                     (1, (2, 3))  -> tells us that (1, 2) and (1, 3) were in the rdd 2->1-> 3
        // (2, 1)                     (1, (3, 2))  -> tells us that 3 -> 1 -> 2
        // (2, 3)                     (1, (3, 3))  
        // (3, 2)                     ...
        // (3, 1)                     swapped rdd looks like ((2,3), 1)
        // (1, 4)                     fbReadyForJoin looks like ((2,3), -1)
        // (4, 1)                     almostThere rdd looks like: ((2,3), (1, -1)) 
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


   def toyGraph(sc: SparkContext): RDD[(Int, Int)] = {
       val toy = List((1, 2),
                      (2, 1),
                      (1, 3),
                      (3, 1),
                      (3, 4),
                      (4, 3),
                      (2, 4),
                      (4, 2),
                      (4, 5),
                      (5, 4),
                      (2, 5),
                      (5, 2)
                     )
       sc.parallelize(toy, 4) //distribute the toy list among 4 workers, returns an RDD



    //    1 ----- 2\
    //    |       | \
    //    |       |  \
    //    3 -----4 -- 5

  }


   def toyGraph2(sc: SparkContext): RDD[(Int, Int)] = {
       val toy = List((1, 2),
                      (2, 1),
                      (1, 3),
                      (3, 1),
                      (3, 4),
                      (4, 3),
                      (2, 4),
                      (4, 2),
                      (4, 5),
                      (5, 4),
                      (2, 5),
                      (5, 2),
                      (2, 6),
                      (6, 2),
                      (5, 6),
                      (6, 5)
                     )
       sc.parallelize(toy, 4) //distribute the toy list among 4 workers, returns an RDD



    //    1 ----- 2\--6
    //    |       | \ |
    //    |       |  \|
    //    3 -----4 -- 5

  }
}




