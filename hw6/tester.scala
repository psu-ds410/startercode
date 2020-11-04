
object Tester extends App {
   val result1: (Int, Int) = HW.q1_top_two(4,2,1)
   println(result1)

   val result2: String  = HW.q2_interpolation("world", 23)
   println(result2)

   val result3a: Double = HW.q3_polynomial(List(4.0,1.0,2.0))
   
   val result3b: Double = HW.q3_polynomial(Vector(4.0,1.0,2.0))
   
   val result4: Int = HW.q4_application(1,2,3){(x,y) => x+y}
   
   val result5: Vector[String] = HW.q5_stringy(4)
   
   val result6: Vector[Int] = HW.q6_modab(3,2, Vector(1,2,3,4,5,6))
   
   val result7: Vector[Int] = HW.q7_modab_map(4,3, Vector(1,2,3,4,5,6,7,8,9))
   
   val result8a: Int = HW.q8_find(List(2,3,4)){x => x%2 == 0}

   val result8b: Int = HW.q8_find(Vector(2,3,4)){x => x%2 == 0}
   
   val result9a: Int = HW.q9_find_tail(List(2,3,4)){x => x%2 == 0}

   val result9b: Int = HW.q9_find_tail(Vector(2,3,4)){x => x%2 == 0}

   val result10a: Double = HW.q10_neumaier(Vector(1.0, 1e100, 1.0, -1e100))

   val result10b: Double = HW.q10_neumaier(List(1.0, 1e100, 1.0, -1e100))


}
