package day7_29

import day7_29.Custom1.Boy
import day7_29.Custom2.Girl

object Compared {

  implicit object Comparing2 extends Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.face == y.face) {
        x.age - y.age
      }
      else {
        y.face - x.face
      }
    }
  }

}