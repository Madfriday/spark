package day7_28

import scala.io.Source


object Utils {
  def ip2Long(ip: String): Long = {
    val segment1 = ip.split("[.]")
    var numL = 0L
    for (a <- 0 until segment1.length) {
      numL = segment1(a).toLong | numL << 8L
    }
    numL
  }

  def readRelus(path: String): Array[(Long, Long, String)] = {
    val bf = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    val relus = lines.map(x => {
      val f = x.split("[|]")
      val fistNum = f(2).toLong
      val secNum = f(3).toLong
      val province = f(6)
      (fistNum, secNum, province)

    }).toArray
    relus
  }

  def binanrySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middel = (low + high) / 2
      if ((ip >= lines(middel)._1) && (ip <= lines(middel)._2)) {
        return middel
      }
      if (ip < lines(middel)._1) {
        high = middel - 1
      }
      else {
        low = middel + 1
      }
    }
    -1
  }
}