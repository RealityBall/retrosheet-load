package org.bustos.RetrosheetLoad

class RetrosheetPlay (val play: String) {
  
  val isStolenBase: Boolean = play.startsWith("SB")
  val baseStolen: Char = {
    if (isStolenBase) play(2)
    else ' '
  }
  val isSingle: Boolean = play.startsWith("S") && !play.startsWith("SB")
  val isDouble: Boolean = play.startsWith("D")  && !play.startsWith("DI") // DI: Defensive indifference, runners may advance
  val isTriple: Boolean = play.startsWith("T")
  val isHomeRun: Boolean = play.startsWith("HR")
  val isBaseOnBalls: Boolean = (play.startsWith("W") && !play.startsWith("WP")) || play.startsWith("IW")
  val isHitByPitch: Boolean = play.startsWith("HP")
  val isSacFly: Boolean = play.contains("SF")
  val isSacHit: Boolean = play.contains("SH")

  val advancements: List[(String, String)] = {
    if (play.contains(".")) {
      val moves = play split("""\.""") tail
      val resultingMoves = moves map {_.split(";")} flatMap {x => x} filter {_.contains("-")} map {_.split("-")} map {x => (x(0) -> x(1)(0).toString)}
      resultingMoves.toList.sortWith((x, y) => y._2 < x._2 || x._2 == "B")
    } else List()
  }
  
  val resultingPosition: Int = {
    if (isSingle || isBaseOnBalls || isHitByPitch) 1
    else if (isDouble) 2
    else if (isTriple) 3
    else if (!advancements.filter(x => x._1 == "B" && !x._2.startsWith("H")).isEmpty) {
      // Batter advanced due to Error
      //println(play)
      val position = advancements.filter(x => x._1 == "B" && !x._2.startsWith("H")).map(_._2)
      position.head.toInt
    } else 0
  }

  val rbis: Int = {
    if (!isStolenBase) advancements.filter(x => x._2 == "H").size
    else 0
  }

   val atBat: Boolean = !play.contains("SH") && !play.contains("SF") && 
                        !play.startsWith("W") && !play.startsWith("WP") && 
                        !play.startsWith("IW") && !play.startsWith("HP") && 
                        !play.startsWith("NP") && !play.startsWith("DI") && !play.startsWith("BK")

}