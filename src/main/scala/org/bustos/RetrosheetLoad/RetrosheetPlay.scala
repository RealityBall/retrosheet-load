package org.bustos.RetrosheetLoad

object RetrosheetPlay {
/*
 * Retrosheet pitch characters
 *  
  B  ball
  C  called strike
  F  foul
  H  hit batter
  I  intentional ball
  K  strike (unknown type)
  L  foul bunt
  M  missed bunt attempt
  N  no pitch (on balks and interference calls)
  O  foul tip on bunt
  P  pitchout
  Q  swinging on pitchout
  R  foul ball on pitchout
  S  swinging strike
  T  foul tip
  U  unknown or missed pitch
  V  called ball because pitcher went to his mouth
  X  ball put into play by batter
  Y  ball put into play on pitchout
*/

  val ballType = List('B', 'I', 'V', 'P')  
}

class RetrosheetPlay (val pitchSeq: String, val play: String) {

  import RetrosheetPlay._
  
  val isStolenBase: Boolean = play.startsWith("SB")
  val baseStolen: Char = {
    if (isStolenBase) play(2)
    else ' '
  }
  val isSingle: Boolean = play.startsWith("S") && !play.startsWith("SB")
  val isDouble: Boolean = play.startsWith("D")  && !play.startsWith("DI") // DI: Defensive indifference, runners may advance
  val isTriple: Boolean = play.startsWith("T")
  val isHomeRun: Boolean = play.startsWith("HR")
  val isStrikeOut: Boolean = play.startsWith("K")
  val isBaseOnBalls: Boolean = (play.startsWith("W") && !play.startsWith("WP")) || play.startsWith("IW")
  val isHitByPitch: Boolean = play.startsWith("HP")
  val isSacFly: Boolean = play.contains("SF")
  val isSacHit: Boolean = play.contains("SH")
  val outs: Int = {
    if (!play.startsWith("E") && !play.contains("K+WP.B-1") && !play.contains("K+PB.B-1") && 
        (isStrikeOut || isSacFly || isSacHit || play(0).isDigit)) 1
    else if (play.contains("DP")) 2
    else if (play.contains("TP")) 3
    else 0
  }
  
  val pitches: String = {
    if (pitchSeq.contains(".")) pitchSeq.substring(pitchSeq.lastIndexOf(".")).filter(x => {!x.isDigit && x != '*' && x != '.' && x != '>'})
    else pitchSeq.filter(x => {!x.isDigit && x != '*' && x != '.' && x != '>'})
  }
  val pitchesByType = pitches.groupBy({ x => x })
  val balls: Int = ballType.foldLeft(0)({(x, y) => x + (if (pitchesByType.contains(y)) pitchesByType(y).length else 0)})

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