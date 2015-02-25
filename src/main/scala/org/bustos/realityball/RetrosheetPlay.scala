package org.bustos.realityball

import RealityballRecords.BallparkDaily

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
  *
  * Retrosheet play descriptors
  *
  AP    appeal play
  BP    pop up bunt
  BG    ground ball bunt
  BGDP  bunt grounded into double play
  BINT  batter interference
  BL    line drive bunt
  BOOT  batting out of turn
  BP    bunt pop up
  BPDP  bunt popped into double play
  BR    runner hit by batted ball
  C     called third strike
  COUB  courtesy batter
  COUF  courtesy fielder
  COUR  courtesy runner
  DP    unspecified double play
  E$    error on $
  F     fly
  FDP   fly ball double play
  FINT  fan interference
  FL    foul
  FO    force out
  G     ground ball
  GDP   ground ball double play
  GTP   ground ball triple play
  IF    infield fly rule
  INT   interference
  IPHR  inside the park home run
  L     line drive
  LDP   lined into double play
  LTP   lined into triple play
  MREV  manager challenge of call on the field
  NDP   no double play credited for this play
  OBS   obstruction (fielder obstructing a runner)
  P     pop fly
  PASS  a runner passed another runner and was called out
  R$    relay throw from the initial fielder to $ with no out made
  RINT  runner interference
  SF    sacrifice fly
  SH    sacrifice hit (bunt)
  TH    throw
  TH%   throw to base %
  TP    unspecified triple play
  UINT  umpire interference
  UREV  umpire review of call on the field
  *
  */
  val groundBallType = List("/BG", "/G", "/SH")
  val flyBallType = List("/BP", "/BL", "/BP", "/F", "/L", "/P", "/SF")
  val ballType = List('B', 'I', 'V', 'P')
}

class RetrosheetPlay(val pitchSeq: String, val play: String) {

  import RetrosheetPlay._

  val isStolenBase: Boolean = play.startsWith("SB")
  val baseStolen: Char = {
    if (isStolenBase) play(2)
    else ' '
  }

  def totalBases: Int = {
    if (isSingle) 1
    else if (isDouble) 2
    else if (isTriple) 3
    else if (isHomeRun) 4
    else 0
  }

  val isSingle: Boolean = play.startsWith("S") && !play.startsWith("SB")
  val isDouble: Boolean = play.startsWith("D") && !play.startsWith("DI") // DI: Defensive indifference, runners may advance
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

  val isGroundBall: Boolean = {
    groundBallType.exists { play.contains(_) }
  }

  val isGroundOut: Boolean = {
    outs > 0 && isGroundBall
  }

  val isFlyBall: Boolean = {
    flyBallType.exists { x => play.contains(x) } && !play.contains("/FO/G")
  }

  val isFlyOut: Boolean = {
    outs > 0 && isFlyBall
  }

  val pitches: String = {
    if (pitchSeq.contains(".")) pitchSeq.substring(pitchSeq.lastIndexOf(".")).filter(x => { !x.isDigit && x != '*' && x != '.' && x != '>' })
    else pitchSeq.filter(x => { !x.isDigit && x != '*' && x != '.' && x != '>' })
  }
  val pitchesByType = pitches.groupBy({ x => x })
  val balls: Int = ballType.foldLeft(0)({ (x, y) => x + (if (pitchesByType.contains(y)) pitchesByType(y).length else 0) })

  val advancements: List[(String, String)] = {
    if (play.contains(".")) {
      val moves = play split ("""\.""") tail
      val resultingMoves = moves map { _.split(";") } flatMap { x => x } filter { _.contains("-") } map { _.split("-") } map { x => (x(0) -> x(1)(0).toString) }
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
    if (!isStolenBase) advancements.filter(x => x._2 == "H").size + { if (isHomeRun) 1 else 0 }
    else 0
  }

  val scoringRunners: List[(String, String)] = {
    advancements.filter(x => x._2 == "H")
  }

  val runsScored: Boolean = !scoringRunners.isEmpty

  val plateAppearance: Boolean = !play.startsWith("NP") && !play.startsWith("PO")

  val atBat: Boolean = !play.contains("SH") && !play.contains("SF") &&
    !play.startsWith("W") && !play.startsWith("WP") &&
    !play.startsWith("IW") && !play.startsWith("HP") &&
    !play.startsWith("NP") && !play.startsWith("DI") &&
    !play.startsWith("BK") && !play.startsWith("SB") &&
    !play.startsWith("PO")
  // SH  Sacrifice Hit
  // SF  Sacrifice Fly
  // W   Walk
  // WP  Wild Pitch
  // IW  Intentional Walk
  // HP  Hit By Pith
  // NP  No Pitch (usually means substitution)
  // DI  Defensive Indifference
  // BK  Balk
  // SB  Stolen Base
  // PO  Picked off runner

  def updateBallpark(ballpark: BallparkDaily, facingRighty: Boolean) = {
    if (facingRighty) {
      ballpark.RHhits = ballpark.RHhits + { if (isSingle || isDouble || isTriple || isHomeRun) 1 else 0 }
      ballpark.RHtotalBases = ballpark.RHtotalBases + totalBases
      ballpark.RHatBat = ballpark.RHatBat + { if (atBat) 1 else 0 }
      ballpark.RHbaseOnBalls = ballpark.RHbaseOnBalls + { if (isBaseOnBalls) 1 else 0 }
      ballpark.RHhitByPitch = ballpark.RHhitByPitch + { if (isHitByPitch) 1 else 0 }
      ballpark.RHsacFly = ballpark.RHsacFly + { if (isSacFly) 1 else 0 }
    } else {
      ballpark.LHhits = ballpark.LHhits + { if (isSingle || isDouble || isTriple || isHomeRun) 1 else 0 }
      ballpark.LHtotalBases = ballpark.LHtotalBases + totalBases
      ballpark.LHatBat = ballpark.LHatBat + { if (atBat) 1 else 0 }
      ballpark.LHbaseOnBalls = ballpark.LHbaseOnBalls + { if (isBaseOnBalls) 1 else 0 }
      ballpark.LHhitByPitch = ballpark.LHhitByPitch + { if (isHitByPitch) 1 else 0 }
      ballpark.LHsacFly = ballpark.LHsacFly + { if (isSacFly) 1 else 0 }
    }
  }

}
