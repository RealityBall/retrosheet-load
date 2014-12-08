package org.bustos.RetrosheetLoad

import RetrosheetLoad.RunningStatistics
import scala.collection.mutable.Queue
import scala.math.pow

class RetrosheetHitterDay(val date: String, val playerID: String) {
  
  var RHatBat: Int = 0
  var RHsingle: Int = 0
  var RHdouble: Int = 0
  var RHtriple: Int = 0
  var RHhomeRun: Int = 0
  var RHRBI: Int = 0
  var RHbaseOnBalls: Int = 0
  var RHhitByPitch: Int = 0
  var RHsacFly: Int = 0
  var RHsacHit: Int = 0     

  var LHatBat: Int = 0
  var LHsingle: Int = 0
  var LHdouble: Int = 0
  var LHtriple: Int = 0
  var LHhomeRun: Int = 0
  var LHRBI: Int = 0
  var LHbaseOnBalls: Int = 0
  var LHhitByPitch: Int = 0
  var LHsacFly: Int = 0
  var LHsacHit: Int = 0
  
  var runs: Int = 0
  var stolenBase: Int = 0
  var caughtStealing: Int = 0
  
  var RHbattingAverage: Double = 0.0
  var LHbattingAverage: Double = 0.0
  var battingAverage: Double = 0.0
  var RHonBasePercentage: Double = 0.0
  var LHonBasePercentage: Double = 0.0
  var onBasePercentage: Double = 0.0
  var RHsluggingPercentage: Double = 0.0
  var LHsluggingPercentage: Double = 0.0
  var sluggingPercentage: Double = 0.0
  var RHfantasyScore: Double = 0.0
  var LHfantasyScore: Double = 0.0
  var fantasyScore: Double = 0.0
  
  var RHbattingAverage25: Double = 0.0
  var LHbattingAverage25: Double = 0.0
  var battingAverage25: Double = 0.0
  var RHonBasePercentage25: Double = 0.0
  var LHonBasePercentage25: Double = 0.0
  var onBasePercentage25: Double = 0.0
  var RHsluggingPercentage25: Double = 0.0
  var LHsluggingPercentage25: Double = 0.0
  var sluggingPercentage25: Double = 0.0
  var RHfantasyScore25: Double = 0.0
  var LHfantasyScore25: Double = 0.0
  var fantasyScore25: Double = 0.0
  
  var RHbattingVolatility100: Double = 0.0
  var LHbattingVolatility100: Double = 0.0
  var battingVolatility100: Double = 0.0  
  var RHonBaseVolatility100: Double = 0.0
  var LHonBaseVolatility100: Double = 0.0
  var onBaseVolatility100: Double = 0.0
  var RHsluggingVolatility100: Double = 0.0
  var LHsluggingVolatility100: Double = 0.0
  var sluggingVolatility100: Double = 0.0
  var RHfantasyScoreVolatility100: Double = 0.0
  var LHfantasyScoreVolatility100: Double = 0.0
  var fantasyScoreVolatility100: Double = 0.0
      
  def accumulate(data: RunningStatistics) = {
    data.fullAccum.LHatBat = data.fullAccum.LHatBat + LHatBat
    data.fullAccum.RHatBat = data.fullAccum.RHatBat + RHatBat
    data.fullAccum.LHsingle = data.fullAccum.LHsingle + LHsingle
    data.fullAccum.RHsingle = data.fullAccum.RHsingle + RHsingle
    data.fullAccum.LHdouble = data.fullAccum.LHdouble + LHdouble
    data.fullAccum.RHdouble = data.fullAccum.RHdouble + RHdouble
    data.fullAccum.LHtriple = data.fullAccum.LHtriple + LHtriple
    data.fullAccum.RHtriple = data.fullAccum.RHtriple + RHtriple
    data.fullAccum.LHhomeRun = data.fullAccum.LHhomeRun + LHhomeRun
    data.fullAccum.RHhomeRun = data.fullAccum.RHhomeRun + RHhomeRun
    data.fullAccum.LHbaseOnBalls = data.fullAccum.LHbaseOnBalls + LHbaseOnBalls
    data.fullAccum.RHbaseOnBalls = data.fullAccum.RHbaseOnBalls + RHbaseOnBalls
    data.fullAccum.LHhitByPitch = data.fullAccum.LHhitByPitch + LHhitByPitch
    data.fullAccum.RHhitByPitch = data.fullAccum.RHhitByPitch + RHhitByPitch
    data.fullAccum.LHsacFly = data.fullAccum.LHsacFly + LHsacFly
    data.fullAccum.RHsacFly = data.fullAccum.RHsacFly + RHsacFly
    data.fullAccum.LHsacHit = data.fullAccum.LHsacHit + LHsacHit
    data.fullAccum.RHsacHit = data.fullAccum.RHsacHit + RHsacHit
    val LHaccumHits: Int = data.fullAccum.LHsingle + data.fullAccum.LHdouble + data.fullAccum.LHtriple + data.fullAccum.LHhomeRun
    val RHaccumHits: Int = data.fullAccum.RHsingle + data.fullAccum.RHdouble + data.fullAccum.RHtriple + data.fullAccum.RHhomeRun
    val LHaccumTotalBases: Int = data.fullAccum.LHsingle + 2 * data.fullAccum.LHdouble + 3 * data.fullAccum.LHtriple + 4 * data.fullAccum.LHhomeRun
    val RHaccumTotalBases: Int = data.fullAccum.RHsingle + 2 * data.fullAccum.RHdouble + 3 * data.fullAccum.RHtriple + 4 * data.fullAccum.RHhomeRun
    if (data.fullAccum.LHatBat > 0) {
      data.fullAccum.LHbattingAverage = LHaccumHits.toDouble / data.fullAccum.LHatBat.toDouble
      data.fullAccum.LHsluggingPercentage = LHaccumTotalBases.toDouble / data.fullAccum.LHatBat
      data.fullAccum.LHonBasePercentage = (LHaccumHits + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch).toDouble /
        (data.fullAccum.LHatBat + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch + data.fullAccum.LHsacFly).toDouble
    }
    if (data.fullAccum.RHatBat > 0) {
      data.fullAccum.RHbattingAverage = RHaccumHits.toDouble / data.fullAccum.RHatBat
      data.fullAccum.RHsluggingPercentage = RHaccumTotalBases.toDouble / data.fullAccum.RHatBat
      data.fullAccum.RHonBasePercentage = (RHaccumHits + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch).toDouble /
        (data.fullAccum.RHatBat + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch + data.fullAccum.RHsacFly).toDouble
    }
    if (data.fullAccum.LHatBat > 0 || data.fullAccum.RHatBat > 0) {
      data.fullAccum.battingAverage = (LHaccumHits + RHaccumHits).toDouble / (data.fullAccum.RHatBat + data.fullAccum.LHatBat).toDouble
      data.fullAccum.onBasePercentage = (LHaccumHits + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch +
        RHaccumHits + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch).toDouble /
        (data.fullAccum.RHatBat + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch + data.fullAccum.RHsacFly +
          data.fullAccum.LHatBat + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch + data.fullAccum.LHsacFly).toDouble
      data.fullAccum.sluggingPercentage = (LHaccumTotalBases + RHaccumTotalBases).toDouble / (data.fullAccum.LHatBat + data.fullAccum.RHatBat).toDouble
    }
    LHbattingAverage = data.fullAccum.LHbattingAverage
    RHbattingAverage = data.fullAccum.RHbattingAverage
    battingAverage = data.fullAccum.battingAverage
    LHonBasePercentage = data.fullAccum.LHonBasePercentage
    RHonBasePercentage = data.fullAccum.RHonBasePercentage
    onBasePercentage = data.fullAccum.onBasePercentage
    LHsluggingPercentage = data.fullAccum.LHsluggingPercentage
    RHsluggingPercentage = data.fullAccum.RHsluggingPercentage
    sluggingPercentage = data.fullAccum.sluggingPercentage
    
    val LHhits: Int = LHsingle + LHdouble + LHtriple + LHhomeRun
    val RHhits: Int = RHsingle + RHdouble + RHtriple + RHhomeRun
    val LHtotalBases: Int = LHsingle + 2 * LHdouble + 3 * LHtriple + 4 * LHhomeRun
    val RHtotalBases: Int = RHsingle + 2 * RHdouble + 3 * RHtriple + 4 * RHhomeRun
    data.averagesData.ba.enqueue((LHhits + RHhits, LHatBat + RHatBat))
    data.averagesData.obp.enqueue((LHhits + LHbaseOnBalls + LHhitByPitch + RHaccumHits + RHbaseOnBalls + RHhitByPitch,
                                   RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly + LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly))
    data.averagesData.slugging.enqueue((LHtotalBases + RHtotalBases, LHatBat + RHatBat))
    data.averagesData.baRH.enqueue((RHhits, RHatBat))
    data.averagesData.obpRH.enqueue((RHaccumHits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly))
    data.averagesData.sluggingRH.enqueue((RHtotalBases, RHatBat))
    data.averagesData.baLH.enqueue((LHhits, LHatBat))
    data.averagesData.obpLH.enqueue((LHaccumHits + LHbaseOnBalls + LHhitByPitch, LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly))
    data.averagesData.sluggingLH.enqueue((LHtotalBases, LHatBat))
    if (data.averagesData.ba.size > 25) {
      data.averagesData.ba.dequeue
      data.averagesData.obp.dequeue
      data.averagesData.slugging.dequeue
      data.averagesData.baRH.dequeue
      data.averagesData.obpRH.dequeue
      data.averagesData.sluggingRH.dequeue
      data.averagesData.baLH.dequeue
      data.averagesData.obpLH.dequeue
      data.averagesData.sluggingLH.dequeue
      
      RHbattingAverage25 = movingAverage(data.averagesData.baRH)
      LHbattingAverage25 = movingAverage(data.averagesData.baLH)
      battingAverage25 = movingAverage(data.averagesData.ba)
      RHonBasePercentage25 = movingAverage(data.averagesData.obpRH)
      LHonBasePercentage25 = movingAverage(data.averagesData.obpLH)
      onBasePercentage25 = movingAverage(data.averagesData.obp)
      RHsluggingPercentage25 = movingAverage(data.averagesData.sluggingRH)
      LHsluggingPercentage25 = movingAverage(data.averagesData.sluggingLH)
      sluggingPercentage25 = movingAverage(data.averagesData.slugging)
      
      //RHfantasyScore25 = movingAverage(data.averagesData.)
      //LHfantasyScore25 = movingAverage(data.averagesData.)
      //fantasyScore25 = movingAverage(data.averagesData.)
    }
  
    data.volatilityData.ba.enqueue((LHhits + RHhits, LHatBat + RHatBat))
    data.volatilityData.obp.enqueue((LHhits + LHbaseOnBalls + LHhitByPitch + RHaccumHits + RHbaseOnBalls + RHhitByPitch,
                                     RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly + LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly))
    data.volatilityData.slugging.enqueue((LHtotalBases + RHtotalBases, LHatBat + RHatBat))
    data.volatilityData.baRH.enqueue((RHhits, RHatBat))
    data.volatilityData.obpRH.enqueue((RHaccumHits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly))
    data.volatilityData.sluggingRH.enqueue((RHtotalBases, RHatBat))
    data.volatilityData.baLH.enqueue((LHhits, LHatBat))
    data.volatilityData.obpLH.enqueue((LHaccumHits + LHbaseOnBalls + LHhitByPitch, LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly))
    data.volatilityData.sluggingLH.enqueue((LHtotalBases, LHatBat))
    if (data.volatilityData.ba.size > 100) {
      data.volatilityData.ba.dequeue
      data.volatilityData.obp.dequeue
      data.volatilityData.slugging.dequeue
      data.volatilityData.baRH.dequeue
      data.volatilityData.obpRH.dequeue
      data.volatilityData.sluggingRH.dequeue
      data.volatilityData.baLH.dequeue
      data.volatilityData.obpLH.dequeue
      data.volatilityData.sluggingLH.dequeue
      RHbattingVolatility100 = movingVolatility(data.volatilityData.baRH)
      LHbattingVolatility100 = movingVolatility(data.volatilityData.baLH)
      battingVolatility100 = movingVolatility(data.volatilityData.ba)  
      RHonBaseVolatility100 = movingVolatility(data.volatilityData.obpRH)
      LHonBaseVolatility100 = movingVolatility(data.volatilityData.obpLH)
      onBaseVolatility100 = movingVolatility(data.volatilityData.obp)
      RHsluggingVolatility100 = movingVolatility(data.volatilityData.sluggingRH)
      LHsluggingVolatility100 = movingVolatility(data.volatilityData.sluggingLH)
      sluggingVolatility100 = movingVolatility(data.volatilityData.slugging)
      //RHfantasyScoreVolatility100 = movingVolatility(data.volatilityData.)
      //LHfantasyScoreVolatility100 = movingVolatility(data.volatilityData.)
      //fantasyScoreVolatility100 = movingVolatility(data.volatilityData.)      
    }
  }

  def movingAverage(data: Queue[(Int, Int)]): Double = {
    val runningSum = data.foldLeft((0, 0))({(x, y) => (x._1 + y._1, x._2 + y._2)})
    if (runningSum._2 > 0) runningSum._1.toDouble / runningSum._2.toDouble
    else 0.0
  }

  def movingVolatility(data: Queue[(Int, Int)]): Double = {
    val average = movingAverage(data)    
    val runningSum = data.foldLeft((0.0, 0))({(x, y) => {
      if (y._2 > 0) (x._1 + pow(y._1.toDouble / y._2.toDouble - average, 2.0), x._2 + 1)
      else x
    }})
    if (runningSum._2 > 0) runningSum._1 / runningSum._2
    else 0.0
  }

  def addStolenBase(play: RetrosheetPlay, facingRighty: Boolean) {
    stolenBase = stolenBase + 1
    if (facingRighty) RHfantasyScore = RHfantasyScore + 2
    else LHfantasyScore = LHfantasyScore + 2
    fantasyScore = fantasyScore + 2
  }

  def updateWithPlay(play: RetrosheetPlay, facingRighty: Boolean) = {
    if (play.atBat) {
      if (facingRighty) RHatBat = RHatBat + 1
      else LHatBat = LHatBat + 1
    }
    if (play.isSingle) {
      if (facingRighty) {
        RHsingle = RHsingle + 1
        RHfantasyScore = RHfantasyScore + 1
      } else {
        LHsingle = LHsingle + 1
        LHfantasyScore = LHfantasyScore + 1
      }
      fantasyScore = fantasyScore + 1
    } else if (play.isDouble) {
      if (facingRighty) {
        RHdouble = RHdouble + 1
        RHfantasyScore = RHfantasyScore + 2
      } else {
        LHdouble = LHdouble + 1
        LHfantasyScore = LHfantasyScore + 2
      }
      fantasyScore = fantasyScore + 2
    } else if (play.isTriple) {
      if (facingRighty) {
        RHtriple = RHtriple + 1
        RHfantasyScore = RHfantasyScore + 3
      } else {
        LHtriple = LHtriple + 1
        LHfantasyScore = LHfantasyScore + 3
      }
      fantasyScore = fantasyScore + 3
    } else if (play.isHomeRun) {
      if (facingRighty) {
        RHhomeRun = RHhomeRun + 1
        RHfantasyScore = RHfantasyScore + 4
      } else {
        LHhomeRun = LHhomeRun + 1
        LHfantasyScore = LHfantasyScore + 4
      }
      fantasyScore = fantasyScore + 4
    } else if (play.isBaseOnBalls) {
      if (facingRighty) {
        RHbaseOnBalls = RHbaseOnBalls + 1
        RHfantasyScore = RHfantasyScore + 1
      } else {
        LHbaseOnBalls = LHbaseOnBalls + 1
        LHfantasyScore = LHfantasyScore + 1
      }
      fantasyScore = fantasyScore + 1
    } else if (play.isHitByPitch) {
      if (facingRighty) {
        RHhitByPitch = RHhitByPitch + 1
        RHfantasyScore = RHfantasyScore + 1
      } else {
        LHhitByPitch = LHhitByPitch + 1
        LHfantasyScore = LHfantasyScore + 1
      }
      fantasyScore = fantasyScore + 1
    } else if (play.isSacFly) {
      if (facingRighty) {
        RHsacFly = RHsacFly + 1
      } else {
        LHsacFly = LHsacFly + 1
      }
    } else if (play.isSacHit) {
      if (facingRighty) {
        RHsacHit = RHsacHit + 1
      } else {
        LHsacHit = LHsacHit + 1
      }
    }
    if (facingRighty) {
      RHRBI = RHRBI + play.rbis
      RHfantasyScore = RHfantasyScore + play.rbis
    } else {
      LHRBI = LHRBI + play.rbis
      LHfantasyScore = LHfantasyScore + play.rbis
    }
    fantasyScore = fantasyScore + play.rbis
  }
}