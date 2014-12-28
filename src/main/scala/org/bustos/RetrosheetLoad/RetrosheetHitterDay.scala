package org.bustos.RetrosheetLoad

import scala.collection.mutable.Queue
import scala.math.{ pow, sqrt }

class RetrosheetHitterDay(val date: String, val playerID: String) {
  
  import RetrosheetRecords._
  
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
  
  var dailyBattingAverage = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var battingAverage = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var onBasePercentage = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var sluggingPercentage = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var fantasyScore = Statistic(0.0, 0.0, 0.0)
  
  var battingAverage25 = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var onBasePercentage25 = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var sluggingPercentage25 = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var fantasyScore25 = Statistic(Double.NaN, Double.NaN, Double.NaN)
  
  var battingVolatility100 = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var onBaseVolatility100 = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var sluggingVolatility100 = Statistic(Double.NaN, Double.NaN, Double.NaN)
  var fantasyScoreVolatility100 = Statistic(Double.NaN, Double.NaN, Double.NaN)
      
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
    val LHhits: Int = LHsingle + LHdouble + LHtriple + LHhomeRun
    val RHhits: Int = RHsingle + RHdouble + RHtriple + RHhomeRun
    val LHtotalBases: Int = LHsingle + 2 * LHdouble + 3 * LHtriple + 4 * LHhomeRun
    val RHtotalBases: Int = RHsingle + 2 * RHdouble + 3 * RHtriple + 4 * RHhomeRun
    val LHaccumHits: Int = data.fullAccum.LHsingle + data.fullAccum.LHdouble + data.fullAccum.LHtriple + data.fullAccum.LHhomeRun
    val RHaccumHits: Int = data.fullAccum.RHsingle + data.fullAccum.RHdouble + data.fullAccum.RHtriple + data.fullAccum.RHhomeRun
    val LHaccumTotalBases: Int = data.fullAccum.LHsingle + 2 * data.fullAccum.LHdouble + 3 * data.fullAccum.LHtriple + 4 * data.fullAccum.LHhomeRun
    val RHaccumTotalBases: Int = data.fullAccum.RHsingle + 2 * data.fullAccum.RHdouble + 3 * data.fullAccum.RHtriple + 4 * data.fullAccum.RHhomeRun
    if (data.fullAccum.LHatBat > 0) {
      if (LHatBat > 0) dailyBattingAverage.lh = LHhits.toDouble / LHatBat.toDouble
      data.fullAccum.battingAverage.lh = LHaccumHits.toDouble / data.fullAccum.LHatBat.toDouble
      data.fullAccum.sluggingPercentage.lh = LHaccumTotalBases.toDouble / data.fullAccum.LHatBat
      data.fullAccum.onBasePercentage.lh = (LHaccumHits + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch).toDouble /
        (data.fullAccum.LHatBat + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch + data.fullAccum.LHsacFly).toDouble
    }
    if (data.fullAccum.RHatBat > 0) {
      if (RHatBat > 0) dailyBattingAverage.rh = RHhits.toDouble / RHatBat.toDouble
      data.fullAccum.battingAverage.rh = RHaccumHits.toDouble / data.fullAccum.RHatBat
      data.fullAccum.sluggingPercentage.rh = RHaccumTotalBases.toDouble / data.fullAccum.RHatBat
      data.fullAccum.onBasePercentage.rh = (RHaccumHits + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch).toDouble /
        (data.fullAccum.RHatBat + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch + data.fullAccum.RHsacFly).toDouble
    }
    if (data.fullAccum.LHatBat > 0 || data.fullAccum.RHatBat > 0) {
      if (LHatBat + RHatBat > 0) dailyBattingAverage.total = (LHhits + RHhits).toDouble / (LHatBat + RHatBat).toDouble
      data.fullAccum.battingAverage.total = (LHaccumHits + RHaccumHits).toDouble / (data.fullAccum.RHatBat + data.fullAccum.LHatBat).toDouble
      data.fullAccum.onBasePercentage.total = (LHaccumHits + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch +
        RHaccumHits + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch).toDouble /
        (data.fullAccum.RHatBat + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch + data.fullAccum.RHsacFly +
          data.fullAccum.LHatBat + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch + data.fullAccum.LHsacFly).toDouble
      data.fullAccum.sluggingPercentage.total = (LHaccumTotalBases + RHaccumTotalBases).toDouble / (data.fullAccum.LHatBat + data.fullAccum.RHatBat).toDouble
    }
    data.fullAccum.fantasyScore = fantasyScore.copy()
    
    battingAverage = data.fullAccum.battingAverage.copy()
    onBasePercentage = data.fullAccum.onBasePercentage.copy()
    sluggingPercentage = data.fullAccum.sluggingPercentage.copy()
    fantasyScore = data.fullAccum.fantasyScore.copy()
    
    data.averagesData.ba.enqueue(StatisticInputs(LHhits + RHhits, LHatBat + RHatBat, RHhits, RHatBat, LHhits, LHatBat))
    data.averagesData.obp.enqueue(StatisticInputs(LHhits + LHbaseOnBalls + LHhitByPitch + RHhits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly + LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly,
                                                  RHhits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly,
                                                  LHhits + LHbaseOnBalls + LHhitByPitch, LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly))
    data.averagesData.slugging.enqueue(StatisticInputs(LHtotalBases + RHtotalBases, LHatBat + RHatBat, RHtotalBases, RHatBat, LHtotalBases, LHatBat))
    data.averagesData.fantasy.enqueue(fantasyScore)
    if (data.averagesData.ba.size > 25) {
      data.averagesData.ba.dequeue
      data.averagesData.obp.dequeue
      data.averagesData.slugging.dequeue
      data.averagesData.fantasy.dequeue
    }      
    battingAverage25 = movingAverage(data.averagesData.ba)
    onBasePercentage25 = movingAverage(data.averagesData.obp)
    sluggingPercentage25 = movingAverage(data.averagesData.slugging)
    fantasyScore25 = movingAverageSimple(data.averagesData.fantasy)
  
    data.volatilityData.ba.enqueue(StatisticInputs(LHhits + RHhits, LHatBat + RHatBat, RHhits, RHatBat, LHhits, LHatBat))
    data.volatilityData.obp.enqueue(StatisticInputs(LHhits + LHbaseOnBalls + LHhitByPitch + RHhits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly + LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly,
                                                    RHhits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly,
                                                    LHhits + LHbaseOnBalls + LHhitByPitch, LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly))
    data.volatilityData.slugging.enqueue(StatisticInputs(LHtotalBases + RHtotalBases, LHatBat + RHatBat, RHtotalBases, RHatBat, LHtotalBases, LHatBat))
    if (data.volatilityData.ba.size > 100) {
      data.volatilityData.ba.dequeue
      data.volatilityData.obp.dequeue
      data.volatilityData.slugging.dequeue
    }
    battingVolatility100 = movingVolatility(data.volatilityData.ba)  
    onBaseVolatility100 = movingVolatility(data.volatilityData.obp)
    sluggingVolatility100 = movingVolatility(data.volatilityData.slugging)
    fantasyScoreVolatility100 = movingVolatilitySimple(data.volatilityData.fantasy)      
  }

  def movingAverageSimple(data: Queue[Statistic]): Statistic = {
    val runningSum = data.foldLeft(Statistic(0.0, 0.0, 0.0))({(x, y) => Statistic(accum(x.total, y.total), accum(x.rh, y.rh), accum(x.lh, y.lh))})
    val runningCounts = data.foldLeft(Statistic(0.0, 0.0, 0.0))({(x, y) => Statistic(accumCount(x.total, y.total), accumCount(x.rh, y.rh), accumCount(x.lh, y.lh))})
    if (data.size > 0) Statistic(runningSum.total.toDouble / runningCounts.total.toDouble, 
                                 runningSum.rh.toDouble / runningCounts.rh.toDouble, 
                                 runningSum.lh.toDouble / runningCounts.lh.toDouble)
    else Statistic(Double.NaN, Double.NaN, Double.NaN)
  }

  def movingAverage(data: Queue[StatisticInputs]): Statistic = {
    val runningSum = data.foldLeft(StatisticInputs(0, 0, 0, 0, 0, 0))({(x, y) => StatisticInputs(x.totalNumer + y.totalNumer, x.totalDenom + y.totalDenom, 
                                                                                                 x.rhNumer + y.rhNumer, x.rhDenom + y.rhDenom, 
                                                                                                 x.lhNumer + y.lhNumer, x.lhDenom + y.lhDenom)})
    if (data.size > 0) Statistic(if (runningSum.totalDenom > 0.0) runningSum.totalNumer.toDouble / runningSum.totalDenom.toDouble else Double.NaN, 
                                 if (runningSum.rhDenom > 0.0) runningSum.rhNumer.toDouble / runningSum.rhDenom.toDouble else Double.NaN, 
                                 if (runningSum.lhDenom > 0.0) runningSum.lhNumer.toDouble / runningSum.lhDenom.toDouble else Double.NaN)
    else Statistic(Double.NaN, Double.NaN, Double.NaN)
  }

  def movingVolatilitySimple(data: Queue[Statistic]): Statistic = {
    val average = movingAverageSimple(data)    
    val runningAccum = data.foldLeft(Statistic(0.0, 0.0, 0.0))({(x, y) => Statistic(x.total + pow(y.total - average.total, 2.0), x.rh + pow(y.rh - average.rh, 2.0), x.lh + pow(y.lh - average.lh, 2.0))})
    val runningCounts = data.foldLeft(Statistic(0.0, 0.0, 0.0))({(x, y) => Statistic(accumCount(x.total, y.total), accumCount(x.rh, y.rh), accumCount(x.lh, y.lh))})
    Statistic(sqrt(runningAccum.total / runningCounts.total), sqrt(runningAccum.rh / runningCounts.rh), sqrt(runningAccum.lh / runningCounts.lh))
  }

  def movingVolatility(data: Queue[StatisticInputs]): Statistic = {
    val average = movingAverage(data)    
    val runningTotal = data.foldLeft((0.0, 0))({(x, y) => {
      if (y.totalDenom > 0) (x._1 + pow(y.totalNumer.toDouble / y.totalDenom.toDouble - average.total, 2.0), x._2 + 1)
      else x
    }})   
    val runningRH = data.foldLeft((0.0, 0))({(x, y) => {
      if (y.rhDenom > 0) (x._1 + pow(y.rhNumer.toDouble / y.rhDenom.toDouble - average.rh, 2.0), x._2 + 1)
      else x
    }})   
    val runningLH = data.foldLeft((0.0, 0))({(x, y) => {
      if (y.lhDenom > 0) (x._1 + pow(y.lhNumer.toDouble / y.lhDenom.toDouble - average.lh, 2.0), x._2 + 1)
      else x
    }})
    Statistic(sqrt(runningTotal._1 / runningTotal._2.toDouble), sqrt(runningRH._1 / runningRH._2.toDouble), sqrt(runningLH._1 / runningLH._2.toDouble))
  }

  def accum(x: Double, y: Double): Double = {
    if (!y.isNaN) {
      if (x.isNaN) y
      else x + y
    } else x
  }
  
  def accumCount(x: Double, y: Double): Double = {
    if (!y.isNaN) {
      if (x.isNaN) 1
      else x + 1
    } else x
  }
  
  def addStolenBase(play: RetrosheetPlay, facingRighty: Boolean) {
    stolenBase = stolenBase + 1
    if (facingRighty) fantasyScore.rh = fantasyScore.rh + 2
    else fantasyScore.lh = fantasyScore.lh + 2
    fantasyScore.total = fantasyScore.total + 2
  }

  def updateWithPlay(play: RetrosheetPlay, facingRighty: Boolean) = {
    if (play.atBat) {
      if (facingRighty) RHatBat = RHatBat + 1
      else LHatBat = LHatBat + 1
    }
    if (play.isSingle) {
      if (facingRighty) {
        RHsingle = RHsingle + 1
        fantasyScore.rh = fantasyScore.rh + 1
      } else {
        LHsingle = LHsingle + 1
        fantasyScore.lh = fantasyScore.lh + 1
      }
      fantasyScore.total = fantasyScore.total + 1
    } else if (play.isDouble) {
      if (facingRighty) {
        RHdouble = RHdouble + 1
        fantasyScore.rh = fantasyScore.rh + 2
      } else {
        LHdouble = LHdouble + 1
        fantasyScore.lh = fantasyScore.lh + 2
      }
      fantasyScore.total = fantasyScore.total + 2
    } else if (play.isTriple) {
      if (facingRighty) {
        RHtriple = RHtriple + 1
        fantasyScore.rh = fantasyScore.rh + 3
      } else {
        LHtriple = LHtriple + 1
        fantasyScore.lh = fantasyScore.lh + 3
      }
      fantasyScore.total = fantasyScore.total + 3
    } else if (play.isHomeRun) {
      if (facingRighty) {
        RHhomeRun = RHhomeRun + 1
        fantasyScore.rh = fantasyScore.rh + 4
      } else {
        LHhomeRun = LHhomeRun + 1
        fantasyScore.lh = fantasyScore.lh + 4
      }
      fantasyScore.total = fantasyScore.total + 4
    } else if (play.isBaseOnBalls) {
      if (facingRighty) {
        RHbaseOnBalls = RHbaseOnBalls + 1
        fantasyScore.rh = fantasyScore.rh + 1
      } else {
        LHbaseOnBalls = LHbaseOnBalls + 1
        fantasyScore.lh = fantasyScore.lh + 1
      }
      fantasyScore.total = fantasyScore.total + 1
    } else if (play.isHitByPitch) {
      if (facingRighty) {
        RHhitByPitch = RHhitByPitch + 1
        fantasyScore.rh = fantasyScore.rh + 1
      } else {
        LHhitByPitch = LHhitByPitch + 1
        fantasyScore.lh = fantasyScore.lh + 1
      }
      fantasyScore.total = fantasyScore.total + 1
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
      fantasyScore.rh = fantasyScore.rh + play.rbis
    } else {
      LHRBI = LHRBI + play.rbis
      fantasyScore.lh = fantasyScore.lh + play.rbis
    }
    fantasyScore.total = fantasyScore.total + play.rbis
  }
}