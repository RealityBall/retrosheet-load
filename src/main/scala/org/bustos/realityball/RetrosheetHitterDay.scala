package org.bustos.realityball

import FantasyScoreSheet._
import org.bustos.realityball.common.RealityballRecords._
import org.bustos.realityball.common.RealityballConfig._
import org.joda.time._
import scala.collection.mutable.Queue

object RetrosheetHitterDay {

  case class RunningHitterStatistics(fullAccum: RetrosheetHitterDay, averagesData: RunningHitterData, averagesDates: Queue[DateTime],
                                     volatilityData: RunningHitterData, volatilityDates: Queue[DateTime], var fantasyProduction: Map[DateTime, Double])

}

class RetrosheetHitterDay(var date: DateTime, val id: String, var pitcherId: String, var pitcherIndex: Int, val lineupPosition: Int, val gameId: String, val side: Int) extends StatisticsTrait {

  import RetrosheetHitterDay._

  def year: String = CcyymmddFormatter.print(date).substring(0, 4)

  var lineupPositionRegime = 0
  var productionRate = 0.0
  var daysSinceProduction = 0
  var style: String = ""

  var RHplateAppearance: Int = 0
  var RHatBat: Int = 0
  var RHsingle: Int = 0
  var RHdouble: Int = 0
  var RHtriple: Int = 0
  var RHhomeRun: Int = 0
  var RHRBI: Int = 0
  var RHruns: Int = 0
  var RHbaseOnBalls: Int = 0
  var RHhitByPitch: Int = 0
  var RHsacFly: Int = 0
  var RHsacHit: Int = 0
  var RHstrikeOut: Int = 0
  var RHflyBall: Int = 0
  var RHgroundBall: Int = 0
  var RHstyle: String = ""

  var LHplateAppearance: Int = 0
  var LHatBat: Int = 0
  var LHsingle: Int = 0
  var LHdouble: Int = 0
  var LHtriple: Int = 0
  var LHhomeRun: Int = 0
  var LHRBI: Int = 0
  var LHruns: Int = 0
  var LHbaseOnBalls: Int = 0
  var LHhitByPitch: Int = 0
  var LHsacFly: Int = 0
  var LHsacHit: Int = 0
  var LHstrikeOut: Int = 0
  var LHflyBall: Int = 0
  var LHgroundBall: Int = 0
  var LHstyle: String = ""

  def LHhits(): Int = { LHsingle + LHdouble + LHtriple + LHhomeRun }
  def RHhits(): Int = { RHsingle + RHdouble + RHtriple + RHhomeRun }
  def LHtotalBases(): Int = { LHsingle + 2 * LHdouble + 3 * LHtriple + 4 * LHhomeRun }
  def RHtotalBases(): Int = { RHsingle + 2 * RHdouble + 3 * RHtriple + 4 * RHhomeRun }

  var stolenBase: Int = 0
  var caughtStealing: Int = 0

  val fantasyScoresDates: Queue[DateTime] = Queue.empty[DateTime]

  var dailyBattingAverage = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var battingAverage = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var onBasePercentage = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var sluggingPercentage = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var fantasyScores = FantasyGamesBatting.keys.map(_ -> Statistic(date, 0.0, 0.0, 0.0)).toMap

  var battingAverageMov = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var onBasePercentageMov = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var sluggingPercentageMov = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var fantasyScoresMov = FantasyGamesBatting.keys.map(_ -> Statistic(date, 0.0, 0.0, 0.0)).toMap

  var battingVolatility = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var onBaseVolatility = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var sluggingVolatility = Statistic(date, Double.NaN, Double.NaN, Double.NaN)
  var fantasyScoresVolatility = FantasyGamesBatting.keys.map(_ -> Statistic(date, 0.0, 0.0, 0.0)).toMap

  def gameDays(dates: Queue[DateTime]): Int = {
    dates.distinct.size
  }

  def aggregateFantasyDay(statistic: (DateTime, Queue[Statistic])): Statistic = {
    statistic._2.foldLeft(Statistic(statistic._1, 0.0, 0.0, 0.0))({ case (x, y) => x.total = x.total + y.total; x.rh = x.rh + y.rh; x.lh = x.lh + y.lh; x })
  }

  def accumulate(data: RunningHitterStatistics) = {

    if (year == data.fullAccum.year) {
      data.fullAccum.LHatBat = data.fullAccum.LHatBat + LHatBat
      data.fullAccum.RHatBat = data.fullAccum.RHatBat + RHatBat
      data.fullAccum.LHplateAppearance = data.fullAccum.LHplateAppearance + LHplateAppearance
      data.fullAccum.RHplateAppearance = data.fullAccum.RHplateAppearance + RHplateAppearance
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
    } else {
      data.fullAccum.date = date
      data.fullAccum.LHatBat = LHatBat
      data.fullAccum.RHatBat = RHatBat
      data.fullAccum.LHplateAppearance = LHplateAppearance
      data.fullAccum.RHplateAppearance = RHplateAppearance
      data.fullAccum.LHsingle = LHsingle
      data.fullAccum.RHsingle = RHsingle
      data.fullAccum.LHdouble = LHdouble
      data.fullAccum.RHdouble = RHdouble
      data.fullAccum.LHtriple = LHtriple
      data.fullAccum.RHtriple = RHtriple
      data.fullAccum.LHhomeRun = LHhomeRun
      data.fullAccum.RHhomeRun = RHhomeRun
      data.fullAccum.LHbaseOnBalls = LHbaseOnBalls
      data.fullAccum.RHbaseOnBalls = RHbaseOnBalls
      data.fullAccum.LHhitByPitch = LHhitByPitch
      data.fullAccum.RHhitByPitch = RHhitByPitch
      data.fullAccum.LHsacFly = LHsacFly
      data.fullAccum.RHsacFly = RHsacFly
      data.fullAccum.LHsacHit = LHsacHit
      data.fullAccum.RHsacHit = RHsacHit
    }

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
    if (LHatBat + RHatBat > 0) dailyBattingAverage.total = (LHhits + RHhits).toDouble / (LHatBat + RHatBat).toDouble
    if (data.fullAccum.LHatBat > 0 || data.fullAccum.RHatBat > 0) {
      data.fullAccum.battingAverage.total = (LHaccumHits + RHaccumHits).toDouble / (data.fullAccum.RHatBat + data.fullAccum.LHatBat).toDouble
      data.fullAccum.sluggingPercentage.total = (LHaccumTotalBases + RHaccumTotalBases).toDouble / (data.fullAccum.LHatBat + data.fullAccum.RHatBat).toDouble
    }
    if (data.fullAccum.RHatBat + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch + data.fullAccum.RHsacFly +
      data.fullAccum.LHatBat + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch + data.fullAccum.LHsacFly > 0) {
      data.fullAccum.onBasePercentage.total = (LHaccumHits + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch +
        RHaccumHits + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch).toDouble /
        (data.fullAccum.RHatBat + data.fullAccum.RHbaseOnBalls + data.fullAccum.RHhitByPitch + data.fullAccum.RHsacFly +
          data.fullAccum.LHatBat + data.fullAccum.LHbaseOnBalls + data.fullAccum.LHhitByPitch + data.fullAccum.LHsacFly).toDouble
    }

    battingAverage = data.fullAccum.battingAverage.copy()
    onBasePercentage = data.fullAccum.onBasePercentage.copy()
    sluggingPercentage = data.fullAccum.sluggingPercentage.copy()

    data.averagesDates.enqueue(date)
    data.volatilityDates.enqueue(date)

    data.averagesData.lineupPosition.enqueue(lineupPosition)
    data.averagesData.ba.enqueue(StatisticInputs(LHhits + RHhits, LHatBat + RHatBat, RHhits, RHatBat, LHhits, LHatBat))
    data.averagesData.obp.enqueue(StatisticInputs(LHhits + LHbaseOnBalls + LHhitByPitch + RHhits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly + LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly,
      RHhits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly,
      LHhits + LHbaseOnBalls + LHhitByPitch, LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly))
    data.averagesData.slugging.enqueue(StatisticInputs(LHtotalBases + RHtotalBases, LHatBat + RHatBat, RHtotalBases, RHatBat, LHtotalBases, LHatBat))
    fantasyScores.foreach({
      case (k, v) =>
        if (data.averagesData.fantasy(k).size > 0 && data.averagesData.fantasy(k).last.date == date) {
          val fantasyDate = data.averagesData.fantasy(k).last
          fantasyDate.total = fantasyDate.total + fantasyScores(k).total
          fantasyDate.rh = fantasyDate.rh + fantasyScores(k).rh
          fantasyDate.lh = fantasyDate.lh + fantasyScores(k).lh
        } else data.averagesData.fantasy(k).enqueue(fantasyScores(k).copy())
    })
    data.averagesData.strikeOuts.enqueue(Statistic(date, LHstrikeOut + RHstrikeOut, LHstrikeOut, RHstrikeOut))
    data.averagesData.flyBalls.enqueue(Statistic(date, LHflyBall + RHflyBall, LHflyBall, RHflyBall))
    data.averagesData.groundBalls.enqueue(Statistic(date, LHgroundBall + RHgroundBall, LHgroundBall, RHgroundBall))
    data.averagesData.baseOnBalls.enqueue(Statistic(date, LHbaseOnBalls + RHbaseOnBalls, LHbaseOnBalls, RHbaseOnBalls))

    while (gameDays(data.averagesDates) > MovingAverageWindow) {
      data.averagesDates.dequeue
      data.averagesData.lineupPosition.dequeue
      data.averagesData.ba.dequeue
      data.averagesData.obp.dequeue
      data.averagesData.slugging.dequeue
      data.averagesData.strikeOuts.dequeue
      data.averagesData.flyBalls.dequeue
      data.averagesData.groundBalls.dequeue
      data.averagesData.baseOnBalls.dequeue
    }
    if (data.averagesData.fantasy(FanDuelName).size > MovingAverageWindow) {
      data.averagesData.fantasy.values.foreach(_.dequeue)
    }

    val recentStrikeOuts = data.averagesData.strikeOuts.foldLeft((0.0, 0.0))({ (x, y) => (x._1 + y.rh, x._2 + y.lh) })
    val recentRHstrikeOuts = recentStrikeOuts._1
    val recentLHstrikeOuts = recentStrikeOuts._2
    val recentFlyBalls = data.averagesData.flyBalls.foldLeft((0.0, 0.0))({ (x, y) => (x._1 + y.rh, x._2 + y.lh) })
    val recentRHflyBalls = recentFlyBalls._1
    val recentLHflyBalls = recentFlyBalls._2
    val recentGroundBalls = data.averagesData.groundBalls.foldLeft((0.0, 0.0))({ (x, y) => (x._1 + y.rh, x._2 + y.lh) })
    val recentRHgroundBalls = recentGroundBalls._1
    val recentLHgroundBalls = recentGroundBalls._2
    val recentBaseOnBalls = data.averagesData.baseOnBalls.foldLeft((0.0, 0.0))({ (x, y) => (x._1 + y.rh, x._2 + y.lh) })
    val recentRHbaseOnBalls = recentBaseOnBalls._1
    val recentLHbaseOnBalls = recentBaseOnBalls._2
    val abResultTotals = Statistic(date, recentLHstrikeOuts + recentLHflyBalls + recentLHgroundBalls + recentLHbaseOnBalls + recentRHstrikeOuts + recentRHflyBalls + recentRHgroundBalls + recentRHbaseOnBalls,
      recentRHstrikeOuts + recentRHflyBalls + recentRHgroundBalls + recentRHbaseOnBalls,
      recentLHstrikeOuts + recentLHflyBalls + recentLHgroundBalls + recentLHbaseOnBalls)

    if ((recentRHstrikeOuts + recentLHstrikeOuts) / abResultTotals.total > StrikeOutBatterStyleThreshold) style = StrikeOut
    else if ((recentRHflyBalls + recentLHflyBalls) / abResultTotals.total > FlyballBatterStyleThreshold) style = FlyBall
    else if ((recentRHgroundBalls + recentLHgroundBalls) / abResultTotals.total > GroundballBatterStyleThreshold) style = GroundBall
    else if ((recentRHbaseOnBalls + recentLHbaseOnBalls) / abResultTotals.total > BaseOnBallsBatterStyleThreshold) style = BaseOnBalls

    if (recentLHstrikeOuts / abResultTotals.lh > StrikeOutBatterStyleThreshold) LHstyle = StrikeOut
    else if (recentLHflyBalls / abResultTotals.lh > FlyballBatterStyleThreshold) LHstyle = FlyBall
    else if (recentLHgroundBalls / abResultTotals.lh > GroundballBatterStyleThreshold) LHstyle = GroundBall
    else if (recentLHbaseOnBalls / abResultTotals.lh > BaseOnBallsBatterStyleThreshold) LHstyle = BaseOnBalls

    if (recentRHstrikeOuts / abResultTotals.rh > StrikeOutBatterStyleThreshold) RHstyle = StrikeOut
    else if (recentRHflyBalls / abResultTotals.rh > FlyballBatterStyleThreshold) RHstyle = FlyBall
    else if (recentRHgroundBalls / abResultTotals.rh > GroundballBatterStyleThreshold) RHstyle = GroundBall
    else if (recentRHbaseOnBalls / abResultTotals.rh > BaseOnBallsBatterStyleThreshold) RHstyle = BaseOnBalls

    battingAverageMov = queueMean(data.averagesData.ba, false)
    onBasePercentageMov = queueMean(data.averagesData.obp, false)
    sluggingPercentageMov = queueMean(data.averagesData.slugging, false)
    fantasyScoresMov = fantasyScoresMov.map({
      case (k, v) =>
        k -> queueMeanSimple(data.averagesData.fantasy(k), false)
    }).toMap

    data.volatilityData.ba.enqueue(StatisticInputs(battingAverageMov.total, 1.0, battingAverageMov.rh, 1.0, battingAverageMov.lh, 1.0))
    data.volatilityData.obp.enqueue(StatisticInputs(onBasePercentageMov.total, 1.0, onBasePercentageMov.rh, 1.0, onBasePercentageMov.lh, 1.0))
    data.volatilityData.slugging.enqueue(StatisticInputs(sluggingPercentageMov.total, 1.0, sluggingPercentageMov.rh, 1.0, sluggingPercentageMov.lh, 1.0))
    /*
    data.volatilityData.ba.enqueue(StatisticInputs(LHhits + RHhits, LHatBat + RHatBat, RHhits, RHatBat, LHhits, LHatBat))
    data.volatilityData.obp.enqueue(StatisticInputs(LHhits + LHbaseOnBalls + LHhitByPitch + RHhits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly + LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly,
      RHhits + RHbaseOnBalls + RHhitByPitch, RHatBat + RHbaseOnBalls + RHhitByPitch + RHsacFly,
      LHhits + LHbaseOnBalls + LHhitByPitch, LHatBat + LHbaseOnBalls + LHhitByPitch + LHsacFly))
    data.volatilityData.slugging.enqueue(StatisticInputs(LHtotalBases + RHtotalBases, LHatBat + RHatBat, RHtotalBases, RHatBat, LHtotalBases, LHatBat))
    */
    fantasyScores.foreach({
      case (k, v) =>
        if (data.volatilityData.fantasy(k).size > 0 && data.volatilityData.fantasy(k).last.date == date) {
          val fantasyDate = data.volatilityData.fantasy(k).last
          fantasyDate.total = fantasyDate.total + fantasyScores(k).total
          fantasyDate.rh = fantasyDate.rh + fantasyScores(k).rh
          fantasyDate.lh = fantasyDate.lh + fantasyScores(k).lh
        } else data.volatilityData.fantasy(k).enqueue(fantasyScores(k).copy())
    })

    while (gameDays(data.volatilityDates) > VolatilityWindow) {
      data.volatilityDates.dequeue
      data.volatilityData.ba.dequeue
      data.volatilityData.obp.dequeue
      data.volatilityData.slugging.dequeue
    }
    if (data.volatilityData.fantasy(FanDuelName).size > VolatilityWindow) {
      data.volatilityData.fantasy.values.foreach(_.dequeue)
    }
    //if (id == "cabrm001" && date == "2015/05/01") {
    //  println("")
    //}
    battingVolatility = movingVolatility(data.volatilityData.ba, false)
    onBaseVolatility = movingVolatility(data.volatilityData.obp, false)
    sluggingVolatility = movingVolatility(data.volatilityData.slugging, false)
    fantasyScoresVolatility = fantasyScoresVolatility.map({
      case (k, v) =>
        k -> movingVolatilitySimple(data.volatilityData.fantasy(k), false)
    }).toMap

    // Days between fantasy score production
    if (!data.fantasyProduction.contains(date)) data.fantasyProduction += (date -> data.averagesData.fantasy(FanDuelName).head.total)
    else {
      data.fantasyProduction = data.fantasyProduction.updated(date, { data.averagesData.fantasy(FanDuelName).last.total + data.fantasyProduction(date) })
    }

    // Lineup position regime
    val positionDoubles = data.averagesData.lineupPosition.filter { _ != 0 } map { _.toDouble }
    val averageLineupPosition = mean(positionDoubles)
    val stddevLineupPosition = standardDeviation(positionDoubles)
    if (stddevLineupPosition < 1.5) lineupPositionRegime = averageLineupPosition.round.toInt
  }

  def updateFantasyScore(playOutcome: String, quantity: Int, gameName: String, track: Statistic, facingRighty: Boolean): Statistic = {
    if (facingRighty) {
      track.rh = track.rh + FantasyGamesBatting(gameName)(playOutcome) * quantity
    } else {
      track.lh = track.lh + FantasyGamesBatting(gameName)(playOutcome) * quantity
    }
    track.total = track.total + FantasyGamesBatting(gameName)(playOutcome) * quantity
    track
  }

  def addStolenBase(facingRighty: Boolean) {
    stolenBase = stolenBase + 1
    fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("SB", 1, k, v, facingRighty)) })
  }

  def addRun(facingRighty: Boolean) {
    if (facingRighty) RHruns = RHruns + 1
    else LHruns = LHruns + 1
    fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("R", 1, k, v, facingRighty)) })
  }

  def updateWithPlay(play: RetrosheetPlay, facingRighty: Boolean) = {
    if (play.atBat) {
      if (facingRighty) RHatBat = RHatBat + 1
      else LHatBat = LHatBat + 1
    }

    if (play.plateAppearance) {
      if (facingRighty) RHplateAppearance = RHplateAppearance + 1
      else LHplateAppearance = LHplateAppearance + 1
    }

    if (play.outs > 0) {
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("OUT", 1, k, v, facingRighty)) })
    }
    if (play.isSingle) {
      if (facingRighty) {
        RHsingle = RHsingle + 1
      } else {
        LHsingle = LHsingle + 1
      }
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("1B", 1, k, v, facingRighty)) })
    } else if (play.isDouble) {
      if (facingRighty) {
        RHdouble = RHdouble + 1
      } else {
        LHdouble = LHdouble + 1
      }
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("2B", 1, k, v, facingRighty)) })
    } else if (play.isTriple) {
      if (facingRighty) {
        RHtriple = RHtriple + 1
      } else {
        LHtriple = LHtriple + 1
      }
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("3B", 1, k, v, facingRighty)) })
    } else if (play.isHomeRun) {
      if (facingRighty) {
        RHhomeRun = RHhomeRun + 1
      } else {
        LHhomeRun = LHhomeRun + 1
      }
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("HR", 1, k, v, facingRighty)) })
    } else if (play.isBaseOnBalls) {
      if (facingRighty) {
        RHbaseOnBalls = RHbaseOnBalls + 1
      } else {
        LHbaseOnBalls = LHbaseOnBalls + 1
      }
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("BB", 1, k, v, facingRighty)) })
    } else if (play.isHitByPitch) {
      if (facingRighty) {
        RHhitByPitch = RHhitByPitch + 1
      } else {
        LHhitByPitch = LHhitByPitch + 1
      }
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("HBP", 1, k, v, facingRighty)) })
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
    } else if (play.isStrikeOut) {
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("SO", 1, k, v, facingRighty)) })
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("OUT", 1, k, v, facingRighty)) })
      incrementStrikeOuts(facingRighty)
    }
    if (play.isFlyBall) incrementFlyBall(facingRighty)
    if (play.isGroundBall) incrementGroundBall(facingRighty)
    if (play.rbis > 0) {
      if (facingRighty) {
        RHRBI = RHRBI + play.rbis
        RHruns = RHruns + play.rbis
      } else {
        LHRBI = LHRBI + play.rbis
        LHruns = LHruns + play.rbis
      }
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("RBI", play.rbis, k, v, facingRighty)) })
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("R", play.rbis, k, v, facingRighty)) })
    }
  }

  private def incrementStrikeOuts(facingRighty: Boolean) = {
    if (facingRighty) {
      RHstrikeOut = RHstrikeOut + 1
    } else {
      LHstrikeOut = LHstrikeOut + 1
    }
  }

  private def incrementFlyBall(facingRighty: Boolean) = {
    if (facingRighty) {
      RHflyBall = RHflyBall + 1
    } else {
      LHflyBall = LHflyBall + 1
    }
  }

  private def incrementGroundBall(facingRighty: Boolean) = {
    if (facingRighty) {
      RHgroundBall = RHgroundBall + 1
    } else {
      LHgroundBall = LHgroundBall + 1
    }
  }

}
