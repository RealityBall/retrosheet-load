package org.bustos.realityball

import org.joda.time._
import FantasyScoreSheet._
import org.bustos.realityball.common.RealityballRecords.{ PitcherDaily, Statistic }
import org.bustos.realityball.common.RealityballConfig._

object RetrosheetPitcherDay {

  import scala.collection.mutable.Queue

  case class RunningPitcherStatistics(fullAccum: RetrosheetPitcherDay, fantasy: Map[String, Queue[Statistic]], recentStrikeOuts: Queue[Int], recentFlyouts: Queue[Int], recentGroundouts: Queue[Int])
}

class RetrosheetPitcherDay(val id: String, val game: String, var date: DateTime, opposing: String, win: Int, loss: Int, save: Int) extends StatisticsTrait {

  import RetrosheetPitcherDay._

  val MovingAverageGameWindow = 25

  val record = new PitcherDaily(id, game, CcyymmddFormatter.print(date), 0, opposing, win, loss, save, 0, 0, 0, 0, 0, 0, 0, 0, false, false, 0, 0, "")

  var fantasyScores = FantasyGamesPitching.keys.map(_ -> Statistic(CcyymmddFormatter.print(date), 0.0, 0.0, 0.0)).toMap
  var fantasyScoresMov = FantasyGamesPitching.keys.map(_ -> Statistic(CcyymmddFormatter.print(date), 0.0, 0.0, 0.0)).toMap

  def updateFantasyScore(playOutcome: String, gameName: String, track: Statistic): Statistic = {
    track.total = track.total + FantasyGamesPitching(gameName)(playOutcome)
    track
  }

  def accumulate(data: RunningPitcherStatistics) = {
    data.fullAccum.fantasyScores = fantasyScores
    fantasyScores.map({ case (k, v) => data.fantasy(k).enqueue(fantasyScores(k)) })
    if (data.fantasy.size > MovingAverageGameWindow) {
      data.fantasy.mapValues(_.dequeue)
    }
    data.recentStrikeOuts.enqueue(record.strikeOuts)
    data.recentFlyouts.enqueue(record.flyOuts)
    data.recentGroundouts.enqueue(record.groundOuts)
    if (data.recentStrikeOuts.size > MovingAverageGameWindow) {
      data.recentStrikeOuts.dequeue
      data.recentFlyouts.dequeue
      data.recentGroundouts.dequeue
    }
    val recentStrikeOuts = data.recentStrikeOuts.foldLeft(0)(_ + _)
    val recentFlyouts = data.recentFlyouts.foldLeft(0)(_ + _)
    val recentGroundouts = data.recentGroundouts.foldLeft(0)(_ + _)
    val totalOuts = recentStrikeOuts + recentFlyouts + recentGroundouts
    if (totalOuts > 0) {
      if (recentStrikeOuts.toDouble / totalOuts.toDouble > StrikeOutPitcherStyleThreshold) record.style = StrikeOut
      else if (recentFlyouts.toDouble / totalOuts.toDouble > FlyballPitcherStyleThreshold) record.style = FlyBall
      else if (recentGroundouts.toDouble / totalOuts.toDouble > GroundballPitcherStyleThreshold) record.style = GroundBall
    }

    fantasyScoresMov = fantasyScoresMov.map({ case (k, v) => k -> queueMeanSimple(data.fantasy(k), false) }).toMap
    if (data.fullAccum.date.getYear == date.getYear) record.daysSinceLastApp = Days.daysBetween(data.fullAccum.date.withTimeAtStartOfDay(), date.withTimeAtStartOfDay()).getDays
    data.fullAccum.date = date
  }

  def processPlay(play: RetrosheetPlay) = {
    record.outs = record.outs + play.outs
    record.pitches = record.pitches + play.pitches.length
    record.balls = record.balls + play.balls
    if (play.isSingle || play.isDouble || play.isTriple || play.isHomeRun) {
      record.hits = record.hits + 1
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("H", k, v)) })
    }
    if (play.isGroundOut) record.groundOuts = record.groundOuts + 1
    if (play.isFlyOut) record.flyOuts = record.flyOuts + 1
    if (play.isStrikeOut) {
      record.strikeOuts = record.strikeOuts + 1
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("SO", k, v)) })
    }
    if (play.isBaseOnBalls) {
      record.walks = record.walks + 1
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("BB", k, v)) })
    }
    if (play.isHitByPitch) {
      record.hitByPitch = record.hitByPitch + 1
      fantasyScores = fantasyScores.map({ case (k, v) => (k -> updateFantasyScore("HBP", k, v)) })
    }
  }

}
