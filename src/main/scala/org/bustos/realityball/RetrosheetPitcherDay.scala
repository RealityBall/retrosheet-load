package org.bustos.realityball

import org.joda.time._
import FantasyScoreSheet._
import RealityballRecords.{ PitcherDaily, Statistic }
import RealityballConfig._

object RetrosheetPitcherDay {

  import scala.collection.mutable.Queue

  case class RunningPitcherStatistics(fullAccum: RetrosheetPitcherDay, fantasy: Map[String, Queue[Statistic]])
}

class RetrosheetPitcherDay(val id: String, val game: String, var date: DateTime, opposing: String, win: Int, loss: Int, save: Int) extends StatisticsTrait {

  import RetrosheetPitcherDay._

  val record = new PitcherDaily(id, game, CcyymmddFormatter.print(date), 0, opposing, win, loss, save, 0, 0, 0, 0, 0, 0, 0, 0, false, false, 0, 0)

  var fantasyScores = FantasyGamesPitching.keys.map(_ -> Statistic(0.0, 0.0, 0.0)).toMap
  var fantasyScoresMov = FantasyGamesPitching.keys.map(_ -> Statistic(0.0, 0.0, 0.0)).toMap

  def updateFantasyScore(playOutcome: String, gameName: String, track: Statistic): Statistic = {
    track.total = track.total + FantasyGamesPitching(gameName)(playOutcome)
    track
  }

  def accumulate(data: RunningPitcherStatistics) = {
    data.fullAccum.fantasyScores = fantasyScores
    fantasyScores.map({ case (k, v) => data.fantasy(k).enqueue(fantasyScores(k)) })
    if (data.fantasy.size > 25) {
      data.fantasy.mapValues(_.dequeue)
    }
    fantasyScoresMov = fantasyScoresMov.map({ case (k, v) => k -> queueMeanSimple(data.fantasy(k)) }).toMap
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
