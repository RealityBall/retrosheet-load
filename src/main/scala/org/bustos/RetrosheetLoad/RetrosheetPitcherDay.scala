package org.bustos.RetrosheetLoad

import RetrosheetRecords.{ PitcherDaily, Statistic, RunningPitcherStatistics }
import FantasyScoreSheet._

class RetrosheetPitcherDay(val id: String, val date: String, win: Int, loss: Int, save: Int) extends StatisticsTrait {
  
  val record = new PitcherDaily(id, date, win, loss, save, 0, 0, 0, 0, 0, 0, false, false, 0, 0) 

  var fantasyScores = FantasyGamesPitching.keys.map(_ -> Statistic(0.0, 0.0, 0.0)).toMap
  var fantasyScoresMov = FantasyGamesPitching.keys.map(_ -> Statistic(0.0, 0.0, 0.0)).toMap
  
  def updateFantasyScore(playOutcome: String, gameName: String, track: Statistic): Statistic = {
    track.total = track.total + FantasyGamesPitching(gameName)(playOutcome)
    track
  }  

  def accumulate(data: RunningPitcherStatistics) = {
    data.fullAccum.fantasyScores = fantasyScores
    fantasyScores.map({case (k, v) => data.fantasy(k).enqueue(fantasyScores(k))})
    if (data.fantasy.size > 25) {
      data.fantasy.mapValues(_.dequeue)
    }      
    fantasyScoresMov = fantasyScoresMov.map({case (k, v) => k -> movingAverageSimple(data.fantasy(k))}).toMap
  }

  def processPlay(play: RetrosheetPlay) = {
    record.outs = record.outs + play.outs  
    record.pitches = record.pitches + play.pitches.length
    record.balls = record.balls + play.balls
    if (play.isSingle || play.isDouble || play.isTriple || play.isHomeRun) {
      record.hits = record.hits + 1
      fantasyScores = fantasyScores.map({case (k, v) => (k -> updateFantasyScore("H", k, v))})
    }
    if (play.isStrikeOut) {
      record.strikeOuts = record.strikeOuts + 1
      fantasyScores = fantasyScores.map({case (k, v) => (k -> updateFantasyScore("SO", k, v))})
    }
    if (play.isBaseOnBalls) {
      record.walks = record.walks + 1
      fantasyScores = fantasyScores.map({case (k, v) => (k -> updateFantasyScore("BB", k, v))})
    }
    if (play.isHitByPitch) {
      record.hitByPitch = record.hitByPitch + 1
      fantasyScores = fantasyScores.map({case (k, v) => (k -> updateFantasyScore("HBP", k, v))})
    }
  }
  
}