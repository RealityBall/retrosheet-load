package org.bustos.realityball

import org.joda.time._
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.slick.lifted.AbstractTable
import scala.util.matching.Regex
import scala.io.Source
import scala.collection.parallel._
import java.io.File
import org.slf4j.LoggerFactory
import RealityballRecords._
import RetrosheetHitterDay._
import RetrosheetPitcherDay._
import RealityballConfig._
import FantasyScoreSheet._

object RetrosheetLoad extends App {

  import scala.collection.mutable.MutableList
  import scala.collection.mutable.Queue

  val logger = LoggerFactory.getLogger(getClass)

  val gameIdExpression: Regex = "id,(.*)".r
  val gameInfoExpression: Regex = "info,(.*)".r
  val pitcherExpression: Regex = "start,(.*),(.*),(.*),(.*),1".r
  val lineupExpression: Regex = "start,(.*),(.*),(.*),(.*),(.*)".r
  val pitcherSubExpression: Regex = "sub,(.*),(.*),(.*),(.*),1".r
  val playExpression: Regex = "play,(.*),(.*),(.*),(.*),(.*),(.*)".r
  val playerRoster: Regex = "(.*),(.*),(.*),([BRL]),([BRL]),(.*),(.*)".r
  val teamExpression: Regex = "(.*),(.*),(.*),(.*)".r
  val teamMetaExpression: Regex = "(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)".r
  val erExpression: Regex = "data,er,(.*),(.*)".r

  val tables: Map[String, TableQuery[_ <: slick.driver.MySQLDriver.Table[_]]] = Map(
    ("hitterRawLHstats" -> hitterRawLH), ("hitterRawRHstats" -> hitterRawRH), ("hitterDailyStats" -> hitterStats),
    ("hitterMovingStats" -> hitterMovingStats), ("hitterFantasyStats" -> hitterFantasyTable), ("hitterFantasyMovingStats" -> hitterFantasyMovingTable), ("hitterFantasyVolatilityStats" -> hitterFantasyVolatilityTable),
    ("pitcherFantasyStats" -> pitcherFantasy), ("pitcherFantasyMovingStats" -> pitcherFantasyMoving), ("hitterVolatilityStats" -> hitterVolatilityStats),
    ("games" -> gamesTable), ("ballparkDailies" -> ballparkDailiesTable), ("ballpark" -> ballparkTable), ("gameConditions" -> gameConditionsTable),
    ("gameScoring" -> gameScoringTable), ("teams" -> teamsTable), ("players" -> playersTable), ("pitcherDaily" -> pitcherDailyTable))

  def someOrNone(x: Double): Option[Double] = { if (x.isNaN) None else Some(x) }

  def moveRunners(id: String, play: RetrosheetPlay, runners: List[String]): List[String] = {
    val advancements = play.advancements
    var newRunners = runners
    play.advancements.map(x => {
      if (x._1 != "B") {
        if (!x._2.startsWith("H")) { // Starts with because runs could be unearned "(UR)"
          newRunners = newRunners.updated(x._2.toInt - 1, newRunners(x._1.toInt - 1))
        }
        newRunners = newRunners.updated(x._1.toInt - 1, "        ")
      }
    })
    if (play.resultingPosition > 0) newRunners = newRunners.updated(play.resultingPosition - 1, id)
    //println("          " + runners + " --> " + newRunners + " -- " + play.play + " -- " + play.rbis)
    newRunners
  }

  def maintainDatabase = {
    db.withSession { implicit session =>
      def maintainTable(schema: TableQuery[_ <: slick.driver.MySQLDriver.Table[_]], name: String) = {
        if (!MTable.getTables(name).list.isEmpty) {
          schema.ddl.drop
        }
        schema.ddl.create
      }
      tables.map { case (k, v) => maintainTable(v, k) }
    }
  }

  def processBallparks = {
    logger.info("Updating ballpark codes...")
    val ballparkFile = new File(DataRoot + "retrosheet/parkcode.txt")
    db.withSession { implicit session =>
      Source.fromFile(ballparkFile).getLines.foreach { line =>
        if (!line.startsWith("PARKID")) {
          val data = line.split(",", -1)
          ballparkTable += Ballpark(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8))
        }
      }
    }
  }

  def processTeams(year: String) = {
    logger.info("Teams for " + year)
    val teamFile = new File(DataRoot + "retrosheet/" + year + "eve/TEAM" + year)
    val teamMetaFile = new File(DataRoot + "generatedData/teamMetaData.csv")
    val metaData: Map[String, (String, String, String, String, String, String, String)] = {
      Source.fromFile(teamMetaFile).getLines.map {
        _ match {
          case teamMetaExpression(retrosheetId, site, zipCode, mlbComId, mlbComName, timeZone, coversComId, coversComName) => (retrosheetId -> (site, zipCode, mlbComId, mlbComName, timeZone, coversComId, coversComName))
          case _ => ("" -> ("", "", "", "", "", "", ""))
        }
      }.toMap
    }
    db.withTransaction { implicit session =>
      Source.fromFile(teamFile).getLines.foreach {
        _ match {
          case teamExpression(mnemonic, league, city, name) => teamsTable += Team(year, mnemonic, league, city, name, metaData(mnemonic)._1, metaData(mnemonic)._2, metaData(mnemonic)._3,
            metaData(mnemonic)._4, metaData(mnemonic)._5, metaData(mnemonic)._6, metaData(mnemonic)._7)
          case _ =>
        }
      }
    }
  }

  def playersForYear(year: String): Map[String, Player] = {
    var players = Map.empty[String, Player]
    (new File(DataRoot + "retrosheet/" + year + "eve")).listFiles.filter(x => x.getName.endsWith(".ROS")).map {
      {
        Source.fromFile(_).getLines.foreach {
          _ match {
            case playerRoster(id, lastName, firstName, hits, throwsWith, team, position) => players += (id -> Player(id, year, lastName, firstName, hits, throwsWith, team, position))
            case _ =>
          }
        }
      }
    }
    players
  }

  def dateFromString(dateString: String): DateTime = {
    new DateTime(dateString.substring(0, 4).toInt, dateString.substring(5, 7).toInt, dateString.substring(8, 10).toInt, 0, 0)
  }

  def processYear(year: String) = {
    val eventFiles = (new File(DataRoot + "retrosheet/" + year + "eve")).listFiles.filter(x => x.getName.endsWith(".EVN") || x.getName.endsWith(".EVA"))
    var gamePitchers = Map.empty[String, RetrosheetPitcherDay]
    var games = List.empty[RetrosheetGameInfo]
    var ballparks = Map.empty[String, BallparkDaily]

    def pitcherRecord(id: String, currentGame: RetrosheetGameInfo, side: String): RetrosheetPitcherDay = {
      val currentPitcher = new RetrosheetPitcherDay(id, currentGame.game.id, dateFromString(currentGame.game.date),
        if (side == "0") currentGame.game.homeTeam else currentGame.game.visitingTeam,
        if (currentGame.scoring.wp == id) 1 else 0, if (currentGame.scoring.lp == id) 1 else 0, if (currentGame.scoring.save == id) 1 else 0)
      if (!pitcherSummaries.contains(id)) pitcherSummaries += (id -> List(currentPitcher))
      else pitcherSummaries += (id -> (currentPitcher :: pitcherSummaries(id)))
      gamePitchers += (id -> currentPitcher)
      currentPitcher
    }

    def hitterForDay(game: Game, id: String, lineupPosition: Int, side: Int, currentPitchers: Map[Int, RetrosheetPitcherDay]): RetrosheetHitterDay = {
      val pitcherMatchup = {
        if (side == 1 && currentPitchers.contains(0)) currentPitchers(0).id
        else if (side == 0 && currentPitchers.contains(1)) currentPitchers(1).id
        else ""
      }
      if (!batterSummaries.contains(id)) batterSummaries += (id -> List(new RetrosheetHitterDay(game.date, id, pitcherMatchup, 1, lineupPosition, game.id, side)))
      if (batterSummaries(id).head.date != game.date) {
        batterSummaries += (id -> (new RetrosheetHitterDay(game.date, id, pitcherMatchup, 1, lineupPosition, game.id, side) :: batterSummaries(id)))
      } else if (batterSummaries(id).head.pitcherId != pitcherMatchup) {
        if (batterSummaries(id).head.pitcherId == "") batterSummaries(id).head.pitcherId = pitcherMatchup
        else {
          val batterSummary = batterSummaries(id).head
          batterSummaries += (id -> (new RetrosheetHitterDay(game.date, id, pitcherMatchup, batterSummary.pitcherIndex + 1, batterSummary.lineupPosition, game.id, batterSummary.side) :: batterSummaries(id)))
        }
      }
      batterSummaries(id).head
    }

    processTeams(year)
    val players = playersForYear(year)

    eventFiles.map { f =>
      println("")
      logger.info(f.getName + " - ")
      var currentGame: RetrosheetGameInfo = null
      var currentBallpark: BallparkDaily = null
      var currentPitchers = Map.empty[Int, RetrosheetPitcherDay] // 0: Away, 1: Home
      var currentInning: Int = 1
      var currentOut: Int = 0
      var currentSide: Int = 0
      var runners: List[String] = List("        ", "        ", "        ")
      Source.fromFile(f).getLines.foreach { line =>
        line match {
          case gameIdExpression(id) => {
            currentGame = new RetrosheetGameInfo(id)
            currentBallpark = new BallparkDaily("", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            ballparks += (currentGame.game.id -> currentBallpark)
            games = currentGame :: games
            gamePitchers = gamePitchers.empty
            print(currentGame.game.id + ",")
          }
          case gameInfoExpression(data) => {
            currentGame.processInfoRecord(line)
            ballparks(currentGame.game.id).id = currentGame.game.id
            ballparks(currentGame.game.id).date = currentGame.game.date
          }
          case pitcherExpression(id, name, side, lineupPosition) => {
            // Starting Pitcher
            currentPitchers += (side.toInt -> pitcherRecord(id, currentGame, side))
            if (side == "1") currentGame.game.startingHomePitcher = id
            else currentGame.game.startingVisitingPitcher = id
            if (lineupPosition.toInt > 0) {
              hitterForDay(currentGame.game, id, lineupPosition.toInt, side.toInt, currentPitchers)
            }
          }
          case pitcherSubExpression(id, name, side, lineupPosition) => {
            // Pitcher Substitution
            currentPitchers += (side.toInt -> pitcherRecord(id, currentGame, side))
          }
          case lineupExpression(id, name, side, lineupPosition, position) => {
            hitterForDay(currentGame.game, id, lineupPosition.toInt, side.toInt, currentPitchers)
          }
          case erExpression(id, earnedRuns) => {
            val pitcherRecord = gamePitchers(id)
            pitcherRecord.record.earnedRuns = earnedRuns.toInt
          }
          case playExpression(inning, side, id, count, pitches, playString) => {
            if (inning.toInt != currentInning || side.toInt != currentSide) {
              currentInning = inning.toInt
              currentSide = side.toInt
              runners = List("        ", "        ", "        ")
            }
            val play = new RetrosheetPlay(pitches, playString)
            if (currentSide == 0) {
              currentPitchers(1).processPlay(play)
            } else {
              currentPitchers(0).processPlay(play)
            }
            val facingRighty = (side == "0" && players(currentPitchers(1).id).throwsWith == "R") || (side == "1" && players(currentPitchers(0).id).throwsWith == "R")
            val currentHitterDay = hitterForDay(currentGame.game, id, 0, side.toInt, currentPitchers)
            if (play.runsScored) {
              play.scoringRunners.map({
                case (runner, result) => {
                  if (runner != "B" && batterSummaries.contains(runners(runner.toInt - 1))) batterSummaries(runners(runner.toInt - 1)).head.addRun(facingRighty)
                }
              })
            }
            if (play.isStolenBase) {
              if (play.baseStolen == '2') {
                if (batterSummaries.contains(runners(0))) batterSummaries(runners(0)).head.addStolenBase(facingRighty)
              } else if (play.baseStolen == '3') {
                if (batterSummaries.contains(runners(1))) batterSummaries(runners(1)).head.addStolenBase(facingRighty)
              } else if (play.baseStolen == 'H') {
                if (batterSummaries.contains(runners(2))) batterSummaries(runners(2)).head.addStolenBase(facingRighty)
              }
            }
            runners = moveRunners(id, play, runners)
            currentHitterDay.updateWithPlay(play, facingRighty)
            play.updateBallpark(currentBallpark, facingRighty)
          }
          case _ => {}
        }
      }
    }
    db.withTransaction { implicit session =>
      logger.info("Games for " + year)
      gamesTable ++= games.map(_.game)
      logger.info("Game Conditions for " + year)
      gameConditionsTable ++= games.map(_.conditions)
      logger.info("Game Scoring for " + year)
      gameScoringTable ++= games.map(_.scoring)
      logger.info("Players for " + year)
      playersTable ++= players.values.map({ player => Player(player.id, year, player.lastName, player.firstName, player.batsWith, player.throwsWith, player.team, player.position) })
      logger.info("Ballpark dailies for " + year)
      ballparkDailiesTable ++= ballparks.values
    }
  }

  def emptyRunningHitterData: RunningHitterData = {
    RunningHitterData(Queue.empty[Int], Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], FantasyGamesBatting.keys.map((_ -> Queue.empty[Statistic])).toMap,
      Queue.empty[Statistic], Queue.empty[Statistic], Queue.empty[Statistic], Queue.empty[Statistic])
  }

  def computeStatistics = {
    logger.info("Computing Running Batter Statistics.")
    //val startTime = System.currentTimeMillis
    batterSummaries.values.map { playerHistory =>
      print(".")
      val sortedHistory = playerHistory.sortBy { x => x.date }
      val currentHitterDay = new RetrosheetHitterDay(sortedHistory.head.date, "", "", 0, 0, "", 0)
      val movingAverageData = RunningHitterStatistics(currentHitterDay, emptyRunningHitterData, Queue.empty[DateTime], emptyRunningHitterData, Queue.empty[DateTime], Map.empty[String, Double])
      sortedHistory.foldLeft(movingAverageData)({ case (data, dailyData) => dailyData.accumulate(data); data })
      sortedHistory.foldLeft(CcyymmddSlashDelimFormatter.parseDateTime(sortedHistory.head.date))({ (x, y) =>
        if (y.pitcherIndex == 1 && movingAverageData.fantasyProduction.contains(y.date)) {
          if (movingAverageData.fantasyProduction(y.date) > 2.0) {
            val newDate = CcyymmddSlashDelimFormatter.parseDateTime(y.date)
            y.productionInterval = Days.daysBetween(x.withTimeAtStartOfDay(), newDate.withTimeAtStartOfDay()).getDays
            newDate
          } else x
        } else x
      })
    }
    //println(System.currentTimeMillis - startTime)
    println(".")
    logger.info("Computing Running Pitcher Statistics.")
    pitcherSummaries.values.map { playerHistory =>
      print(".")
      val sortedHistory = playerHistory.sorted(Ordering.by({ x: RetrosheetPitcherDay => CcyymmddFormatter.print(x.date) }))
      val currentPitcherDay = new RetrosheetPitcherDay(sortedHistory.head.id, "", sortedHistory.head.date, "", 0, 0, 0)
      val movingAverageData = RunningPitcherStatistics(currentPitcherDay, FantasyGamesPitching.keys.map((_ -> Queue.empty[Statistic])).toMap, Queue.empty[Int], Queue.empty[Int], Queue.empty[Int])
      sortedHistory.foldLeft(movingAverageData)({ case (data, dailyData) => dailyData.accumulate(data); data })
    }
  }

  def persist = {
    println("")
    var progress: Int = 0
    batterSummaries.values.map { playerHistory =>
      db.withTransaction { implicit session =>

        //val playerHistory = batterSummaries("cabrm001")
        progress = progress + 1
        val sortedHistory = playerHistory.sortBy { x => x.date }
        print(playerHistory.head.id + " [" + progress + "/" + batterSummaries.size + "] " + sortedHistory.length + " ")
        print(".")
        val rawLH = sortedHistory.map({ day =>
          (day.date, day.id, day.gameId, day.side, day.pitcherId, day.pitcherIndex,
            day.LHatBat, day.LHsingle, day.LHdouble, day.LHtriple, day.LHhomeRun, day.LHRBI, day.LHruns,
            day.LHbaseOnBalls, day.LHhitByPitch, day.LHsacFly, day.LHsacHit, day.LHstrikeOut, day.LHflyBall, day.LHgroundBall)
        })
        print(">")
        hitterRawLH ++= rawLH
        print(".")
        val rawRH = sortedHistory.map({ day =>
          (day.date, day.id, day.gameId, day.side, day.pitcherId, day.pitcherIndex,
            day.RHatBat, day.RHsingle, day.RHdouble, day.RHtriple, day.RHhomeRun, day.RHRBI, day.RHruns,
            day.RHbaseOnBalls, day.RHhitByPitch, day.RHsacFly, day.RHsacHit, day.RHstrikeOut, day.RHflyBall, day.RHgroundBall)
        })
        print(">")
        hitterRawRH ++= rawRH
        print(".")
        val hStat = sortedHistory.map({ day =>
          (day.date, day.id, day.gameId, day.side, day.lineupPosition, day.lineupPositionRegime, day.pitcherId, day.pitcherIndex, day.RHatBat + day.LHatBat, day.RHplateAppearance + day.LHplateAppearance,
            someOrNone(day.dailyBattingAverage.rh), someOrNone(day.dailyBattingAverage.lh), someOrNone(day.dailyBattingAverage.total),
            someOrNone(day.battingAverage.rh), someOrNone(day.battingAverage.lh), someOrNone(day.battingAverage.total),
            someOrNone(day.onBasePercentage.rh), someOrNone(day.onBasePercentage.lh), someOrNone(day.onBasePercentage.total),
            someOrNone(day.sluggingPercentage.rh), someOrNone(day.sluggingPercentage.lh), someOrNone(day.sluggingPercentage.total))
        })
        print(">")
        hitterStats ++= hStat
        print(".")
        val movStat = sortedHistory.map({ day =>
          HitterStatsMoving(day.date, day.id, day.pitcherId, day.pitcherIndex,
            someOrNone(day.battingAverageMov.rh), someOrNone(day.battingAverageMov.lh), someOrNone(day.battingAverageMov.total),
            someOrNone(day.onBasePercentageMov.rh), someOrNone(day.onBasePercentageMov.lh), someOrNone(day.onBasePercentageMov.total),
            someOrNone(day.sluggingPercentageMov.rh), someOrNone(day.sluggingPercentageMov.lh), someOrNone(day.sluggingPercentageMov.total),
            day.RHstyle, day.LHstyle, day.style)
        })
        print(">")
        hitterMovingStats ++= movStat
        print(".")
        val volStat = sortedHistory.map({ day =>
          (day.date, day.id, day.pitcherId, day.pitcherIndex,
            someOrNone(day.battingVolatility.rh), someOrNone(day.battingVolatility.lh), someOrNone(day.battingVolatility.total),
            someOrNone(day.onBaseVolatility.rh), someOrNone(day.onBaseVolatility.lh), someOrNone(day.onBaseVolatility.total),
            someOrNone(day.sluggingVolatility.rh), someOrNone(day.sluggingVolatility.lh), someOrNone(day.sluggingVolatility.total))
        })
        print(">")
        hitterVolatilityStats ++= volStat
        print(".")
        val fantasyStat = sortedHistory.map({ day =>
          HitterFantasyDaily(day.date, day.id, day.gameId, day.side, day.pitcherId, day.pitcherIndex, day.productionInterval,
            someOrNone(day.fantasyScores(FanDuelName).rh), someOrNone(day.fantasyScores(FanDuelName).lh), someOrNone(day.fantasyScores(FanDuelName).total),
            someOrNone(day.fantasyScores(DraftKingsName).rh), someOrNone(day.fantasyScores(DraftKingsName).lh), someOrNone(day.fantasyScores(DraftKingsName).total),
            someOrNone(day.fantasyScores(DraftsterName).rh), someOrNone(day.fantasyScores(DraftsterName).lh), someOrNone(day.fantasyScores(DraftsterName).total))
        })
        print(">")
        hitterFantasyTable ++= fantasyStat
        print(".")
        val fantasyMovStat = sortedHistory.map({ day =>
          HitterFantasy(day.date, day.id, day.pitcherId, day.pitcherIndex,
            someOrNone(day.fantasyScoresMov(FanDuelName).rh), someOrNone(day.fantasyScoresMov(FanDuelName).lh), someOrNone(day.fantasyScoresMov(FanDuelName).total),
            someOrNone(day.fantasyScoresMov(DraftKingsName).rh), someOrNone(day.fantasyScoresMov(DraftKingsName).lh), someOrNone(day.fantasyScoresMov(DraftKingsName).total),
            someOrNone(day.fantasyScoresMov(DraftsterName).rh), someOrNone(day.fantasyScoresMov(DraftsterName).lh), someOrNone(day.fantasyScoresMov(DraftsterName).total))
        })
        print(">")
        hitterFantasyMovingTable ++= fantasyMovStat
        print(".")
        val fantasyVolStat = sortedHistory.map({ day =>
          HitterFantasy(day.date, day.id, day.pitcherId, day.pitcherIndex,
            someOrNone(day.fantasyScoresVolatility(FanDuelName).rh), someOrNone(day.fantasyScoresVolatility(FanDuelName).lh), someOrNone(day.fantasyScoresVolatility(FanDuelName).total),
            someOrNone(day.fantasyScoresVolatility(DraftKingsName).rh), someOrNone(day.fantasyScoresVolatility(DraftKingsName).lh), someOrNone(day.fantasyScoresVolatility(DraftKingsName).total),
            someOrNone(day.fantasyScoresVolatility(DraftsterName).rh), someOrNone(day.fantasyScoresVolatility(DraftsterName).lh), someOrNone(day.fantasyScoresVolatility(DraftsterName).total))
        })
        print(">")
        hitterFantasyVolatilityTable ++= fantasyVolStat
        println(".")
      }
    }

    progress = 0
    pitcherSummaries.values.map { playerHistory =>
      db.withTransaction { implicit session =>
        progress = progress + 1
        val sortedHistory = playerHistory.sorted(Ordering.by({ x: RetrosheetPitcherDay => CcyymmddFormatter.print(x.date) }))
        print(playerHistory.head.id + " [" + progress + "/" + pitcherSummaries.size + "] " + sortedHistory.length + " ")
        print(".")
        val pitcherDaily = sortedHistory.map(_.record)
        print(">")
        pitcherDailyTable ++= pitcherDaily
        print(".")
        val fantasyStat = sortedHistory.map({ day =>
          (CcyymmddFormatter.print(day.date), day.id,
            someOrNone(day.fantasyScores(FanDuelName).total), someOrNone(day.fantasyScores(DraftKingsName).total), someOrNone(day.fantasyScores(DraftsterName).total))
        })
        print(">")
        pitcherFantasy ++= fantasyStat
        print(".")
        val fantasyMovStat = sortedHistory.map({ day =>
          (CcyymmddFormatter.print(day.date), day.id,
            someOrNone(day.fantasyScoresMov(FanDuelName).total), someOrNone(day.fantasyScoresMov(DraftKingsName).total), someOrNone(day.fantasyScoresMov(DraftsterName).total))
        })
        println(">")
        pitcherFantasyMoving ++= fantasyMovStat
      }
    }
  }

  val crunchtime = new CrunchtimeBaseballMapping
  crunchtime.processIdMappings
  maintainDatabase
  processBallparks
  var batterSummaries = Map.empty[String, List[RetrosheetHitterDay]]
  var pitcherSummaries = Map.empty[String, List[RetrosheetPitcherDay]]
  (2013 to 2014).map { i => processYear(i.toString) }
  computeStatistics
  persist

}
