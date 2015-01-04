package org.bustos.RetrosheetLoad

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.slick.lifted.AbstractTable
import scala.util.Properties.envOrNone
import java.io.File
import scala.io.Source
import scala.util.matching.Regex
import scala.collection.mutable.Queue
import RetrosheetRecords._
import FantasyScoreSheet._

object RetrosheetLoad extends App {

  import scala.collection.mutable.MutableList
  
  val gameIdExpression: Regex = "id,(.*)".r
  val gameInfoExpression: Regex = "info,(.*)".r
  val pitcherHomeExpression: Regex = "start,(.*),(.*),1,(.*),1".r
  val pitcherAwayExpression: Regex = "start,(.*),(.*),0,(.*),1".r
  val pitcherSubHomeExpression: Regex = "sub,(.*),(.*),1,(.*),1".r
  val pitcherSubAwayExpression: Regex = "sub,(.*),(.*),0,(.*),1".r
  val playExpression: Regex = "play,(.*),(.*),(.*),(.*),(.*),(.*)".r
  val playerRoster: Regex = "(.*),(.*),(.*),([BRL]),([BRL]),(.*),(.*)".r
  val teamExpression: Regex = "(.*),(.*),(.*),(.*)".r
  val erExpression: Regex = "data,er,(.*),(.*)".r

  val mysqlURL = envOrNone("MLB_MYSQL_URL").get
  val mysqlUser = envOrNone("MLB_MYSQL_USER").get
  val mysqlPassword = envOrNone("MLB_MYSQL_PASSWORD").get
  val db = Database.forURL("jdbc:mysql://localhost:3306/mlbretrosheet", driver="com.mysql.jdbc.Driver", user="root", password="")
  //val db = Database.forURL(mysqlURL, driver="com.mysql.jdbc.Driver", user=mysqlUser, password=mysqlPassword)
  
  var tables = Map.empty[String, TableQuery[_ <: slick.driver.MySQLDriver.Table[_]]]
  val hitterRawLH: TableQuery[HitterRawLHStatsTable] = TableQuery[HitterRawLHStatsTable]
  tables += ("hitterRawLHstats" -> hitterRawLH)
  val hitterRawRH: TableQuery[HitterRawRHStatsTable] = TableQuery[HitterRawRHStatsTable]
  tables += ("hitterRawRHstats" -> hitterRawRH)
  val hitterStats: TableQuery[HitterDailyStatsTable] = TableQuery[HitterDailyStatsTable]
  tables += ("hitterDailyStats" -> hitterStats)
  val hitterMovingStats: TableQuery[HitterStatsMovingTable] = TableQuery[HitterStatsMovingTable]
  tables += ("hitterMovingStats" -> hitterMovingStats)
  val hitterFantasy: TableQuery[HitterFantasyTable] = TableQuery[HitterFantasyTable]
  tables += ("hitterFantasyStats" -> hitterFantasy)
  val hitterFantasyMoving: TableQuery[HitterFantasyMovingTable] = TableQuery[HitterFantasyMovingTable]
  tables += ("hitterFantasyMovingStats" -> hitterFantasyMoving)
  val hitterVolatilityStats: TableQuery[HitterStatsVolatilityTable] = TableQuery[HitterStatsVolatilityTable]
  tables += ("hitterVolatilityStats" -> hitterVolatilityStats)
  val gamesTable: TableQuery[GamesTable] = TableQuery[GamesTable]
  tables += ("games" -> gamesTable)
  val gameConditionsTable: TableQuery[GameConditionsTable] = TableQuery[GameConditionsTable]
  tables += ("gameConditions" -> gameConditionsTable)
  val gameScoringTable: TableQuery[GameScoringTable] = TableQuery[GameScoringTable]
  tables += ("gameScoring" -> gameScoringTable)
  val teamsTable: TableQuery[TeamsTable] = TableQuery[TeamsTable]
  tables += ("teams" -> teamsTable)
  val playersTable: TableQuery[PlayersTable] = TableQuery[PlayersTable]
  tables += ("players" -> playersTable)  
  val pitcherDailyTable: TableQuery[PitcherDailyTable] = TableQuery[PitcherDailyTable]
  tables += ("pitcherDaily" -> pitcherDailyTable)  
  
  def someOrNone(x: Double): Option[Double] = {if (x.isNaN) None else Some(x)}
      
  def moveRunners(id: String, play: RetrosheetPlay, runners: List[String]): List[String] = {
    val advancements = play.advancements
    var newRunners = runners
    play.advancements.map(x => {
      if (x._1 != "B") {
        if (!x._2.startsWith("H")) { // Starts with because runs could be unearned "(UR)"
          newRunners = newRunners.updated (x._2.toInt - 1, newRunners(x._1.toInt - 1))
        }
        newRunners = newRunners.updated (x._1.toInt - 1, "        ")
      }
    })
    if (play.resultingPosition > 0) newRunners = newRunners.updated (play.resultingPosition - 1, id)
    //println("          " + runners + " --> " + newRunners + " -- " + play.play + " -- " + play.rbis)
    newRunners
  }

  def computeRunningPlayerStats (data: RunningStatistics, dailyData: RetrosheetHitterDay): RunningStatistics = {
    dailyData.accumulate(data)
    data
  }
  
  def maintainDatabase = {
    db.withSession { implicit session =>
      def maintainTable(schema: TableQuery[_ <: slick.driver.MySQLDriver.Table[_]], name: String) = {
        if (!MTable.getTables(name).list.isEmpty) {
          schema.ddl.drop
        }
        schema.ddl.create
      }
      tables.map {case (k, v) => maintainTable(v, k)}
    }
  }
  
  def processPitches(pitcher: PitcherDaily, play: RetrosheetPlay) = {
    pitcher.pitches = pitcher.pitches + play.pitches.length
    pitcher.balls = pitcher.balls + play.balls
    if (play.isSingle || play.isDouble || play.isTriple || play.isHomeRun) pitcher.hits = pitcher.hits + 1
    if (play.isStrikeOut) pitcher.strikeOuts = pitcher.strikeOuts + 1
    if (play.isBaseOnBalls) pitcher.walks = pitcher.walks + 1
    if (play.isHitByPitch) pitcher.hitByPitch = pitcher.hitByPitch + 1
  }
  
  maintainDatabase
  for (i <- 2010 to 2014) processYear(i.toString)
  
  def processYear(year: String) = {    
    val eventFiles = (new File("/Users/mauricio/Google Drive/Projects/fantasySports/retrosheetData/" + year + "eve")).listFiles.filter(x => x.getName.endsWith(".EVN") || x.getName.endsWith(".EVA"))
    var daySummaries = Map.empty[String, List[RetrosheetHitterDay]]
  
    val teamFile = new File("/Users/mauricio/Google Drive/Projects/fantasySports/retrosheetData/" + year + "eve/TEAM" + year)
    var teams = Map.empty[String, Team]
    var games = List.empty[RetrosheetGameInfo]
  
    for (line <- Source.fromFile(teamFile).getLines) {
      line match {
        case teamExpression(mnemonic, league, city, name) => teams += (mnemonic -> Team(year, mnemonic, league, city, name))
        case _ =>
      }
    }
  
    val rosterFiles = (new File("/Users/mauricio/Google Drive/Projects/fantasySports/retrosheetData/" + year + "eve")).listFiles.filter(x => x.getName.endsWith(".ROS"))
    var players = Map.empty[String, Player]
    var pitchers = MutableList.empty[PitcherDaily]
    var gamePitchers = Map.empty[String, PitcherDaily]
  
    for (f <- rosterFiles) {
      for (line <- Source.fromFile(f).getLines) {
        line match {
          case playerRoster(id, lastName, firstName, hits, throwsWith, team, position) => players += (id -> Player(id, year, lastName, firstName, hits, throwsWith, team, position))
          case _ =>
        }
      }
    }
  
    for (f <- eventFiles) {
      println("")
      println(f.getName + " - ")
      var currentGame: RetrosheetGameInfo = null
      var currentHomePitcher: PitcherDaily = null
      var currentAwayPitcher: PitcherDaily = null
      var currentInning: Int = 1
      var currentOut: Int = 0
      var currentSide: Int = 0
      var runners: List[String] = List("        ", "        ", "        ")
      for (line <- Source.fromFile(f).getLines) {
        line match {
          case gameIdExpression(id) => {
            currentGame = new RetrosheetGameInfo(id)
            games = currentGame :: games
            gamePitchers = gamePitchers.empty
            print(currentGame.game.id + ",")
          }
          case gameInfoExpression(data) => {
            currentGame.processInfoRecord(line)
          }
          case pitcherHomeExpression(id, name, hitting)    => {
            currentHomePitcher = PitcherDaily(id, currentGame.game.date, currentGame.scoring.wp == id, currentGame.scoring.lp == id, currentGame.scoring.save == id, 0, 0, 0, 0, 0, 0, false, false, 0, 0)
            pitchers += currentHomePitcher
            gamePitchers += (id -> currentHomePitcher)
          }
          case pitcherAwayExpression(id, name, hitting)    => {
            currentAwayPitcher = PitcherDaily(id, currentGame.game.date, currentGame.scoring.wp == id, currentGame.scoring.lp == id, currentGame.scoring.save == id, 0, 0, 0, 0, 0, 0, false, false, 0, 0)
            pitchers += currentAwayPitcher
            gamePitchers += (id -> currentAwayPitcher)
          }
          case pitcherSubHomeExpression(id, name, hitting) => {
            currentHomePitcher = PitcherDaily(id, currentGame.game.date, currentGame.scoring.wp == id, currentGame.scoring.lp == id, currentGame.scoring.save == id, 0, 0, 0, 0, 0, 0, false, false, 0, 0)
            pitchers += currentHomePitcher
            gamePitchers += (id -> currentHomePitcher)
          }
          case pitcherSubAwayExpression(id, name, hitting) => {
            currentAwayPitcher = PitcherDaily(id, currentGame.game.date, currentGame.scoring.wp == id, currentGame.scoring.lp == id, currentGame.scoring.save == id, 0, 0, 0, 0, 0, 0, false, false, 0, 0)
            pitchers += currentAwayPitcher
            gamePitchers += (id -> currentAwayPitcher)
          }
          case erExpression(id, earnedRuns) => {
            val pitcherRecord = gamePitchers(id)
            pitcherRecord.earnedRuns = earnedRuns.toInt
          }
          case playExpression(inning, side, id, count, pitches, playString) => {
            if (inning.toInt != currentInning || side.toInt != currentSide) {
              currentInning = inning.toInt
              currentSide = side.toInt
              runners = List("        ", "        ", "        ")
            }
            //if (playString.contains(".")) print("inning: " + inning + " ")
            val play = new RetrosheetPlay(pitches, playString)
            if (currentSide == 0) currentHomePitcher.outs = currentHomePitcher.outs + play.outs
            else currentAwayPitcher.outs = currentAwayPitcher.outs + play.outs
            if (!daySummaries.contains(id)) {
              val newHitterDay = new RetrosheetHitterDay(currentGame.game.date, id)
              daySummaries += (id -> List(newHitterDay))
            }
            var currentHitterDay = daySummaries(id).head
            if (currentHitterDay.date != currentGame.game.date) {
              currentHitterDay = new RetrosheetHitterDay(currentGame.game.date, id)
              daySummaries += (id -> (currentHitterDay :: daySummaries(id)))
            }
            val facingRighty = (side == "0" && players(currentHomePitcher.id).throwsWith == "R") || (side == "1" && players(currentAwayPitcher.id).throwsWith == "R")
            if (side == "0") processPitches(currentHomePitcher, play)
            else processPitches(currentAwayPitcher, play)
            if (play.isStolenBase) {
              if (play.baseStolen == '2') {
                if (daySummaries.contains(runners(0))) daySummaries(runners(0)).last.addStolenBase(play, facingRighty)  
              }
              else if (play.baseStolen == '3') {
                if (daySummaries.contains(runners(1))) daySummaries(runners(1)).last.addStolenBase(play, facingRighty)
              }
              else if (play.baseStolen == 'H') {
                if (daySummaries.contains(runners(2))) daySummaries(runners(2)).last.addStolenBase(play, facingRighty)
              }            
            }
            runners = moveRunners(id, play, runners)
            currentHitterDay.updateWithPlay(play, facingRighty)
          }
          case _ => {}
        }
      }
    }
  
    println("")
    println("Computing Running Statistics.")
    for (playerHistory <- daySummaries.values) {
      print(".")
      val sortedHistory = playerHistory.sortBy { x => x.date }
      val currentHitterDay = new RetrosheetHitterDay("", "")
      val movingAverageData = RunningStatistics(currentHitterDay, 
                                RunningData(Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], FantasyGames.keys.map((_ -> Queue.empty[Statistic])).toMap), 
                                RunningVolatilityData(Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], FantasyGames.keys.map((_ -> Queue.empty[Statistic])).toMap ))
      val finalStatistcs = sortedHistory.foldLeft(movingAverageData)(computeRunningPlayerStats)
    }
    println("")
    
    db.withSession { implicit session =>
      println("Games for " + year)
      gamesTable ++= games.map(_.game)
      println("Game Conditions for " + year)
      gameConditionsTable ++= games.map(_.conditions)
      println("Game Scoring for " + year)
      gameScoringTable ++= games.map(_.scoring)
      println("Teams for " + year)
      teamsTable ++= teams.values.map({team => Team(team.year, team.mnemonic, team.league, team.city, team.name)})
      println("Players for " + year)
      playersTable ++= players.values.map({player => Player(player.id, year, player.lastName, player.firstName, player.batsWith, player.throwsWith, player.team, player.position)})
      println("Pitcher dailies for " + year)
      pitcherDailyTable ++= pitchers
  
      var progress: Int = 0
      for (playerHistory <- daySummaries.values) {
        //val playerHistory = daySummaries("cabrm001")
        progress = progress + 1
        val sortedHistory = playerHistory.sortBy { x => x.date }
        print(playerHistory.head.id + " [" + progress + "/" + daySummaries.size + "] " + sortedHistory.length + " ")
        print(".")
        val rawLH = sortedHistory.map({day => (day.date, day.id,
          day.LHatBat, day.LHsingle, day.LHdouble, day.LHtriple, day.LHhomeRun, day.LHRBI,
          day.LHbaseOnBalls, day.LHhitByPitch, day.LHsacFly, day.LHsacHit)})
        print(">")
        hitterRawLH ++= rawLH
        print(".")
        val rawRH = sortedHistory.map({day => (day.date, day.id,
          day.RHatBat, day.RHsingle, day.RHdouble, day.RHtriple, day.RHhomeRun, day.RHRBI,
          day.RHbaseOnBalls, day.RHhitByPitch, day.RHsacFly, day.RHsacHit)})
        print(">")
        hitterRawRH ++= rawRH
        print(".")
        val hStat = sortedHistory.map ({day => (day.date, day.id,
          someOrNone(day.dailyBattingAverage.rh), someOrNone(day.dailyBattingAverage.lh), someOrNone(day.dailyBattingAverage.total),
          someOrNone(day.battingAverage.rh), someOrNone(day.battingAverage.lh), someOrNone(day.battingAverage.total),
          someOrNone(day.onBasePercentage.rh), someOrNone(day.onBasePercentage.lh), someOrNone(day.onBasePercentage.total),
          someOrNone(day.sluggingPercentage.rh), someOrNone(day.sluggingPercentage.lh), someOrNone(day.sluggingPercentage.total))})
        print(">")
        hitterStats ++= hStat
        print(".")
        val movStat = sortedHistory.map({day => (day.date, day.id, 
          someOrNone(day.battingAverage25.rh), someOrNone(day.battingAverage25.lh), someOrNone(day.battingAverage25.total),  
          someOrNone(day.onBasePercentage25.rh), someOrNone(day.onBasePercentage25.lh), someOrNone(day.onBasePercentage25.total), 
          someOrNone(day.sluggingPercentage25.rh), someOrNone(day.sluggingPercentage25.lh), someOrNone(day.sluggingPercentage25.total))})
        print(">")
        hitterMovingStats ++= movStat
        print(".")
        val volStat = sortedHistory.map({day => (day.date, day.id, 
          someOrNone(day.battingVolatility100.rh), someOrNone(day.battingVolatility100.lh), someOrNone(day.battingVolatility100.total),  
          someOrNone(day.onBaseVolatility100.rh), someOrNone(day.onBaseVolatility100.lh), someOrNone(day.onBaseVolatility100.total),
          someOrNone(day.sluggingVolatility100.rh), someOrNone(day.sluggingVolatility100.lh), someOrNone(day.sluggingVolatility100.total))})
        print(">")
        hitterVolatilityStats ++= volStat
        print(".")
        val fantasyStat = sortedHistory.map({day => (day.date, day.id,
           someOrNone(day.fantasyScores(FanDuelName).rh), someOrNone(day.fantasyScores(FanDuelName).lh), someOrNone(day.fantasyScores(FanDuelName).total),
           someOrNone(day.fantasyScores(DraftKingsName).rh), someOrNone(day.fantasyScores(DraftKingsName).lh), someOrNone(day.fantasyScores(DraftKingsName).total),
           someOrNone(day.fantasyScores(DraftsterName).rh), someOrNone(day.fantasyScores(DraftsterName).lh), someOrNone(day.fantasyScores(DraftsterName).total))})
        print(">")
        hitterFantasy++= fantasyStat
        print(".")
        val fantasyMovStat = sortedHistory.map({day => (day.date, day.id,
           someOrNone(day.fantasyScores25(FanDuelName).rh), someOrNone(day.fantasyScores25(FanDuelName).lh), someOrNone(day.fantasyScores25(FanDuelName).total),
           someOrNone(day.fantasyScores25(DraftKingsName).rh), someOrNone(day.fantasyScores25(DraftKingsName).lh), someOrNone(day.fantasyScores25(DraftKingsName).total),
           someOrNone(day.fantasyScores25(DraftsterName).rh), someOrNone(day.fantasyScores25(DraftsterName).lh), someOrNone(day.fantasyScores25(DraftsterName).total))})
        print(">")
        hitterFantasyMoving ++= fantasyMovStat
        println(".")
      }
    }
  }
}
