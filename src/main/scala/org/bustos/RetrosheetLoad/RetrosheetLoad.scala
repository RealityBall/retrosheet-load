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
  val pitcherExpression: Regex = "start,(.*),(.*),(.*),(.*),1".r
  val lineupExpression: Regex = "start,(.*),(.*),(.*),(.*),(.*)".r
  val pitcherSubExpression: Regex = "sub,(.*),(.*),(.*),(.*),1".r
  val playExpression: Regex = "play,(.*),(.*),(.*),(.*),(.*),(.*)".r
  val playerRoster: Regex = "(.*),(.*),(.*),([BRL]),([BRL]),(.*),(.*)".r
  val teamExpression: Regex = "(.*),(.*),(.*),(.*)".r
  val erExpression: Regex = "data,er,(.*),(.*)".r
  val ballparkExpression: Regex = "(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)".r
  val mysqlURL = envOrNone("MLB_MYSQL_URL").get
  val mysqlUser = envOrNone("MLB_MYSQL_USER").get
  val mysqlPassword = envOrNone("MLB_MYSQL_PASSWORD").get
  val db = Database.forURL("jdbc:mysql://localhost:3306/mlbretrosheet", driver="com.mysql.jdbc.Driver", user="root", password="")
  val DataRoot = "/Users/mauricio/Google Drive/Projects/fantasySports/retrosheetData/"
  //val db = Database.forURL(mysqlURL, driver="com.mysql.jdbc.Driver", user=mysqlUser, password=mysqlPassword)
  
  val hitterRawLH: TableQuery[HitterRawLHStatsTable] = TableQuery[HitterRawLHStatsTable]
  val hitterRawRH: TableQuery[HitterRawRHStatsTable] = TableQuery[HitterRawRHStatsTable]
  val hitterStats: TableQuery[HitterDailyStatsTable] = TableQuery[HitterDailyStatsTable]
  val hitterMovingStats: TableQuery[HitterStatsMovingTable] = TableQuery[HitterStatsMovingTable]
  val hitterFantasy: TableQuery[HitterFantasyTable] = TableQuery[HitterFantasyTable]
  val hitterFantasyMoving: TableQuery[HitterFantasyMovingTable] = TableQuery[HitterFantasyMovingTable]
  val pitcherFantasy: TableQuery[PitcherFantasyTable] = TableQuery[PitcherFantasyTable]
  val pitcherFantasyMoving: TableQuery[PitcherFantasyMovingTable] = TableQuery[PitcherFantasyMovingTable]
  val hitterVolatilityStats: TableQuery[HitterStatsVolatilityTable] = TableQuery[HitterStatsVolatilityTable]
  val gamesTable: TableQuery[GamesTable] = TableQuery[GamesTable]
  val ballparkDailiesTable: TableQuery[BallparkDailiesTable] = TableQuery[BallparkDailiesTable]
  val ballparkTable: TableQuery[BallparkTable] = TableQuery[BallparkTable]
  val gameConditionsTable: TableQuery[GameConditionsTable] = TableQuery[GameConditionsTable]
  val gameScoringTable: TableQuery[GameScoringTable] = TableQuery[GameScoringTable]
  val teamsTable: TableQuery[TeamsTable] = TableQuery[TeamsTable]
  val playersTable: TableQuery[PlayersTable] = TableQuery[PlayersTable]
  val pitcherDailyTable: TableQuery[PitcherDailyTable] = TableQuery[PitcherDailyTable]
  
  val tables: Map[String, TableQuery[_ <: slick.driver.MySQLDriver.Table[_]]] = Map (
      ("hitterRawLHstats" -> hitterRawLH), ("hitterRawRHstats" -> hitterRawRH), ("hitterDailyStats" -> hitterStats),
      ("hitterMovingStats" -> hitterMovingStats), ("hitterFantasyStats" -> hitterFantasy), ("hitterFantasyMovingStats" -> hitterFantasyMoving),
      ("pitcherFantasyStats" -> pitcherFantasy), ("pitcherFantasyMovingStats" -> pitcherFantasyMoving), ("hitterVolatilityStats" -> hitterVolatilityStats) ,
      ("games" -> gamesTable), ("ballparkDailies" -> ballparkDailiesTable), ("ballpark" -> ballparkTable), ("gameConditions" -> gameConditionsTable),
      ("gameScoring" -> gameScoringTable), ("teams" -> teamsTable), ("players" -> playersTable), ("pitcherDaily" -> pitcherDailyTable))  
  
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
  
  def processBallparks = {
    println("Updating ballpark codes...")
    val ballparkFile = new File(DataRoot + "parkcode.txt")
    db.withSession { implicit session =>
      Source.fromFile(ballparkFile).getLines.foreach { line => 
        if (!line.startsWith("PARKID")) {
          val data = line.split(',')
          ballparkTable += Ballpark(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8))
        }
      }
    }    
  }

  def processTeams(year: String) = {
    println("Teams for " + year)
    val teamFile = new File(DataRoot + year + "eve/TEAM" + year)
    db.withTransaction { implicit session =>
      Source.fromFile(teamFile).getLines.foreach {
        _ match {
          case teamExpression(mnemonic, league, city, name) => teamsTable += Team(year, mnemonic, league, city, name)
          case _ =>
        }
      }
    }
  }
  
  def playersForYear(year: String): Map[String, Player] = {
    var players = Map.empty[String, Player]
    (new File(DataRoot + year + "eve")).listFiles.filter(x => x.getName.endsWith(".ROS")).map {
        {Source.fromFile(_).getLines.foreach {
          _ match {
            case playerRoster(id, lastName, firstName, hits, throwsWith, team, position) => players += (id -> Player(id, year, lastName, firstName, hits, throwsWith, team, position))
            case _ =>
          }
        }
      }
    }
    players
  }

  def processYear(year: String) = {    
    val eventFiles = (new File(DataRoot + year + "eve")).listFiles.filter(x => x.getName.endsWith(".EVN") || x.getName.endsWith(".EVA"))
    var gamePitchers = Map.empty[String, RetrosheetPitcherDay]
    var games = List.empty[RetrosheetGameInfo]
    var ballparks = Map.empty[String, BallparkDaily]
  
    def pitcherRecord(id: String, currentGame: RetrosheetGameInfo): RetrosheetPitcherDay = {
      val currentPitcher = new RetrosheetPitcherDay(id, currentGame.game.date, if (currentGame.scoring.wp == id) 1 else 0, if (currentGame.scoring.lp == id) 1 else 0, if (currentGame.scoring.save == id) 1 else 0)
      if (!pitcherSummaries.contains(id)) pitcherSummaries += (id -> List(currentPitcher))
      else pitcherSummaries += (id -> (currentPitcher :: pitcherSummaries(id)))
      gamePitchers += (id -> currentPitcher)
      currentPitcher
    }
    
    def hitterForDay(game: Game, id: String, lineupPosition: Int): RetrosheetHitterDay = {
      if (!batterSummaries.contains(id)) batterSummaries += (id -> List(new RetrosheetHitterDay(game.date, id, lineupPosition))) 
      if (batterSummaries(id).head.date != game.date) {
        batterSummaries += (id -> (new RetrosheetHitterDay(game.date, id, lineupPosition) :: batterSummaries(id)))
      }
      batterSummaries(id).head
    }
  
    processTeams(year)
    val players = playersForYear(year)
  
    eventFiles.map {f =>
      println("")
      println(f.getName + " - ")
      var currentGame: RetrosheetGameInfo = null
      var currentBallpark: BallparkDaily = null
      var currentPitchers = Map.empty[String, RetrosheetPitcherDay] // 0: Away, 1: Home
      var currentInning: Int = 1
      var currentOut: Int = 0
      var currentSide: Int = 0
      var runners: List[String] = List("        ", "        ", "        ")
      Source.fromFile(f).getLines.foreach {line =>
        line match {
          case gameIdExpression(id) => {
            currentGame = new RetrosheetGameInfo(id)
            currentBallpark = new BallparkDaily("", "", 0, 0, 0, 0, 0, 0)
            ballparks += (currentGame.game.id -> currentBallpark)
            games = currentGame :: games
            gamePitchers = gamePitchers.empty
            print(currentGame.game.id + ",")
          }
          case gameInfoExpression(data) => {
            currentGame.processInfoRecord(line)
            ballparks(currentGame.game.id).id = currentGame.game.site
            ballparks(currentGame.game.id).date = currentGame.game.date
          }
          case pitcherExpression(id, name, side, lineupPosition) => {
            currentPitchers += (side -> pitcherRecord(id, currentGame))
          }
          case pitcherSubExpression(id, name, side, lineupPosition) => {
            currentPitchers += (side -> pitcherRecord(id, currentGame))
          }
          case lineupExpression(id, name, side, lineupPosition, position) => {
            val currentHitterDay = hitterForDay(currentGame.game, id, lineupPosition.toInt)
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
            //if (playString.contains(".")) print("inning: " + inning + " ")
            val play = new RetrosheetPlay(pitches, playString)
            if (currentSide == 0) {
              currentPitchers("1").processPlay(play)  
            } else {
              currentPitchers("0").processPlay(play)  
            }
            val facingRighty = (side == "0" && players(currentPitchers("1").id).throwsWith == "R") || (side == "1" && players(currentPitchers("0").id).throwsWith == "R")
            val currentHitterDay = hitterForDay(currentGame.game, id, 0)
            if (play.isStolenBase) {
              if (play.baseStolen == '2') {
                if (batterSummaries.contains(runners(0))) batterSummaries(runners(0)).head.addStolenBase(play, facingRighty)  
              }
              else if (play.baseStolen == '3') {
                if (batterSummaries.contains(runners(1))) batterSummaries(runners(1)).head.addStolenBase(play, facingRighty)
              }
              else if (play.baseStolen == 'H') {
                if (batterSummaries.contains(runners(2))) batterSummaries(runners(2)).head.addStolenBase(play, facingRighty)
              }            
            }
            runners = moveRunners(id, play, runners)
            currentHitterDay.updateWithPlay(play, facingRighty)
            currentHitterDay.updateBallpark(currentBallpark)
          }
          case _ => {}
        }
      }
    }
    db.withTransaction { implicit session =>
      println("Games for " + year)
      gamesTable ++= games.map(_.game)
      println("Game Conditions for " + year)
      gameConditionsTable ++= games.map(_.conditions)
      println("Game Scoring for " + year)
      gameScoringTable ++= games.map(_.scoring)
      println("Players for " + year)
      playersTable ++= players.values.map({player => Player(player.id, year, player.lastName, player.firstName, player.batsWith, player.throwsWith, player.team, player.position)})
      println("Ballpark dailies for " + year)
      ballparkDailiesTable ++= ballparks.values
    }
  }
  
  def computeStatistics = {
    println("")
    println("Computing Running Batter Statistics.")
    batterSummaries.values.map {playerHistory =>
      print(".")
      val sortedHistory = playerHistory.sortBy { x => x.date }
      val currentHitterDay = new RetrosheetHitterDay(sortedHistory.head.date, "", 0)
      val movingAverageData = RunningHitterStatistics(currentHitterDay, 
                                RunningHitterData(Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], FantasyGamesBatting.keys.map((_ -> Queue.empty[Statistic])).toMap), 
                                RunningHitterData(Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], FantasyGamesBatting.keys.map((_ -> Queue.empty[Statistic])).toMap ))
      sortedHistory.foldLeft(movingAverageData)({case (data, dailyData) => dailyData.accumulate(data); data})
    }
    println("")
    println("Computing Running Pitcher Statistics.")
    pitcherSummaries.values.map {playerHistory =>
      print(".")
      val sortedHistory = playerHistory.sortBy { x => x.date }
      val currentPitcherDay = new RetrosheetPitcherDay(sortedHistory.head.date, "", 0, 0, 0)
      val movingAverageData = RunningPitcherStatistics(currentPitcherDay, FantasyGamesPitching.keys.map((_ -> Queue.empty[Statistic])).toMap )
      sortedHistory.foldLeft(movingAverageData)({case (data, dailyData) => dailyData.accumulate(data); data})
    }
  }
  
  def persist = {
    println("")
    var progress: Int = 0
    batterSummaries.values.map {playerHistory =>
      db.withTransaction { implicit session =>

        //val playerHistory = batterSummaries("cabrm001")
        progress = progress + 1
        val sortedHistory = playerHistory.sortBy { x => x.date }
        print(playerHistory.head.id + " [" + progress + "/" + batterSummaries.size + "] " + sortedHistory.length + " ")
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
        val hStat = sortedHistory.map ({day => (day.date, day.id, day.lineupPosition, day.RHatBat + day.LHatBat, day.RHplateAppearance + day.LHplateAppearance,
          someOrNone(day.dailyBattingAverage.rh), someOrNone(day.dailyBattingAverage.lh), someOrNone(day.dailyBattingAverage.total),
          someOrNone(day.battingAverage.rh), someOrNone(day.battingAverage.lh), someOrNone(day.battingAverage.total),
          someOrNone(day.onBasePercentage.rh), someOrNone(day.onBasePercentage.lh), someOrNone(day.onBasePercentage.total),
          someOrNone(day.sluggingPercentage.rh), someOrNone(day.sluggingPercentage.lh), someOrNone(day.sluggingPercentage.total))})
        print(">")
        hitterStats ++= hStat
        print(".")
        val movStat = sortedHistory.map({day => (day.date, day.id, 
          someOrNone(day.battingAverageMov.rh), someOrNone(day.battingAverageMov.lh), someOrNone(day.battingAverageMov.total),  
          someOrNone(day.onBasePercentageMov.rh), someOrNone(day.onBasePercentageMov.lh), someOrNone(day.onBasePercentageMov.total), 
          someOrNone(day.sluggingPercentageMov.rh), someOrNone(day.sluggingPercentageMov.lh), someOrNone(day.sluggingPercentageMov.total))})
        print(">")
        hitterMovingStats ++= movStat
        print(".")
        val volStat = sortedHistory.map({day => (day.date, day.id, 
          someOrNone(day.battingVolatility.rh), someOrNone(day.battingVolatility.lh), someOrNone(day.battingVolatility.total),  
          someOrNone(day.onBaseVolatility.rh), someOrNone(day.onBaseVolatility.lh), someOrNone(day.onBaseVolatility.total),
          someOrNone(day.sluggingVolatility.rh), someOrNone(day.sluggingVolatility.lh), someOrNone(day.sluggingVolatility.total))})
        print(">")
        hitterVolatilityStats ++= volStat
        print(".")
        val fantasyStat = sortedHistory.map({day => (day.date, day.id,
           someOrNone(day.fantasyScores(FanDuelName).rh), someOrNone(day.fantasyScores(FanDuelName).lh), someOrNone(day.fantasyScores(FanDuelName).total),
           someOrNone(day.fantasyScores(DraftKingsName).rh), someOrNone(day.fantasyScores(DraftKingsName).lh), someOrNone(day.fantasyScores(DraftKingsName).total),
           someOrNone(day.fantasyScores(DraftsterName).rh), someOrNone(day.fantasyScores(DraftsterName).lh), someOrNone(day.fantasyScores(DraftsterName).total))})
        print(">")
        hitterFantasy ++= fantasyStat
        print(".")
        val fantasyMovStat = sortedHistory.map({day => (day.date, day.id,
           someOrNone(day.fantasyScoresMov(FanDuelName).rh), someOrNone(day.fantasyScoresMov(FanDuelName).lh), someOrNone(day.fantasyScoresMov(FanDuelName).total),
           someOrNone(day.fantasyScoresMov(DraftKingsName).rh), someOrNone(day.fantasyScoresMov(DraftKingsName).lh), someOrNone(day.fantasyScoresMov(DraftKingsName).total),
           someOrNone(day.fantasyScoresMov(DraftsterName).rh), someOrNone(day.fantasyScoresMov(DraftsterName).lh), someOrNone(day.fantasyScoresMov(DraftsterName).total))})
        print(">")
        hitterFantasyMoving ++= fantasyMovStat
        println(".")
      }
    }
    progress = 0
    pitcherSummaries.values.map {playerHistory =>
      db.withTransaction { implicit session =>
        progress = progress + 1
        val sortedHistory = playerHistory.sortBy { x => x.date }
        print(playerHistory.head.id + " [" + progress + "/" + pitcherSummaries.size + "] " + sortedHistory.length + " ")
        print(".")
        val pitcherDaily = sortedHistory.map(_.record)
        print(">")
        pitcherDailyTable ++= pitcherDaily
        print(".")
        val fantasyStat = sortedHistory.map({day => (day.date, day.id,
           someOrNone(day.fantasyScores(FanDuelName).total), someOrNone(day.fantasyScores(DraftKingsName).total), someOrNone(day.fantasyScores(DraftsterName).total))})
        print(">")
        pitcherFantasy ++= fantasyStat
        print(".")
        val fantasyMovStat = sortedHistory.map({day => (day.date, day.id,
           someOrNone(day.fantasyScoresMov(FanDuelName).total), someOrNone(day.fantasyScoresMov(DraftKingsName).total), someOrNone(day.fantasyScoresMov(DraftsterName).total))})
        println(">")
        pitcherFantasyMoving ++= fantasyMovStat
      }
    }
  }

  maintainDatabase
  processBallparks
  var batterSummaries = Map.empty[String, List[RetrosheetHitterDay]]
  var pitcherSummaries = Map.empty[String, List[RetrosheetPitcherDay]]
  (2010 to 2014).map {i => processYear(i.toString)}
  computeStatistics
  persist
  
}
