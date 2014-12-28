package org.bustos.RetrosheetLoad

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.slick.lifted.AbstractTable
import java.io.File
import scala.io.Source
import scala.util.matching.Regex
import scala.collection.mutable.Queue

object RetrosheetLoad extends App {

  import RetrosheetRecords._
  
  val gameIdExpression: Regex = "id,(.*)".r
  val gameInfoExpression: Regex = "info,(.*)".r
  val pitcherHomeExpression: Regex = "start,(.*),(.*),0,(.*),1".r
  val pitcherAwayExpression: Regex = "start,(.*),(.*),1,(.*),1".r
  val pitcherSubHomeExpression: Regex = "sub,(.*),(.*),0,(.*),1".r
  val pitcherSubAwayExpression: Regex = "sub,(.*),(.*),1,(.*),1".r
  val playExpression: Regex = "play,(.*),(.*),(.*),(.*),(.*),(.*)".r
  val pitcherRoster: Regex = "(.*),(.*),(.*),([BRL]),([BRL]),(.*),(.*)".r
  val teamExpression: Regex = "(.*),(.*),(.*),(.*)".r

  val eventFiles = (new File("""/Users/mauricio/Google Drive/Projects/fantasySports/retrosheetData/2014eve""")).listFiles.filter(x => x.getName.endsWith(".EVN") || x.getName.endsWith(".EVA"))
  var daySummaries = Map.empty[String, List[RetrosheetHitterDay]]

  val teamFile = new File("""/Users/mauricio/Google Drive/Projects/fantasySports/retrosheetData/2014eve/TEAM2014""")
  var teams = Map.empty[String, Team]
  var games = List.empty[RetrosheetGameInfo]

  for (line <- Source.fromFile(teamFile).getLines) {
    line match {
      case teamExpression(mnemonic, league, city, name) => teams += (mnemonic -> Team(mnemonic, league, city, name))
      case _ =>
    }
  }

  val rosterFiles = (new File("""/Users/mauricio/Google Drive/Projects/fantasySports/retrosheetData/2014eve""")).listFiles.filter(x => x.getName.endsWith(".ROS"))
  var players = Map.empty[String, Player]

  for (f <- rosterFiles) {
    for (line <- Source.fromFile(f).getLines) {
      line match {
        case pitcherRoster(id, lastName, firstName, hits, throwsWith, team, position) => players += (id -> Player(id, lastName, firstName, hits, throwsWith, team, position))
        case _ =>
      }
    }
  }

  for (f <- eventFiles) {
    println("")
    println(f.getName + " - ")
    var currentGame: RetrosheetGameInfo = null
    var currentHomePitcher: Player = null
    var currentAwayPitcher: Player = null
    var currentInning: Int = 1
    var currentSide: Int = 0
    var runners: List[String] = List("        ", "        ", "        ")
    for (line <- Source.fromFile(f).getLines) {
      line match {
        case gameIdExpression(id) => {
          currentGame = new RetrosheetGameInfo(id)
          games = currentGame :: games
          print(currentGame.game.id + ",")
        }
        case gameInfoExpression(data) => {
          currentGame.processInfoRecord(line)
        }
        case pitcherHomeExpression(id, name, hitting)    => currentHomePitcher = players(id)
        case pitcherAwayExpression(id, name, hitting)    => currentAwayPitcher = players(id)
        case pitcherSubHomeExpression(id, name, hitting) => currentHomePitcher = players(id)
        case pitcherSubAwayExpression(id, name, hitting) => currentAwayPitcher = players(id)
        case playExpression(inning, side, id, count, pitches, playString) => {
          if (inning.toInt != currentInning || side.toInt != currentSide) {
            currentInning = inning.toInt
            currentSide = side.toInt
            runners = List("        ", "        ", "        ")
          }
          //if (playString.contains(".")) print("inning: " + inning + " ")
          val play = new RetrosheetPlay(playString)
          if (!daySummaries.contains(id)) {
            val newHitterDay = new RetrosheetHitterDay(currentGame.game.date, id)
            daySummaries += (id -> List(newHitterDay))
          }
          var currentHitterDay = daySummaries(id).head
          if (currentHitterDay.date != currentGame.game.date) {
            currentHitterDay = new RetrosheetHitterDay(currentGame.game.date, id)
            daySummaries += (id -> (currentHitterDay :: daySummaries(id)))
          }
          val facingRighty = (side == "0" && currentAwayPitcher.throwsWith == "R") || (side == "1" && currentHomePitcher.throwsWith == "R")
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

  println("")
  println("Computing Running Statistics.")
  for (playerHistory <- daySummaries.values) {
    print(".")
    val sortedHistory = playerHistory.sortBy { x => x.date }
    val currentHitterDay = new RetrosheetHitterDay("", "")
    val movingAverageData = RunningStatistics(currentHitterDay, RunningData(Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[Statistic]), 
                                              RunningVolatilityData(Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[StatisticInputs], Queue.empty[Statistic]))
    val finalStatistcs = sortedHistory.foldLeft(movingAverageData)(computeRunningPlayerStats)
  }
  println("")
  
  def computeRunningPlayerStats (data: RunningStatistics, dailyData: RetrosheetHitterDay): RunningStatistics = {
    dailyData.accumulate(data)
    data
  }

  var tables = Map.empty[String, TableQuery[_ <: slick.driver.MySQLDriver.Table[_]]]
  val hitterRawLH: TableQuery[HitterRawLHStatsTable] = TableQuery[HitterRawLHStatsTable]
  tables += ("hitterRawLHstats" -> hitterRawLH)
  val hitterRawRH: TableQuery[HitterRawRHStatsTable] = TableQuery[HitterRawRHStatsTable]
  tables += ("hitterRawRHstats" -> hitterRawRH)
  val hitterStats: TableQuery[HitterDailyStatsTable] = TableQuery[HitterDailyStatsTable]
  tables += ("hitterDailyStats" -> hitterStats)
  val hitterMovingStats: TableQuery[HitterStatsMovingTable] = TableQuery[HitterStatsMovingTable]
  tables += ("hitterMovingStats" -> hitterMovingStats)
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

  // Create a connection (called a "session") to an in-memory H2 database
  val db = Database.forURL("jdbc:mysql://localhost:3306/mlbretrosheet", driver="com.mysql.jdbc.Driver", user="root", password="")
  //val db = Database.forURL("jdbc:mysql://mysql.bustos.org:3306/mlbretrosheet", driver = "com.mysql.jdbc.Driver", user = "mlbrsheetuser", password = "mlbsheetUser")

  db.withSession { implicit session =>

    def maintainTable(schema: TableQuery[_ <: slick.driver.MySQLDriver.Table[_]], name: String) = {
      if (!MTable.getTables(name).list.isEmpty) {
        schema.ddl.drop
      }
      schema.ddl.create
    }
    tables.map {case (k, v) => maintainTable(v, k)}

    games.map {x => gamesTable += x.game; gameConditionsTable += x.conditions; gameScoringTable += x.scoring}
    for (team <- teams.values) {
      teamsTable += (team.mnemonic, team.league, team.city, team.name)
    }
    for (player <- players.values) {
      playersTable += Player(player.id, player.lastName, player.firstName, player.batsWith, player.throwsWith, player.team, player.position)
    }

    def someOrNone(x: Double): Option[Double] = {if (x.isNaN) None else Some(x)}
    
    var progress: Int = 0
    for (playerHistory <- daySummaries.values) {
      //val playerHistory = daySummaries("cabrm001")
      progress = progress + 1
      println(playerHistory.head.playerID + " [" + progress + "/" + daySummaries.size + "]")
      val sortedHistory = playerHistory.sortBy { x => x.date }
      for (day <- sortedHistory) {
        hitterRawLH += (day.date, day.playerID,
          day.LHatBat, day.LHsingle, day.LHdouble, day.LHtriple, day.LHhomeRun, day.LHRBI,
          day.LHbaseOnBalls, day.LHhitByPitch, day.LHsacFly, day.LHsacHit)
        hitterRawRH += (day.date, day.playerID,
          day.RHatBat, day.RHsingle, day.RHdouble, day.RHtriple, day.RHhomeRun, day.RHRBI,
          day.RHbaseOnBalls, day.RHhitByPitch, day.RHsacFly, day.RHsacHit)
        hitterStats += (day.date, day.playerID,
          someOrNone(day.dailyBattingAverage.rh), someOrNone(day.dailyBattingAverage.lh), someOrNone(day.dailyBattingAverage.total),
          someOrNone(day.battingAverage.rh), someOrNone(day.battingAverage.lh), someOrNone(day.battingAverage.total),
          someOrNone(day.onBasePercentage.rh), someOrNone(day.onBasePercentage.lh), someOrNone(day.onBasePercentage.total),
          someOrNone(day.sluggingPercentage.rh), someOrNone(day.sluggingPercentage.lh), someOrNone(day.sluggingPercentage.total),
          someOrNone(day.fantasyScore.rh), someOrNone(day.fantasyScore.lh), someOrNone(day.fantasyScore.total))
        hitterMovingStats += (day.date, day.playerID, 
          someOrNone(day.battingAverage25.rh), someOrNone(day.battingAverage25.lh), someOrNone(day.battingAverage25.total),  
          someOrNone(day.onBasePercentage25.rh), someOrNone(day.onBasePercentage25.lh), someOrNone(day.onBasePercentage25.total), 
          someOrNone(day.sluggingPercentage25.rh), someOrNone(day.sluggingPercentage25.lh), someOrNone(day.sluggingPercentage25.total), 
          someOrNone(day.fantasyScore25.rh), someOrNone(day.fantasyScore25.lh), someOrNone(day.fantasyScore25.total))
        hitterVolatilityStats += (day.date, day.playerID, 
          someOrNone(day.battingVolatility100.rh), someOrNone(day.battingVolatility100.lh), someOrNone(day.battingVolatility100.total),  
          someOrNone(day.onBaseVolatility100.rh), someOrNone(day.onBaseVolatility100.lh), someOrNone(day.onBaseVolatility100.total),
          someOrNone(day.sluggingVolatility100.rh), someOrNone(day.sluggingVolatility100.lh), someOrNone(day.sluggingVolatility100.total), 
          someOrNone(day.fantasyScoreVolatility100.rh), someOrNone(day.fantasyScoreVolatility100.lh), someOrNone(day.fantasyScoreVolatility100.total))
      }
    }
  }
}
