package org.bustos.RetrosheetLoad

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable
import scala.util.matching.Regex
import java.io.File
import scala.io.Source

// The main application
object RetrosheetLoad extends App {

  import scala.collection.mutable.Queue
  case class RunningData(ba: Queue[(Int, Int)], obp: Queue[(Int, Int)], slugging: Queue[(Int, Int)],
                         baRH: Queue[(Int, Int)], obpRH: Queue[(Int, Int)], sluggingRH: Queue[(Int, Int)],
                         baLH: Queue[(Int, Int)], obpLH: Queue[(Int, Int)], sluggingLH: Queue[(Int, Int)])
  case class RunningVolatilityData(ba: Queue[(Int, Int)], obp: Queue[(Int, Int)], slugging: Queue[(Int, Int)],
                                   baRH: Queue[(Int, Int)], obpRH: Queue[(Int, Int)], sluggingRH: Queue[(Int, Int)],
                                   baLH: Queue[(Int, Int)], obpLH: Queue[(Int, Int)], sluggingLH: Queue[(Int, Int)])
  case class RunningStatistics(fullAccum: RetrosheetHitterDay, averagesData: RunningData, volatilityData: RunningVolatilityData)
  case class Team(mnemonic: String, league: String, city: String, name: String)
  case class Player(id: String, lastName: String, firstName: String, batsWith: String, throwsWith: String, team: String, position: String)

  val dateExpression: Regex = "info,date,(2013/.*/.*)".r
  val pitcherHomeExpression: Regex = "start,(.*),(.*),0,(.*),1".r
  val pitcherAwayExpression: Regex = "start,(.*),(.*),1,(.*),1".r
  val pitcherSubHomeExpression: Regex = "sub,(.*),(.*),0,(.*),1".r
  val pitcherSubAwayExpression: Regex = "sub,(.*),(.*),1,(.*),1".r
  val playExpression: Regex = "play,(.*),(.*),(.*),(.*),(.*),(.*)".r
  val pitcherRoster: Regex = "(.*),(.*),(.*),([BRL]),([BRL]),(.*),(.*)".r
  val teamExpression: Regex = "(.*),(.*),(.*),(.*)".r

  val eventFiles = (new File("""/Users/mauricio/Google Drive/Projects/fantasySports/2013eve""")).listFiles.filter(x => x.getName.endsWith(".EVN") || x.getName.endsWith(".EVA"))
  var daySummaries = Map.empty[String, List[RetrosheetHitterDay]]

  val teamFile = new File("""/Users/mauricio/Google Drive/Projects/fantasySports/2013eve/TEAM2013""")
  var teams = Map.empty[String, Team]

  for (line <- Source.fromFile(teamFile).getLines) {
    line match {
      case teamExpression(mnemonic, league, city, name) => teams += (mnemonic -> Team(mnemonic, league, city, name))
      case _ =>
    }
  }

  val rosterFiles = (new File("""/Users/mauricio/Google Drive/Projects/fantasySports/2013eve""")).listFiles.filter(x => x.getName.endsWith(".ROS"))
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
    var currentDate = ""
    var currentHomePitcher: Player = null
    var currentAwayPitcher: Player = null
    var currentInning: Int = 1
    var currentSide: Int = 0
    var runners: List[String] = List("        ", "        ", "        ")
    for (line <- Source.fromFile(f).getLines) {
      line match {
        case dateExpression(date) => {
          currentDate = date
          print(currentDate + ",")
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
            val newHitterDay = new RetrosheetHitterDay(currentDate, id)
            daySummaries += (id -> List(newHitterDay))
          }
          var currentHitterDay = daySummaries(id).head
          if (currentHitterDay.date != currentDate) {
            currentHitterDay = new RetrosheetHitterDay(currentDate, id)
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
    val movingAverageData = RunningStatistics(currentHitterDay,
                                              RunningData(Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)],Queue.empty[(Int, Int)]),
                                              RunningVolatilityData(Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)], Queue.empty[(Int, Int)],Queue.empty[(Int, Int)]))
    val finalStatistcs = sortedHistory.foldLeft(movingAverageData)(computeRunningPlayerStats)
  }
  println("")
  
  def computeRunningPlayerStats (data: RunningStatistics, dailyData: RetrosheetHitterDay): RunningStatistics = {
    dailyData.accumulate(data)
    data
  }

  val hitterRawLH: TableQuery[HitterRawLHStatsTable] = TableQuery[HitterRawLHStatsTable]
  val hitterRawRH: TableQuery[HitterRawRHStatsTable] = TableQuery[HitterRawRHStatsTable]
  val hitterStats: TableQuery[HitterDailyStatsTable] = TableQuery[HitterDailyStatsTable]
  val hitterMovingStats: TableQuery[HitterStatsMovingTable] = TableQuery[HitterStatsMovingTable]
  val hitterVolatilityStats: TableQuery[HitterStatsVolatilityTable] = TableQuery[HitterStatsVolatilityTable]
  val teamsTable: TableQuery[TeamsTable] = TableQuery[TeamsTable]
  val playersTable: TableQuery[PlayersTable] = TableQuery[PlayersTable]

  // Create a connection (called a "session") to an in-memory H2 database
  //val db = Database.forURL("jdbc:mysql://localhost:3306/test", driver="com.mysql.jdbc.Driver", user="root", password="")
  val db = Database.forURL("jdbc:mysql://mysql.bustos.org:3306/mlbretrosheet", driver = "com.mysql.jdbc.Driver", user = "mlbrsheetuser", password = "mlbsheetUser")

  db.withSession { implicit session =>

    // Create the schema by combining the DDLs for the Suppliers and Coffees
    // tables using the query interfaces

    if (MTable.getTables("hitterRawLHstats").list.isEmpty) {
      hitterRawLH.ddl.create
    }
    if (MTable.getTables("hitterRawRHstats").list.isEmpty) {
      hitterRawRH.ddl.create
    }
    if (MTable.getTables("hitterDailyStats").list.isEmpty) {
      hitterStats.ddl.create
    }
    if (MTable.getTables("hitterMovingStats").list.isEmpty) {
      hitterMovingStats.ddl.create
    }
    if (MTable.getTables("hitterVolatiltyStats").list.isEmpty) {
      hitterVolatilityStats.ddl.create
    }
    if (MTable.getTables("teams").list.isEmpty) {
      teamsTable.ddl.create
    }
    if (MTable.getTables("players").list.isEmpty) {
      playersTable.ddl.create
    }

    import scala.slick.jdbc.StaticQuery.interpolation
    val truncateLH = sql"truncate table hitterRawLHstats".as[String]
    truncateLH.execute
    val truncateRH = sql"truncate table hitterRawLHstats".as[String]
    truncateRH.execute
    val truncateStats = sql"truncate table hitterDailyStats".as[String]
    truncateStats.execute
    val truncateMovingStats = sql"truncate table hitterMovingStats".as[String]
    truncateMovingStats.execute
    val truncateVolatilityStats = sql"truncate table hitterVolatilityStats".as[String]
    truncateVolatilityStats.execute
    val truncateTeams = sql"truncate table teams".as[String]
    truncateTeams.execute
    val truncatePlayers = sql"truncate table players".as[String]
    truncatePlayers.execute

    /* Create / Insert */

    for (player <- players.values) {
      playersTable += (player.id, player.lastName, player.firstName, player.batsWith, player.throwsWith, player.team, player.position)
    }
    for (team <- teams.values) {
      teamsTable += (team.mnemonic, team.league, team.city, team.name)
    }

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
          day.RHbattingAverage, day.LHbattingAverage, day.battingAverage,
          day.RHonBasePercentage, day.RHonBasePercentage, day.onBasePercentage,
          day.RHsluggingPercentage, day.LHsluggingPercentage, day.sluggingPercentage,
          day.RHfantasyScore, day.LHfantasyScore, day.fantasyScore)
        hitterMovingStats += (day.date, day.playerID, 
          day.RHbattingAverage25, day.LHbattingAverage25, day.battingAverage25,  
          day.RHonBasePercentage25, day.LHonBasePercentage25, day.onBasePercentage25, day.RHsluggingPercentage25, day.LHsluggingPercentage25,
          day.sluggingPercentage25, day.RHfantasyScore25, day.LHfantasyScore25, day.fantasyScore25)
        hitterVolatilityStats += (day.date, day.playerID, 
          day.RHbattingVolatility100, day.LHbattingVolatility100, day.battingVolatility100,  
          day.RHonBaseVolatility100, day.LHonBaseVolatility100, day.onBaseVolatility100, day.RHsluggingVolatility100, day.LHsluggingVolatility100,
          day.sluggingVolatility100, day.RHfantasyScoreVolatility100, day.LHfantasyScoreVolatility100, day.fantasyScoreVolatility100)
      }
    }
  }
}
