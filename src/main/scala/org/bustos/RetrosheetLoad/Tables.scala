package org.bustos.RetrosheetLoad

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.lifted.{ProvenShape, ForeignKeyQuery}

class TeamsTable(tag: Tag)
  extends Table[(String, String, String, String)](tag, "teams") {

  def mnemonic: Column[String] = column[String]("mnemonic")
  def league: Column[String] = column[String]("league")
  def city: Column[String] = column[String]("city")
  def name: Column[String] = column[String]("name")

  // Every table needs a * projection with the same type as the table's type parameter
  def * : ProvenShape[(String, String, String, String)] = (mnemonic, league, city, name)
}

class PlayersTable(tag: Tag)
  extends Table[(String, String, String, String, String, String, String)](tag, "players") {

  def mnemonic: Column[String] = column[String]("mnemonic")
  def lastName: Column[String] = column[String]("lastName")
  def firstName: Column[String] = column[String]("firstName")
  def batsWith: Column[String] = column[String]("batsWith")
  def throwsWith: Column[String] = column[String]("throwsWith")
  def team: Column[String] = column[String]("team")
  def position: Column[String] = column[String]("position")

  // Every table needs a * projection with the same type as the table's type parameter
  def * : ProvenShape[(String, String, String, String, String, String, String)] = (mnemonic, lastName, firstName, batsWith, throwsWith, team, position)
}

class HitterRawLHStatsTable(tag: Tag)
  extends Table[(String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tag, "hitterRawLHstats") {

  def date: Column[String] = column[String]("date")
  def playerID: Column[String] = column[String]("playerID")
  def LHatBat: Column[Int] = column[Int]("LHatBat")
  def LHsingle: Column[Int] = column[Int]("LHsingle")
  def LHdouble: Column[Int] = column[Int]("LHdouble")
  def LHtriple: Column[Int] = column[Int]("LHtriple")
  def LHhomeRun: Column[Int] = column[Int]("LHhomeRun")
  def LHRBI: Column[Int] = column[Int]("LHRBI")
  def LHbaseOnBalls: Column[Int] = column[Int]("LHbaseOnBalls")
  def LHhitByPitch: Column[Int] = column[Int]("LHhitByPitch")
  def LHsacFly: Column[Int] = column[Int]("LHsacFly")
  def LHsacHit: Column[Int] = column[Int]("LHsacHit")

  // Every table needs a * projection with the same type as the table's type parameter
  def * : ProvenShape[(String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)] =
    (date, playerID, LHatBat, LHsingle, LHdouble, LHtriple, LHhomeRun, LHRBI, LHbaseOnBalls, LHhitByPitch, LHsacFly, LHsacHit)
}

class HitterRawRHStatsTable(tag: Tag)
  extends Table[(String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tag, "hitterRawRHstats") {

  def date: Column[String] = column[String]("date")
  def playerID: Column[String] = column[String]("playerID")
  def RHatBat: Column[Int] = column[Int]("RHatBat")
  def RHsingle: Column[Int] = column[Int]("RHsingle")
  def RHdouble: Column[Int] = column[Int]("RHdouble")
  def RHtriple: Column[Int] = column[Int]("RHtriple")
  def RHhomeRun: Column[Int] = column[Int]("RHhomeRun")
  def RHRBI: Column[Int] = column[Int]("RHRBI")
  def RHbaseOnBalls: Column[Int] = column[Int]("RHbaseOnBalls")
  def RHhitByPitch: Column[Int] = column[Int]("RHhitByPitch")
  def RHsacFly: Column[Int] = column[Int]("RHsacFly")
  def RHsacHit: Column[Int] = column[Int]("RHsacHit")

  // Every table needs a * projection with the same type as the table's type parameter
  def * : ProvenShape[(String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)] =
    (date, playerID, RHatBat, RHsingle, RHdouble, RHtriple, RHhomeRun, RHRBI, RHbaseOnBalls, RHhitByPitch, RHsacFly, RHsacHit)
}

class HitterDailyStatsTable(tag: Tag)
  extends Table[(String, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)](tag, "hitterDailyStats") {

  def date: Column[String] = column[String]("date")
  def playerID: Column[String] = column[String]("playerID")
  def RHdailyBattingAverage: Column[Double] = column[Double]("RHdailyBattingAverage")
  def LHdailyBattingAverage: Column[Double] = column[Double]("LHdailyBattingAverage")
  def dailyBattingAverage: Column[Double] = column[Double]("dailyBattingAverage")
  def RHbattingAverage: Column[Double] = column[Double]("RHbattingAverage")
  def LHbattingAverage: Column[Double] = column[Double]("LHbattingAverage")
  def battingAverage: Column[Double] = column[Double]("battingAverage")
  def RHonBasePercentage: Column[Double] = column[Double]("RHonBasePercentage")
  def LHonBasePercentage: Column[Double] = column[Double]("LHonBasePercentage")
  def onBasePercentage: Column[Double] = column[Double]("onBasePercentage")
  def RHsluggingPercentage: Column[Double] = column[Double]("RHsluggingPercentage")
  def LHsluggingPercentage: Column[Double] = column[Double]("LHsluggingPercentage")
  def sluggingPercentage: Column[Double] = column[Double]("sluggingPercentage")
  def RHfantasyScore: Column[Double] = column[Double]("RHfantasyScore")
  def LHfantasyScore: Column[Double] = column[Double]("LHfantasyScore")
  def fantasyScore: Column[Double] = column[Double]("fantasyScore")

  // Every table needs a * projection with the same type as the table's type parameter
  def * : ProvenShape[(String, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)] =
    (date, playerID, 
      RHdailyBattingAverage, LHdailyBattingAverage, dailyBattingAverage,  
      RHbattingAverage, LHbattingAverage, battingAverage,  
      RHonBasePercentage, LHonBasePercentage, onBasePercentage, RHsluggingPercentage, LHsluggingPercentage,
      sluggingPercentage, RHfantasyScore, LHfantasyScore, fantasyScore)
}

class HitterStatsMovingTable(tag: Tag)
  extends Table[(String, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)](tag, "hitterMovingStats") {

  def date: Column[String] = column[String]("date")
  def playerID: Column[String] = column[String]("playerID")
  def RHbattingAverage25: Column[Double] = column[Double]("RHbattingAverage25")
  def LHbattingAverage25: Column[Double] = column[Double]("LHbattingAverage25")
  def battingAverage25: Column[Double] = column[Double]("battingAverage25")
  def RHonBasePercentage25: Column[Double] = column[Double]("RHonBasePercentage25")
  def LHonBasePercentage25: Column[Double] = column[Double]("LHonBasePercentage25")
  def onBasePercentage25: Column[Double] = column[Double]("onBasePercentage25")
  def RHsluggingPercentage25: Column[Double] = column[Double]("RHsluggingPercentage25")
  def LHsluggingPercentage25: Column[Double] = column[Double]("LHsluggingPercentage25")
  def sluggingPercentage25: Column[Double] = column[Double]("sluggingPercentage25")
  def RHfantasyScore25: Column[Double] = column[Double]("RHfantasyScore25")
  def LHfantasyScore25: Column[Double] = column[Double]("LHfantasyScore25")
  def fantasyScore25: Column[Double] = column[Double]("fantasyScore25")

  // Every table needs a * projection with the same type as the table's type parameter
  def * : ProvenShape[(String, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)] =
    (date, playerID, 
      RHbattingAverage25, LHbattingAverage25, battingAverage25,  
      RHonBasePercentage25, LHonBasePercentage25, onBasePercentage25, RHsluggingPercentage25, LHsluggingPercentage25,
      sluggingPercentage25, RHfantasyScore25, LHfantasyScore25, fantasyScore25)
}

class HitterStatsVolatilityTable(tag: Tag)
  extends Table[(String, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)](tag, "hitterVolatilityStats") {

  def date: Column[String] = column[String]("date")
  def playerID: Column[String] = column[String]("playerID")
  def RHbattingVolatility100: Column[Double] = column[Double]("RHbattingVolatility100")
  def LHbattingVolatility100: Column[Double] = column[Double]("LHbattingVolatility100")
  def battingVolatility100: Column[Double] = column[Double]("battingVolatility100")
  def RHonBaseVolatility100: Column[Double] = column[Double]("RHonBaseVolatility100")
  def LHonBaseVolatility100: Column[Double] = column[Double]("LHonBaseVolatility100")
  def onBaseVolatility100: Column[Double] = column[Double]("onBaseVolatility100")
  def RHsluggingVolatility100: Column[Double] = column[Double]("RHsluggingVolatility100")
  def LHsluggingVolatility100: Column[Double] = column[Double]("LHsluggingVolatility100")
  def sluggingVolatility100: Column[Double] = column[Double]("sluggingVolatility100")
  def RHfantasyScoreVolatility100: Column[Double] = column[Double]("RHfantasyScoreVolatility100")
  def LHfantasyScoreVolatility100: Column[Double] = column[Double]("LHfantasyScoreVolatility100")
  def fantasyScoreVolatility100: Column[Double] = column[Double]("fantasyScoreVolatility100")

  // Every table needs a * projection with the same type as the table's type parameter
  def * : ProvenShape[(String, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)] =
    (date, playerID, 
      RHbattingVolatility100, LHbattingVolatility100, battingVolatility100,  
      RHonBaseVolatility100, LHonBaseVolatility100, onBaseVolatility100, RHsluggingVolatility100, LHsluggingVolatility100,
      sluggingVolatility100, RHfantasyScoreVolatility100, LHfantasyScoreVolatility100, fantasyScoreVolatility100)
}

