package org.bustos.realityball

import org.bustos.realityball.common.RealityballRecords._

class RetrosheetGameInfo(val id: String) {


  val game = Game(id, "", "", "", "", 0, "", "")
  val conditions = GameConditions(id, "", "", false, 0, "", 0, "", "", "")
  val scoring = GameScoring(id, "", "", "", "", "", 0, 0, "", "", "")

  def processInfoRecord(record: String) = {
    val items = record.split(",")
    items(1) match {
      case "visteam"    => game.visitingTeam = items(2)
      case "hometeam"   => game.homeTeam = items(2)
      case "site"       => game.site = items(2)
      case "date"       => game.date = items(2)
      case "number"     => game.number = items(2).toInt
      case "starttime"  => conditions.startTime = items(2)
      case "daynight"   => conditions.daynight = items(2)
      case "usedh"      => conditions.usedh = items(2) == "true"
      case "umphome"    => scoring.umphome = items(2)
      case "ump1b"      => scoring.ump1b = items(2)
      case "ump2b"      => scoring.ump2b = items(2)
      case "ump3b"      => scoring.ump3b = items(2)
      case "howscored"  => scoring.howscored = items(2)
      case "temp"       => if (items(2).forall(_.isDigit)) conditions.temp = items(2).toInt
      case "winddir"    => if (items.size > 2) conditions.winddir = items(2) else ""
      case "windspeed"  => if (items.size > 2) conditions.windspeed = items(2).toInt else ""
      case "fieldcond"  => conditions.fieldcond = items(2)
      case "precip"     => conditions.precip = items(2)
      case "sky"        => conditions.sky = items(2)
      case "timeofgame" => scoring.timeofgame = items(2).toFloat.toInt
      case "attendance" => scoring.attendance = items(2).toFloat.toInt
      case "wp"         => scoring.wp = items(2)
      case "lp"         => scoring.lp = items(2)
      case "save"       => if (items.length > 2) scoring.save = items(2)
      case _            =>
    }
  }
}
