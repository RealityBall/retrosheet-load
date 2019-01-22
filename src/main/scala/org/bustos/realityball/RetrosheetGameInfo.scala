/*

    Copyright (C) 2019 Mauricio Bustos (m@bustos.org)

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

*/

package org.bustos.realityball

import org.bustos.realityball.common.RealityballRecords._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class RetrosheetGameInfo(val id: String) {

  val dateParser = DateTimeFormat.forPattern("yyyy/MM/dd")
  val timeParser = DateTimeFormat.forPattern("hh:mma")

  val game = Game(id, "", "", "", new DateTime, 0, "", "")
  val conditions = GameConditions(id, new DateTime, "", false, 0, "", 0, "", "", "")
  val scoring = GameScoring(id, "", "", "", "", "", 0, 0, "", "", "")

  def processInfoRecord(record: String) = {
    val items = record.split(",")
    println(record)
    items(1) match {
      case "visteam"    => game.visitingTeam = items(2)
      case "hometeam"   => game.homeTeam = items(2)
      case "site"       => game.site = items(2)
      case "date"       => game.date = dateParser.parseDateTime(items(2))
      case "number"     => game.number = items(2).toInt
      case "starttime"  => conditions.startTime = timeParser.parseDateTime(items(2))
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
      case "wp"         => if (items.size > 2) scoring.wp = items(2) else ""
      case "lp"         => if (items.size > 2) scoring.lp = items(2) else ""
      case "save"       => if (items.length > 2) scoring.save = items(2)
      case _            =>
    }
  }
}
