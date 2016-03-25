package org.bustos.realityball

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable
import org.slf4j.LoggerFactory
import java.nio.charset.CodingErrorAction
import java.io.File
import scala.io.Codec
import scala.io.Source
import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballRecords._
import com.github.tototoshi.csv._

class CrunchtimeBaseballMapping {

  val logger = LoggerFactory.getLogger(getClass)

  def processIdMappings = {
    logger.info("Updating id mapping codes...")

    implicit val codec = Codec("UTF-8")
    val nameExpression = """(.*?) (.*)""".r
    val juanCarlosExpression = """Juan Carlos (.*)""".r
    val yeanCarlosExpression = """Yean Carlos (.*)""".r
    val juanPabloExpression = """Juan Pablo (.*)""".r
    val miguelAlfredoExpression = """Miguel Alfredo (.*)""".r
    val luisAntonioExpression = """Luis Antonio (.*)""".r
    val johnRyanExpression = """John Ryan (.*)""".r
    val henryAlbertoExpression = """Henry Alberto (.*)""".r
    val henryAlejandroExpression = """Henry Alejandro (.*)""".r

    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val hasLatest = db.withSession { implicit session =>
      playersTable.filter(_.year === "2015").list.nonEmpty
    }

    db.withSession { implicit session =>
      if (!MTable.getTables("idMapping").list.isEmpty) {
        idMappingTable.ddl.drop
      }
      idMappingTable.ddl.create
      val reader = CSVReader.open(new File(DataRoot + "/crunchtimeBaseball/master.csv"))
      reader.allWithHeaders.foreach { line =>
        idMappingTable += IdMapping(line("mlb_id"), line("mlb_name"), line("mlb_team"),
          line("mlb_pos"), line("bats"), line("throws"),
          line("bref_id"), line("bref_name"),
          line("espn_id"), line("espn_name"),
          line("retro_id"), line("retro_name"))
        val retroId = if (line("retro_id") == "") line("mlb_id")
        else line("retro_id")
        val name = line("mlb_name") match {
          case juanCarlosExpression(last) => List("Juan Carlos", last)
          case yeanCarlosExpression(last) => List("Yean Carlos", last)
          case juanPabloExpression(last) => List("Juan Pablo", last)
          case miguelAlfredoExpression(last) => List("Miguel Alfredo", last)
          case luisAntonioExpression(last) => List("Luis Antonio", last)
          case johnRyanExpression(last) => List("John Ryan", last)
          case henryAlbertoExpression(last) => List("Henry Alberto", last)
          case henryAlejandroExpression(last) => List("Henry Alejandro", last)
          case nameExpression(first, last) => List(first, last)
        }
        val mlbTeam = {
          if (line("mlb_team") == "WSH") "WAS"
          else if (line("mlb_team") == "LAD") "LA"
          else if (line("mlb_team") == "LAA") "ANA"
          else line("mlb_team")
        }
        if (mlbTeam != "" && !hasLatest) {
          val retroTeam = teamsTable.filter({_.mlbComId === mlbTeam}).map(_.mnemonic).list.head
          val lastName = name(1).replaceAll(" Jr.","")
          playersTable += Player(retroId, "2015", lastName, name(0), line("bats"), line("throws"), retroTeam, line("mlb_pos"))
        }
      }
    }
  }

}
