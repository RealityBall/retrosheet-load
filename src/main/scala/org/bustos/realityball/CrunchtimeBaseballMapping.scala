package org.bustos.realityball

import java.io.File
import java.nio.charset.CodingErrorAction

import com.github.tototoshi.csv._
import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballRecords._
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.meta.MTable

import scala.concurrent.Await
import scala.concurrent.duration.Duration._
import scala.io.Codec

class CrunchtimeBaseballMapping {

  val logger = LoggerFactory.getLogger("RetrosheetLoad")

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

    val hasLatest = Await.result(db.run(playersTable.filter(_.year === "2015").result), Inf).nonEmpty

    val existing_tables = Await.result(db.run(MTable.getTables), Inf).toList.map(_.name.name)
    if (existing_tables.contains("idMapping")) {
      Await.result(db.run(idMappingTable.schema.drop), Inf)
    }
    Await.result(db.run(idMappingTable.schema.create), Inf)

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
        val retroTeam = Await.result(db.run(teamsTable.filter({_.mlbComId === mlbTeam}).map(_.mnemonic).result), Inf).head
        val lastName = name(1).replaceAll(" Jr.","")
        playersTable += Player(retroId, "2015", lastName, name(0), line("bats"), line("throws"), retroTeam, line("mlb_pos"))
      }
    }
  }

}
