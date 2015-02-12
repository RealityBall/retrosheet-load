package org.bustos.realityball

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable
import org.slf4j.LoggerFactory
import java.nio.charset.CodingErrorAction
import java.io.File
import scala.io.Codec
import scala.io.Source
import RealityballConfig._
import RealityballRecords._
import com.github.tototoshi.csv._

class CrunchtimeBaseballMapping {

  val logger = LoggerFactory.getLogger(getClass)

  def processIdMappings = {
    logger.info("Updating id mapping codes...")

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    db.withSession { implicit session =>
      if (!MTable.getTables("idMapping").list.isEmpty) {
        idMappingTable.ddl.drop
      }
      idMappingTable.ddl.create
      val reader = CSVReader.open(new File(DataRoot + "/crunchtimeBaseball/master.csv"))
      reader.allWithHeaders.foreach { line =>
        idMappingTable += IdMapping(line("mlb_id"), line("mlb_name"), line("mlb_team"),
          line("bref_id"), line("bref_name"),
          line("espn_id"), line("espn_name"),
          line("retrosheet_id"), line("retrosheet_name"))
      }
    }
  }

}
