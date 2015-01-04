package org.bustos.RetrosheetLoad

object FantasyScoreSheet {
  
  val FanDuelName = "FanDuel"
  val DraftKingsName = "DraftKings"
  val DraftsterName = "Draftster"

  val hittingEvents = List("1B", "2B", "3B", "HR", "RBI", "R", "BB", "HBP", "OUT", "SO", "DP", "SB", "CS")
  val pitchingEvents = List("W", "L", "H", "BB", "HBP", "SO", "ER", "IP", "CG", "CG+SO", "NH")
  
  val FanDuel = Map(("1B" -> 1.0),
                    ("2B" -> 2.0),
                    ("3B" -> 3.0),
                    ("HR" -> 4.0),
                    ("RBI" -> 1.0),
                    ("R" -> 1.0),
                    ("BB" -> 1.0),
                    ("HBP" -> 1.0),
                    ("OUT" -> -0.25),
                    ("SO" -> 0.0),
                    ("DP" -> 0.0),
                    ("SB" -> 2.0),
                    ("CS" -> 0.0))

  val DraftKings = Map(("1B" -> 3.0),
                       ("2B" -> 5.0),
                       ("3B" -> 8.0),
                       ("HR" -> 10.0),
                       ("RBI" -> 2.0),
                       ("R" -> 2.0),
                       ("BB" -> 2.0),
                       ("HBP" -> 2.0),
                       ("OUT" -> 0.0),
                       ("SO" -> 0.0),
                       ("DP" -> 0.0),
                       ("SB" -> 0.0),
                       ("CS" -> 0.0))

  val Draftster = Map(("1B" -> 1.0),
                      ("2B" -> 2.0),
                      ("3B" -> 3.0),
                      ("HR" -> 4.0),
                      ("RBI" -> 1.0),
                      ("R" -> 1.0),
                      ("BB" -> 1.0),
                      ("HBP" -> 1.0),
                      ("OUT" -> 0.0),
                      ("SO" -> -0.75),
                      ("DP" -> -0.75),
                      ("SB" -> 2.0),
                      ("CS" -> -2.0))

  val FantasyGames = Map((FanDuelName -> FanDuel), (DraftKingsName -> DraftKings), (DraftsterName -> Draftster))
  
}