package org.bustos.realityball

object FantasyScoreSheet {
  
  val FanDuelName = "FanDuel"
  val DraftKingsName = "DraftKings"
  val DraftsterName = "Draftster"

  val hittingEvents = List("1B", "2B", "3B", "HR", "RBI", "R", "BB", "HBP", "OUT", "SO", "DP", "SB", "CS")
  val pitchingEvents = List("W", "L", "H", "BB", "HBP", "SO", "ER", "IP", "CG", "CG+SO", "NH")
  
  val FanDuelBatting = Map(("1B" -> 1.0), ("2B" -> 2.0), ("3B" -> 3.0), ("HR" -> 4.0), ("RBI" -> 1.0), ("R" -> 1.0),
                           ("BB" -> 1.0), ("HBP" -> 1.0), ("OUT" -> -0.25), ("SO" -> 0.0), ("DP" -> 0.0), ("SB" -> 2.0),
                           ("CS" -> 0.0))

  val DraftKingsBatting = Map(("1B" -> 3.0), ("2B" -> 5.0), ("3B" -> 8.0), ("HR" -> 10.0), ("RBI" -> 2.0), ("R" -> 2.0),
                              ("BB" -> 2.0), ("HBP" -> 2.0), ("OUT" -> 0.0), ("SO" -> 0.0), ("DP" -> 0.0), ("SB" -> 0.0),
                              ("CS" -> 0.0))

  val DraftsterBatting = Map(("1B" -> 1.0), ("2B" -> 2.0), ("3B" -> 3.0), ("HR" -> 4.0), ("RBI" -> 1.0), ("R" -> 1.0),
                             ("BB" -> 1.0), ("HBP" -> 1.0), ("OUT" -> 0.0), ("SO" -> -0.75), ("DP" -> -0.75), ("SB" -> 2.0),
                             ("CS" -> -2.0))

  val FantasyGamesBatting = Map((FanDuelName -> FanDuelBatting), (DraftKingsName -> DraftKingsBatting), (DraftsterName -> DraftsterBatting))
    
  val FanDuelPitching = Map(("W" -> 4.0), ("L" -> 0.0), ("H" -> 0.0), ("BB" -> 0.0), ("HBP" -> 0.0), ("SO" -> 1.0), ("ER" -> -1.0),
                            ("IP" -> 1.0), ("CG" -> 0.0), ("CG+SO" -> 0.0), ("NH" -> 0.0))

  val DraftKingsPitching = Map(("W" -> 4.0), ("L" -> -2.0), ("H" -> -0.6), ("BB" -> -0.6), ("HBP" -> -0.6), ("SO" -> 2.0), ("ER" -> -2.0),
                               ("IP" -> 2.25), ("CG" -> 2.5), ("CG+SO" -> 2.5), ("NH" -> 5.0))

  val DraftsterPitching = Map(("W" -> 4.0), ("L" -> 0.0), ("H" -> -0.25), ("BB" -> -0.25), ("HBP" -> -1.0), ("SO" -> 1.0), ("ER" -> -1.0),
                              ("IP" -> 0.9), ("CG" -> 2.5), ("CG+SO" -> 0.0), ("NH" -> 5.0))

  val FantasyGamesPitching = Map((FanDuelName -> FanDuelPitching), (DraftKingsName -> DraftKingsPitching), (DraftsterName -> DraftsterPitching))
  
  val gamesComplete: Boolean = {
    val battingGames = FantasyGamesBatting.foldLeft(true)({case (r, (k, v)) => r && hittingEvents.foldLeft(true)({case (r, ev) => r && v.contains(ev)})})
    val pitchingGames = FantasyGamesPitching.foldLeft(true)({case (r, (k, v)) => r && pitchingEvents.foldLeft(true)({case (r, ev) => r && v.contains(ev)})})
    if (battingGames && pitchingGames) true
    else throw new Exception("Games are not well specified")
  }
  
}