library(RSQLite)
library(tidyverse)


# Add columns to "leagues" table ------------------------------------------

# import information about leagues from csv
leagues <- read_csv2("leagues.csv")
# connect to database
conn <- dbConnect(SQLite(), "football.db")
# fetch the 'leagues' table
leagues_db <- dbReadTable(conn, "leagues")
# add column with official league name to the 'leagues' table
dbGetQuery(conn,
  "ALTER TABLE leagues ADD COLUMN official_name TEXT"
)
leagues_newinfo <- left_join(leagues_db, leagues, by = "fd_league")
leagues_offname <- select(leagues_newinfo, fd_league, official_name)
dbExecute(conn, 
  "UPDATE leagues 
  SET official_name = :official_name 
  WHERE fd_league = :fd_league", 
  params = leagues_offname
)
# add column with transfermarkt.de abbreviation for league 
dbGetQuery(conn,
 "ALTER TABLE leagues ADD COLUMN tm_short TEXT"
)
leagues_tmshort <- select(leagues_newinfo, fd_league, tm_short)
dbExecute(conn, 
  "UPDATE leagues 
  SET tm_short = :tm_short 
  WHERE fd_league = :fd_league", 
  params = leagues_tmshort
)
dbDisconnect(conn)


# Missing data? full time results -----------------------------------------

conn <- dbConnect(SQLite(), "football.db")

# find missing full time results
missing_results <- dbGetQuery(conn,
  "SELECT
    game_id,
	  leagues.fd_league AS Div,
	  leagues.league AS league,
	  season,
	  game_date,
	  hometeam.team AS hometeam,
	  awayteam.team AS awayteam,
	  FTHG,
	  FTAG,
	  FTR,
	  HTHG,
	  HTAG,
	  HTR
  FROM
	  games
  LEFT JOIN leagues ON games.league_id = leagues.league_id
  LEFT JOIN teams AS hometeam ON hometeam.team_id = games.hometeam_id 
  LEFT JOIN teams AS awayteam ON awayteam.team_id = games.awayteam_id
  WHERE FTHG IS NULL OR FTAG IS NULL;"  
)
missing_results

# The match Panathinaikos v Olympiakos on March 17th, 2019 had been interrupted 
# because of fan riots. There is no official half time score. The game had been 
# ruled as 0:3 in favor of Olympiakos. One could argue that the full time score 
# should not be included in the database since the goals did not come about in a 
# real match. But since the ruling is within the normal rules of the Greek 
# league the score could also be included as a full time result.
# 
# The match Niki Volos v OFI on May 10th, 2015 had been cancelled.

# change full time score for Panathinaikos v Olympiakos on March 17th, 2019
dbExecute(conn,
          "UPDATE games
  SET
    (FTHG, FTAG, FTR, HTHG, HTAG, HTR)
    = ('0', '3', 'A', NULL, NULL, NULL)
  WHERE game_id = 183433"
)
dbDisconnect(conn)

# Team name discrepancies -------------------------------------------------

conn <- dbConnect(SQLite(), "football.db")

# GERMANY

# "Dusseldorf" with team_id 212. "Fortuna Dusseldorf" with team_id 276 
# change team_id to 276 where it is equal to 212
dbExecute(conn,
  "UPDATE games SET hometeam_id = 276 WHERE hometeam_id = 212"
)
dbExecute(conn,
  "UPDATE games SET awayteam_id = 276 WHERE awayteam_id = 212"
)

# "M'Gladbach" with team_id 430. "M'gladbach" with team_id 429 (new value)
# change team_id to 429 where it is equal to 430
dbExecute(conn,
  "UPDATE games SET hometeam_id = 429 WHERE hometeam_id = 430"
)
dbExecute(conn,
  "UPDATE games SET awayteam_id = 429 WHERE awayteam_id = 430"
)

dbDisconnect(conn)