##----------------------------------------------------------------------------##
# This script fetches all csv files with historical data from several leagues  
# of 11 european  countries from football-data.co.uk and combines the csv      
# files into one sqlite database.                                              
##----------------------------------------------------------------------------##

library(data.table)
library(lubridate)
library(RSQLite)
library(rvest)
library(tidyverse)


# function to find links to the csv files on webpages
find_csv_links <- function(url) {
  page <- read_html(url)
  csv_nodes <- html_nodes(page, "a[href$='.csv']")
  csv_links <- html_attr(csv_nodes, "href")
  csv_names <- html_text(csv_nodes)
  csv_div <- sub(".*/.*/", "", csv_links) 
  names(csv_links) <- csv_names
  return(csv_links)
}

# function to extract the season (e.g. 2018/2019 as "2019") 
# from the links to the csv files
extract_season <- function(string) {
  if (str_detect(string, "/\\d{4}/")) {
    season <- str_extract(string, "/\\d{4}/.*\\.csv")
    season <- sub("/", "", str_extract(season, "\\d{2}/"))
    season <- as.character(year(as.Date(season, "%y")))
  }
}

# function that generates the part of a "CREATE TABLE"-sqlite-command in which  
# the column names and corresponding data types are specified based on the  
# column names and types of a dateframe. input: dataframe, output: string
col_names_types <- function(df) {
  field_types <- unlist(sapply(df, class))
  field_names <- names(field_types)
  names(field_types) <- c()
  field_types <- toupper(ifelse(field_types == "character", "text", field_types))
  field_types <- ifelse(field_types == "NUMERIC", "REAL", field_types)
  field_types <- paste(field_names, field_types, sep = " ")
  part_stmnt <- paste(field_types, collapse = ", ")
  return(part_stmnt)
}

# function that returns information about the countries and their corresponding
# football leagues that is needed for the "leagues"-table 
get_league_names <- function(url) {
  page <- read_html(url)
  csv_nodes <- html_nodes(page, "a[href$='.csv']")
  csv_links <- html_attr(csv_nodes, "href")
  csv_names <- html_text(csv_nodes)
  csv_div <- sub(".csv", "", sub(".*/.*/", "", csv_links)) 
  country_leagues <- unique(paste(csv_names, csv_div, sep = "_"))
  return(country_leagues)
}


# start of the script
page <- read_html("https://www.football-data.co.uk/data.php")
tbls <- html_nodes(page, "table[cellspacing='2']")%>% 
  html_table(fill = TRUE)
df <- tbls[[1]]
# collect the links to the page of each country
tbl_lnks <- html_nodes(page, "table[cellspacing='2'] a[href]")[1:11] %>% 
  html_attr("href")
df <- cbind(df, tbl_lnks)
# extract country name
key_string <- " Football Results"
df$country <- sub(key_string, "", df[, 2])
df$url <- paste0("https://football-data.co.uk/", df[, 4])
countries <- setNames(df$url, df$country)
# create dataframe "leagues"
leagues <- sapply(countries, get_league_names)
leagues <- unlist(leagues)
names(leagues) <- sub("\\d", "", names(leagues))
leagues <- data.frame(names(leagues), leagues)
leagues$fd_league <- sub(".*_", "", leagues$leagues)
leagues$leagues <- sub("_.*", "", leagues$leagues)
leagues <- leagues %>% rename(country = names.leagues., league = leagues)
leagues <- leagues %>% 
  filter(!(country == "England" & grepl("^Division.*$", league)))
# find all csv files on the pages
csv_links <- sapply(countries, find_csv_links)
csv_links <- sapply(csv_links, function(x) x[!duplicated(x)])
csv_links <- sapply(
  csv_links, function(x) paste0("https://www.football-data.co.uk/", x)
)
csv_links <- Map(
  function(x, y) setNames(x, paste(extract_season(x), sep = " ")), 
  csv_links,  
  names(csv_links)
)
csv_links <- as.list(unlist(csv_links))
# read in all the csv-files. 
# the parsing errors are mostly due to empty columns and rows in the csv files
my_data <- lapply(
  csv_links, read_csv, col_types =
  cols(
    .default = col_double(),
    Div = col_character(),
    Date = col_character(),
    Time = col_time(format = ""),
    HomeTeam = col_character(),
    HT = col_character(),
    AwayTeam = col_character(),
    AT = col_character(),
    FTR = col_character(),
    Res = col_character(),
    HTR = col_character(),
    Referee = col_character()
  )
)
main_leagues <-  rbindlist(
  my_data, use.names = TRUE, idcol = TRUE, fill = TRUE
)
rm(my_data)
# for historical data from Greece from 1999/2000 to 2004/2005 the information 
# about the HomeTeam and the AwayTeam is in the columns "HT" and "AT"
main_leagues$HomeTeam <- ifelse(
  !is.na(main_leagues$HT), main_leagues$HT, main_leagues$HomeTeam
) 
main_leagues$AwayTeam <- ifelse(
  !is.na(main_leagues$AT), main_leagues$AT, main_leagues$AwayTeam
) 
# add columns for season and country
main_leagues <- main_leagues %>% 
  mutate(
    country = sub("\\.\\d{4}", "", .id),
    season = sub("[A-Za-z]+\\.", "", .id)
  )
# delete superfluous columns
main_leagues <- select(main_leagues, -starts_with("X"), -.id)
# delete superflous rows
main_leagues <- filter(main_leagues, !is.na(HomeTeam))
# reorder main leagues data_frame
main_leagues <- select(main_leagues, country, season, Div:AFKC)
# change "Date" to nice date
main_leagues$Date <- dmy(main_leagues$Date)
main_leagues$season <- as.integer(main_leagues$season)

# create sqlite database
conn <- dbConnect(SQLite(), "football.db")
# create and table "countries". the country_id column is only an alias for the 
# rowid and is actually not needed in sqlite 
countries <-  as.data.frame(sort(unique(leagues$country)))
colnames(countries) <- "country"
dbExecute(conn,
  "CREATE TABLE countries (
    country_id INTEGER PRIMARY KEY,
    country TEXT NOT NULL
  );"
)
dbWriteTable(conn, "countries", countries, append = TRUE)
rm(countries)

countries <- dbReadTable(conn, "countries")
leagues <- left_join(leagues, countries, by = "country")
leagues <- select(leagues, country_id, league, fd_league)

# create table leagues
part_stmnt <- col_names_types(leagues)
dbExecute(conn,
  paste0(
    "CREATE TABLE leagues (
      league_id INTEGER PRIMARY KEY, ",
      part_stmnt,
      ",
      FOREIGN KEY (country_id)
        REFERENCES countries (country_id)
    );"
  )
)
dbWriteTable(conn, "leagues", leagues, append = TRUE)

# create and fill table "teams"
dbExecute(conn,
  "CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    team TEXT NOT NULL
  );"
)
teams <- as.data.frame(
  sort(unique(c(main_leagues$HomeTeam, main_leagues$AwayTeam)))
)
names(teams) <- "team"
dbWriteTable(conn, "teams", teams, append = TRUE)

# create and fill table games
leagues <- dbReadTable(conn, "leagues")
main_leagues <- main_leagues %>% 
  rename(fd_league = Div)
main_leagues <- left_join(main_leagues, leagues, by = "fd_league")
main_leagues <- main_leagues %>% 
  select(-country, -country_id, -league, -fd_league) %>% 
  select(league_id, season:AFKC)
teams <- dbReadTable(conn, "teams")
teams <- rename(teams, hometeam_id = team_id, HomeTeam = team)
main_leagues <- left_join(main_leagues, teams, by = "HomeTeam")
teams <- rename(teams, awayteam_id = hometeam_id, AwayTeam = HomeTeam)
main_leagues <- left_join(main_leagues, teams, by = "AwayTeam")
main_leagues <- main_leagues %>% 
  select(-HomeTeam, -AwayTeam) %>% 
  select(league_id:Time, hometeam_id, awayteam_id, FTHG:AFKC)
main_leagues$Date <- as.character(main_leagues$Date)
main_leagues$Time <- as.character(main_leagues$Time)
cols_num <- c(7:8, 10:11, 14:25)
main_leagues[cols_num] <- sapply(main_leagues[cols_num],as.integer)
# renaming several columns because of syntax errors
main_leagues <- rename(main_leagues, ATS = AS, HTS = HS, game_date = Date)
names(main_leagues) <- sub(">", "o", names(main_leagues))
names(main_leagues) <- sub("<", "u", names(main_leagues))
names(main_leagues) <- sub("\\.", "_", names(main_leagues))
# creating part of the query
part_stmnt <- col_names_types(main_leagues)
dbExecute(conn,
  paste0(
    "CREATE TABLE games (
      game_id INTEGER PRIMARY KEY, ",
      part_stmnt,
      ",
      FOREIGN KEY (league_id) REFERENCES leagues (league_id),
      FOREIGN KEY (hometeam_id) REFERENCES teams (team_id),
      FOREIGN KEY (awayteam_id) REFERENCES teams (team_id)
    );"
  )
)
dbWriteTable(conn, "games", main_leagues, append = TRUE)
dbDisconnect(conn)
rm(list = ls())
