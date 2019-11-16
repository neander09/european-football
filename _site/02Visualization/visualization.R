##----------------------------------------------------------------------------##
# Based on the football.db generated in 01Data_Collection this script visualizes
# the average number of goals per game and the home advantage in the major 
# european football leagues
##----------------------------------------------------------------------------##

library(RSQLite)
library(tidyverse)

conn <- dbConnect(
  SQLite(), "football.db"
)

df <- dbGetQuery(conn,
  "SELECT 
    country,
    leagues.fd_league AS Div,
    leagues.league AS league,
    season,
    game_date,
    hometeam.team AS hometeam,
    awayteam.team AS awayteam,
    FTHG, 
    FTAG,
    BbAvo2_5
  FROM
    games
  LEFT JOIN 
    leagues ON games.league_id = leagues.league_id
  LEFT JOIN 
    teams AS hometeam ON hometeam.team_id = games.hometeam_id 
  LEFT JOIN 
    teams AS awayteam ON awayteam.team_id = games.awayteam_id
  LEFT JOIN
    countries ON countries.country_id = leagues.country_id
  WHERE 
    Div IN (
      'B1', 'D1', 'E0', 'F1', 'G1',	'I1', 'N1', 'P1', 'SC0', 'SP1', 'T1'
    );"  
)
dbDisconnect(conn)

df <- df %>% 
  mutate(
    goal_sum = FTHG + FTAG,
    ha_diff = FTHG - FTAG
  ) 

# goals per game in the major european football leagues
goals_per_game <-  df %>% group_by(country, season) %>% 
  summarise(avg_goal_sum = mean(goal_sum, na.rm = TRUE))
ggplot(data = goals_per_game, mapping = aes(x = season, y = avg_goal_sum)) +
  geom_line() +
  facet_wrap(~ country)

# home advantage in the major european football leagues measured as the mean 
# difference between goals shot by home team and away team over a whole season
home_adv <- df %>% 
  group_by(country, season) %>% 
  summarise(home_adv = mean(ha_diff, na.rm = TRUE)) 
ggplot(data = home_adv, mapping = aes(x = season, y = home_adv)) +
  geom_point() + geom_smooth(method = 'lm', se = FALSE) + ylim(-0.3,1) +
  labs(x = "Season", y = "Home Advantage") +
  facet_wrap(~ country)

# average odds for the bet "over 2.5 goals" vs average number of goals per game
# depending on the season and the leagut
gpgame_odds <- df %>% 
  group_by(league, season) %>% 
  summarise(
    avg_goal_sum = mean(goal_sum, na.rm = TRUE),
    avg_over_2_5 = mean(BbAvo2_5, na.rm = TRUE)
  )
ggplot(
  data = gpgame_odds, 
  mapping = aes(x = avg_goal_sum, y = avg_over_2_5, color = league)
) + geom_point() + 
  labs(
    x = "average number of goals", 
    y = 'average odds "Over 2.5 Goals"'
  ) + 
  ggtitle('Average Odds for the bet "Over 2.5 Goals" vs.\n 
          average number of goals per game (2005/06 - 2018/19') +
  theme(
    plot.title = element_text(
      size = 12, hjust = 0.5, vjust = 3, lineheight = 0.6, face = "bold"
    ), 
    plot.margin = unit(c(0.75,0,0,0), "cm")
  )


