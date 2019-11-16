# This script would collect historical market values of Bundesliga clubs from
# transfermarkt.de. By changing the url, you could change the league from which
# to collect the market values. Please scrape responsibly!

library(dplyr)
library(lubridate)
library(readr)
library(rvest)

# function collects a table with market values for a specified date
get_table <- function(date) {
  url_tbls <- paste0(
    url,
    "/plus/?stichtag=",
    date
  )
  datum <- date
  webpage <- read_html(url_tbls)
  table <- html_nodes(webpage, 'table.items')[[1]]
  table <- html_table(table, fill = TRUE)
  table <- table[-1, ]
  table <- table[, c(3, 5)]
  table <- cbind(table, datum)
  names(table) <- c("team", "market_value", "datum")
  table$datum <- as.Date(table$datum)
  return(table)
}

url <- "https://www.transfermarkt.de/1-bundesliga/marktwerteverein/wettbewerb/L1"
liga <- sub("^.*/", "", url)
webpage <- read_html(url)
date_options <- html_nodes(webpage, 'select[name="stichtag"]')
date_options <- html_nodes(date_options, 'option')
date_options <- unlist(lapply(date_options, html_text))
date_options <- dmy(date_options)
date_options <- as.character(date_options)

# 'looping' over the date_options vector for which market values are 
# available. After creating a list of data frames these tables are combined in a
# single df
my_data <- lapply(date_options, get_table)
market_values_liga <- bind_rows(my_data)

filepath <- paste0(liga, ".csv")
write_csv2(market_values_liga, filepath)





