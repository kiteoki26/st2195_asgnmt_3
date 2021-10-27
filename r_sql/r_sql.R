library(DBI)
library(dplyr)


#Create database and connect
setwd("D:/ST2195/PA3/csv")
conn <- dbConnect(RSQLite::SQLite(), "airline2.db")

#Create data frames
setwd("D:/ST295/PA3/csv/ontime")
csv_ontime <- dir("D:/ST2187/PA3/csv/ontime/")
ontime <- do.call(rbind,lapply(csv_ontime,read.csv))

setwd("D:/ST2195/PA3/csv")
airports <- read.csv("airports.csv", header=TRUE)
carriers <- read.csv("carriers.csv", header=TRUE)
planes <- read.csv("plane-data.csv", header=TRUE)

#Copy data frames and put in database as tables
dbWriteTable(conn, "ONTIME", ontime)
dbWriteTable(conn, "AIRPORTS", airports)
dbWriteTable(conn, "CARRIERS", carriers)
dbWriteTable(conn, "PLANES", planes)

#Queries
Q1 <- dbGetQuery(conn,
"SELECT model, AVG(DepDelay) AS 'Departure Delay'
FROM PLANES p, ONTIME o
WHERE p.tailnum = o.Tailnum
AND DepDelay > 0
AND Cancelled = 0 
AND Diverted = 0
GROUP BY model
ORDER BY AVG(DepDelay) ASC LIMIT 1")

Q2 <- dbGetQuery(conn,
"SELECT Dest as 'City', COUNT(Dest) AS 'Number of Inbound Flights'
FROM ONTIME 
WHERE Cancelled = 0
GROUP BY Dest
ORDER BY COUNT(Dest) DESC LIMIT 1")

Q3 <- dbGetQuery(conn,
"SELECT Description AS 'Carrier', COUNT(Cancelled) AS 'No. of Cancelled Flights'
FROM CARRIERS c, ONTIME o
WHERE o.UniqueCarrier = c.Code
AND Cancelled = 1
GROUP BY Description
ORDER BY COUNT(Cancelled) DESC LIMIT 1")

Q4 <- dbGetQuery(conn,
"SELECT Description AS 'Carrier', ROUND(SUM(Cancelled)*100.0/ COUNT(UniqueCarrier), 1) AS 'Cancelled Flights'
FROM ONTIME o, CARRIERS c
WHERE o.UniqueCarrier = c.Code
GROUP BY Description
ORDER BY ROUND(SUM(Cancelled)*100.0/ COUNT(UniqueCarrier), 1) DESC LIMIT 1")

#Print and export results to .txt file
setwd("D:/st2187/asgnmt3/r_sql")
cat(Q1, Q2, Q3, Q4, file="query_results.txt", sep="\n")


  