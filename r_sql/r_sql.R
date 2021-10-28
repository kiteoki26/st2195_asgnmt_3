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
"SELECT model, AVG(DepDelay) AS 'Avg. Departure Delay'
FROM PLANES p, ONTIME o
WHERE p.tailnum = o.Tailnum
AND DepDelay > 0
AND Cancelled = 0 
AND Diverted = 0
GROUP BY model
ORDER BY AVG(DepDelay) ASC LIMIT 1")

Q1_dplyr <- ontime %>%
  filter(Cancelled==0, Diverted==0, DepDelay>0)%>%
  inner_join(planes, by=c('TailNum' = 'tailnum'))%>%
  group_by(model) %>%
  summarize(avg_DepDelay=mean(DepDelay, na.rm=TRUE))%>%
  arrange(avg_DepDelay)%>%
  top_n(-1)

Q2 <- dbGetQuery(conn,
"SELECT city, COUNT(Dest) AS 'Inbound Flights'
FROM ONTIME o, AIRPORTS a 
WHERE Cancelled = 0
AND o.Dest = a.iata
GROUP BY Dest
ORDER BY COUNT(Dest) DESC LIMIT 1")

Q2_dplyr <- ontime %>%
  filter(Cancelled==0)%>%
  inner_join(airports, by=c('Dest' = 'iata'))%>%
  group_by(Dest)%>%
  summarize(Dest=n())%>%
  top_n(1)

Q3 <- dbGetQuery(conn,
"SELECT Description AS 'Carrier', COUNT(Cancelled) AS 'Cancelled Flights'
FROM CARRIERS c, ONTIME o
WHERE o.UniqueCarrier = c.Code
AND Cancelled = 1
GROUP BY Description
ORDER BY COUNT(Cancelled) DESC LIMIT 1")


Q3_dplyr <- ontime %>%
  filter(Cancelled==1)%>%
  inner_join(carriers, by=c('UniqueCarrier'='Code'))%>%
  group_by(carrier = Description)%>%
  summarize(Cancelled=n())%>%
  arrange(Cancelled)%>%
  top_n(1)

  
Q4 <- dbGetQuery(conn,
"SELECT Description AS 'Carrier', ROUND(SUM(Cancelled)*100.0/ COUNT(UniqueCarrier), 3) AS 'Cancelled Flights to Inbound Flights % Ratio'
FROM ONTIME o, CARRIERS c
WHERE o.UniqueCarrier = c.Code
GROUP BY Description
ORDER BY ROUND(SUM(Cancelled)*100.0/ COUNT(UniqueCarrier), 1) DESC LIMIT 1")


Q4_dplyr <- ontime %>%
  inner_join(carriers, by=c('UniqueCarrier'='Code'))%>%
  group_by(carrier=Description) %>%
  summarize(across(Cancelled=sum()), across(UniqueCarrier=n()))%>%
  mutate(freq_ratio=round(Cancelled/UniqueCarrier*100),3)%>%
  arrange(freq_ratio)%>%
  top_n(10)
  
  
  