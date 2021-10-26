# Practice Assignment 3 #

Construct an SQLite Database called airline2.db with the following tables: 
1. ONTIME 
2. AIRPORTS
3. CARRIERS
4. PLANES 
Source: Harvard Datavers [Data Expo 2009](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7)

## R Code ##
1. Create a folder called "r_sql" with a R code for constructing the database. 
2. Replicate queries Q1 to Q4. (Should be done using DBI and plyr notation)

## Python ## 
1. Create a python version of the code, based on sqlite2. 

## Queries ##
- Q1 - Which plane model has the lowest associated average departure delay
(excluding cancelled and diverted flights)? 
- Q2 - Which city has the highest number of inbound flights (excluding
cancelled flights)? 
- Q3 - Which carrier has the highest number of cancelled flights? 
- Q4 - Which carrier has the highest number of cancelled flights, relative to
their number of total flights?
- Create a simplified solution for query in Q4 in either R or Python. 

