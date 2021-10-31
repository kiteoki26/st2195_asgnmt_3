import sqlite3
import csv

 

#Create ontime table
def ontime():
    conn=sqlite3.connect("airline2.db")
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS ontime(id INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER, month INTEGER, dayOfmonth INTEGER, dayOfweek INTEGER, DepTime INTEGER, CRSDepTime INTEGER, ArrTime INTEGER, CRSArrTime INTEGER, UniqueCarrier TEXT, FlightNum INTEGER, TailNum VARCHAR(10), ActualElapsedTime INTEGER, CRSElapsedTime INTEGER, AirTime INTEGER, ArrDelay REAL, DepDelay REAL, origin TEXT, dest TEXT, distance INTEGER, TaxiIn INTEGER, TaxiOut INTEGER, Cancelled VARCHAR(1), CancellationCode INTEGER, Diverted VARCHAR(1), CarrierDelay INTEGER, WeatherDelay INTEGER, NASDelay INTEGER, SecurityDelay INTEGER, LateAircraftDelay INTEGER)")
    
    #Import csv
    csv_dir=('D:/ST2187/PA3/csv/ontime/')
    df1=(csv.reader(open(csv_dir+'2000.csv')))
    next(df1)
    df2=(csv.reader(open(csv_dir+'2001.csv')))
    next(df2)
    df3=(csv.reader(open(csv_dir+'2002.csv')))
    next(df3)
    df4=(csv.reader(open(csv_dir+'2003.csv')))
    next(df4)
    df5=(csv.reader(open(csv_dir+'2004.csv')))
    next(df5)
    df6=(csv.reader(open(csv_dir+'2005.csv')))
    next(df6)
    csv_list=[df1,df2,df3,df4,df5,df6]
    insert="INSERT INTO ontime VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    for x in csv_list:
        cur.executemany(insert,x)

    conn.commit()
    conn.close()
    

#Create airports table
def airports():
    conn=sqlite3.connect("airline2.db")
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS airports(iata TEXT PRIMARY KEY, airport TEXT, city TEXT, state TEXT, country TEXT, lat REAL, long REAL)")
    
    #Import csv
    contents=(csv.reader(open('D:/ST2187/PA3/csv/airports.csv')))
    next(contents)
    insert="INSERT INTO airports VALUES (?, ?, ?, ?, ?, ?, ?)"
    cur.executemany(insert, contents)
    
    conn.commit()
    conn.close()

 
#Create planes table
def planes():
    conn=sqlite3.connect("airline2.db")
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS planes(tailnum TEXT, type TEXT, manufacturer TEXT, issue_date DATE, model TEXT, status TEXT, aircraft_type TEXT, engine_type TEXT, year INTEGER)")
    #Import csv
    contents=(csv.reader(open('D:/ST2187/PA3/csv/plane-data.csv')))
    next(contents)
    insert="INSERT INTO planes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cur.executemany(insert, contents)
    
    conn.commit()
    conn.close()
    

#Create carriers table
def carriers():
    conn=sqlite3.connect("airline2.db")
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS carriers(code TEXT PRIMARY KEY, description TEXT)")
    #Import csv
    contents=(csv.reader(open('D:/ST2187/PA3/csv/carriers.csv')))
    next(contents)
    insert="INSERT INTO carriers VALUES (?, ?)"
    cur.executemany(insert, contents)
    
    conn.commit()
    conn.close()
    
    
    
def queries():
    q1="SELECT model, AVG(DepDelay) AS 'Avg. Departure Delay' FROM PLANES p, ONTIME o WHERE p.tailnum = o.Tailnum AND DepDelay > 0 AND Cancelled = 0 AND Diverted = 0 GROUP BY model ORDER BY AVG(DepDelay) ASC LIMIT 1"
    q2="SELECT city, COUNT(Dest) AS 'Inbound Flights' FROM ONTIME o, AIRPORTS a WHERE Cancelled = 0 AND o.Dest = a.iata GROUP BY Dest ORDER BY COUNT(Dest) DESC LIMIT 1"
    q3="SELECT Description AS 'Carrier', COUNT(Cancelled) AS 'Cancelled Flights' FROM CARRIERS c, ONTIME o WHERE o.UniqueCarrier = c.Code AND Cancelled = 1 GROUP BY Description ORDER BY COUNT(Cancelled) DESC LIMIT 1"
    q4="SELECT Description AS 'Carrier', ROUND(SUM(Cancelled)*100.0/ COUNT(UniqueCarrier), 3) AS 'Cancelled Flights to Inbound Flights % Ratio' FROM ONTIME o, CARRIERS c WHERE o.UniqueCarrier = c.Code GROUP BY Description ORDER BY ROUND(SUM(Cancelled)*100.0/ COUNT(UniqueCarrier), 1) DESC LIMIT 1"
    conn=sqlite3("airline2.db")
    cur=conn.cursor()
    a1=cur.execute(q1).fetchall()
    a2=cur.execute(q2).fetchall()
    a3=cur.execute(q3).fetchall()
    a4=cur.execute(q4).fetchall()
    rows = [a1,a2,a3,a4]
    for r in rows:
        for a in r:
            print(a)
    
    conn.commit()
    conn.close()

    
def run():
    ontime()
    carriers()
    planes()
    airports()
    queries()
    
run()
    
    
    
