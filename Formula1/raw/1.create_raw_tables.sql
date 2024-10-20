-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;


-- COMMAND ----------

Drop table if exists f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId int,
 circuitRef string,
 name string,
 location string,
 country string,
 lat double,
 lng double,
 alt int,
 url string)
 using csv
 options(path "abfss://raw@formula1dl1102.dfs.core.windows.net/circuits.csv", header true);


-- COMMAND ----------



-- COMMAND ----------

select * from f1_raw.circuits;


-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId int,
year int,
round int,
circuitId int,
name string,
date string,
time string,
url string)
USING csv
OPTIONS(path "abfss://raw@formula1dl1102.dfs.core.windows.net/races.csv", header "true");

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constuctors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId int,
constructorRef string,
name string,
nationality string,
url string)
USING json
OPTIONS(path "abfss://raw@formula1dl1102.dfs.core.windows.net/constructors.json");

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId int,
driverRef string,
number int,
code string,
dob date,
name struct<forename:string, surname:string>,
nationality string,
url string)
USING json
OPTIONS(path "abfss://raw@formula1dl1102.dfs.core.windows.net/drivers.json");

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId int,
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points float,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed string,
statusId int)
USING json
OPTIONS(path "abfss://raw@formula1dl1102.dfs.core.windows.net/results.json");

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId int,
driverId int,
stop string,
lap string,
time string,
duration string,
milliseconds int)
USING json
OPTIONS(path "abfss://raw@formula1dl1102.dfs.core.windows.net/pit_stops.json", multiLine True);

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId int,
driverId int,
lap string,
position int,
time string,
milliseconds int)
USING csv
OPTIONS(path "abfss://raw@formula1dl1102.dfs.core.windows.net/lap_times");

-- COMMAND ----------

select count(1) from f1_raw.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId int,
raceId int,
driverId int,
constructorId int,
number int,
position int,
q1 string,
q2 string,
q3 string)
USING json
OPTIONS(path "abfss://raw@formula1dl1102.dfs.core.windows.net/qualifying", multiLine True);

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

describe extended f1_raw.qualifying

-- COMMAND ----------


