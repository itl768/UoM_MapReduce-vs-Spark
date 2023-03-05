DROP TABLE IF EXISTS inputDataSet;

CREATE EXTERNAL TABLE inputDataSet (
`Id` int,
  `Year` string,
  `Month` string,
  `DayofMonth` string,
  `DayOfWeek` string,
  `DepTime` string,
  `CRSDepTime` string,
  `ArrTime` string,
  `CRSArrTime` string,
  `UniqueCarrier` string,
  `FlightNum` string,
  `TailNum` string,
  `ActualElapsedTime` string,
  `CRSElapsedTime` string,
  `AirTime` string,
  `ArrDelay` int,
  `DepDelay` string,
  `Origin` string,
  `Dest` string,
  `Distance` string,
  `TaxiIn` string,
  `TaxiOut` string,
  `Cancelled` string,
  `CancellationCode` string,
  `Diverted` string,
  `CarrierDelay` int,
  `WeatherDelay` string,
  `NASDelay` string,
  `SecurityDelay` string,
  `LateAircraftDelay` string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION "${INPUT}";
INSERT OVERWRITE DIRECTORY "${OUTPUT}"
SELECT Year, avg((CarrierDelay/ArrDelay)*100) as percentage from inputDataSet GROUP BY Year;
