import argparse

from pyspark.sql import SparkSession

def calculate_average_flight_delay(data_source, output_uri,delay_type_string):

    with SparkSession.builder.appName("Year wise "+delay_type_string+" from 2003-2010").getOrCreate() as spark:
        if data_source is not None:
            airline_df = spark.read.option("header", "true").csv(data_source)

        airline_df.createOrReplaceTempView("airline_delay")

        average_flight_delay = spark.sql("""SELECT Year, avg(("""+delay_type_string+"""/ArrDelay)*100) as percentage from airline_delay GROUP BY Year""")

        average_flight_delay.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--delay_type_string', help="The type of the delay")
    parser.add_argument(
        '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    calculate_average_flight_delay(args.data_source, args.output_uri,args.delay_type_string)
			