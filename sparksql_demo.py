import sys
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

if __name__ == "__main__":

    sc = SparkContext(appName="SparkSqlDemo")
    sqlContext = SQLContext(sc)

    # Load a text file and convert each line to a dictionary.
    lines = sc.textFile("matchmaker_no_header.csv")
    parts = lines.map(lambda l: l.split(","))

    # define the column names of the table
    people = parts.map(lambda p: Row(male_age=int(p[0]), 
                                     male_smoker=p[1],
                                     male_wants_children=p[2],
                                     male_interests=p[3],
                                     male_address=p[4]))
	
    # Infer the schema, and register the SchemaRDD as a table.
    schemaPeople = sqlContext.inferSchema(people)
    schemaPeople.registerTempTable("people")

    # SQL can be run over SchemaRDDs that have been registered as a table.
    older_smokers = sqlContext.sql("SELECT male_age, male_address, male_interests \
                                    FROM people \
                                    WHERE male_smoker = 'yes' \
                                    AND male_age >= 45 \
                                    ORDER BY male_age")
    
    for smoker in older_smokers.collect():
        print smoker

    # The results of SQL queries are RDDs and support all the normal RDD operations.
    #older_smokers2 = older_smokers.map(lambda p: "age: " + str(p.male_age) + " adrs: " + p.male_address)
    #for smoker in older_smokers.collect():
    #    print smoker
