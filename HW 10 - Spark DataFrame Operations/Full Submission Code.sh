# code for logging into EMR
ssh -i ~/.ssh/labsuser.pem hadoop@ec2.compute-1.amazonaws.com


# 1.Create a subdirectory SPRK/Ex2 in HDFS and upload the d311.csv file to that subdirectory.

# create subdirectory
hadoop fs -mkdir SPRK
hadoop fs -mkdir SPRK/Ex2

# move d311.csv to this new directory
hadoop fs -copyFromLocal d311.csv SPRK/Ex2

# check with ls to see if file is in place
hadoop fs -ls SPRK/Ex2



# 2.Start the Spark shell and read the d311.csv file. View the schema and note that the column names match the record field names in the CSV file. Provide a screenshot of the schema.

# start SPRK shell
spark-shell

# read d311.csv into a dataframe
val d311DF=spark.read.format("csv").option("header", "true").load("SPRK/Ex2/d311.csv")

# show schema
d311DF.printSchema()


# 3.Display the data in the DataFrame using the show function. How many records are displayed? Display the first 5 records of the DataFrame. Provide a screenshot of the result.

# display the data using show
d311DF.show()

# display the first 5 records
d311DF.show(5)



# 4.Use the count action to return the number of items in the DataFrame. Provide a screenshot of the result.

d311DF.count




# 5.Use a select transformation to return a DataFrame with only the Created Date, Agency, Complaint Type and City. The select transformation should return all columns with an alias instead of the real name. Display the schema of the new DataFrame. Provide a screenshot of the result.

# query
val d311Lite=d311DF.select(
  $"Created Date".alias("Date of Creation"),
  $"Agency".alias("Agency Name"),
  $"Complaint Type".alias("Type of Complaint"),
  $"City".alias("City Name")
)

# see the results of query
d311Lite.show()

# show schema
d311Lite.printSchema()



# 6.Write a query (a series of one or more transformations followed by an action) that displays the first 20 lines of Agency, City, Complaint Type, where City is not null. Provide a screenshot of the result.

 // filter before select as its better for performance
val simpleQuery20=d311DF.filter($"City".isNotNull).select($"Agency", $"City", $"Complaint Type")

# display the first 20 records
simpleQuery20.show()