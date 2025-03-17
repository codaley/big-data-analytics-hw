## delete db
drop database retaildb;

################### < NUMBER 1 > ###################
# Log in to MySQL, create the database, and insert the records into the two tables of this database.

# code for logging into EMR
ssh -i ~/.ssh/labsuser.pem hadoop@ec2.compute-1.amazonaws.com

# code for logging into mysql
[hadoop@ip ~]> mysql -u admin -h retaildb.us-east-1.rds.amazonaws.com -p
# enter pw

# code for creating a new database called "retaildb"
create database retaildb;
use retaildb;

# code for creating table "customerDetails"
create table customerDetails (
	ID INT,
	customerName VARCHAR(255),
	customerStreet VARCHAR(255),
  	customerCity VARCHAR(255),
  	customerState VARCHAR(255),
  	customerZip VARCHAR(255),
  	customerBirthYear INT,
  	customerSex CHAR(1)
);

# code for creating table "latePayments"
create table latePayments (
  	ID INT,
  	paymentID VARCHAR(255),
  	paymentDueDate VARCHAR(255),
  	paymentIsLate BOOLEAN
);

# code for inserting data into table "customerDetails"
insert into customerDetails values ('171', 'Madelyn Hensley', '10075 Thierer Plaza', 'New York', 'New York', '81377', '1976', 'M');
insert into customerDetails values ('172', 'Lonny Foster', '23901 Park Meadow Dr', 'Austin', 'Texas', '13498', '1981', 'F');
insert into customerDetails values ('173', 'Karina Livingston', '95 Anderson Park', 'Chattanooga', 'Tennessee', '94518', '1977', 'F');
insert into customerDetails values ('174', 'Avery Mccormick', '09992 Sunfield Parkway', 'Chicago', 'Illinois', '38300', '1992', 'F');
insert into customerDetails values ('175', 'Peter King', '0486 Dryden Road', 'Chicago', 'Illinois', '21012', '1991', 'M');
insert into customerDetails values ('176', 'Bret Ibarra', '14 Transport Place', 'San Diego', 'California', '12865', '1984', 'M');
insert into customerDetails values ('177', 'Leonardo Wheeler', '806 Corry Crossing', 'New York', 'New York', '44464', '1972', 'F');
insert into customerDetails values ('178', 'Bennett Noble', '8 South Terrace', 'Hixson', 'Tennessee', '52890', '1989', 'F');
insert into customerDetails values ('179', 'Marcia Matthews', '0380 Knutson Road', 'Dallas', 'Texas', '80477', '1967', 'F');
insert into customerDetails values ('180', 'Avis Kramer', '49624 Hanover Junction', 'New York', 'New York', '62542', '1965', 'M');
insert into customerDetails values ('181', 'Lynnette Tate', '30741 Paget Court', 'New York', 'New York', '34886', '1987', 'M');
insert into customerDetails values ('182', 'Lakisha Estrada', '50 Dahle Crossing', 'Dallas', 'Texas', '16042', '1991', 'F');
insert into customerDetails values ('183', 'Bill Silva', '4 Mcbride Crossing', 'Detroit', 'Michigan', '15871', '1968', 'M'); 

# code for inserting data into table "latePayments"
insert into latePayments values ('181', '146268743-8', '23-8-2020', TRUE);
insert into latePayments values ('172', '396589804-7', '28-9-2020', FALSE);
insert into latePayments values ('183', '553753031-8', '25-9-2020', FALSE);
insert into latePayments values ('183', '559786593-4', '13-12-2018', TRUE);
insert into latePayments values ('175', '108659198-9', '13-9-2016', TRUE);
insert into latePayments values ('176', '360007723-2', '27-9-2016', FALSE);
insert into latePayments values ('177', '238309554-X', '28-11-2014', FALSE);
insert into latePayments values ('178', '694690715-8', '31-5-2014', TRUE);
insert into latePayments values ('179', '318241713-5', '15-7-2016', TRUE);
insert into latePayments values ('180', '84360172-2', '6-7-2016', FALSE);
insert into latePayments values ('181', '807633856-8', '5-7-2018', FALSE);
insert into latePayments values ('182', '845886260-4', '23-4-2014', FALSE);
insert into latePayments values ('183', '270161074-X', '19-7-2016', TRUE);
insert into latePayments values ('171', '887428332-0', '4-7-2020', FALSE);
insert into latePayments values ('172', '401129380-4', '23-8-2015', TRUE);
insert into latePayments values ('173', '277406266-7', '27-12-2019', FALSE);
insert into latePayments values ('174', '851112155-2', '4-12-2019', TRUE);


################### < NUMBER 2 > ###################
# Verify that both tables are created correctly and contain records.

# code for showing that both tables exist
show tables;

# code for verifying the data is actually in the tables
select * from latePayments;
select * from customerDetails;

# quit mysql and go back to hadoop
quit;
clear

################### < NUMBER 3 > ###################
# Load the data from these two tables into HDFS using the Sqoop import command.

# download "mysql-connector-java" to connect java db to mysql in hadoop & rds
hadoop> wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.3.0-1.el9.noarch.rpm

# install it
hadoop> sudo rpm -i ./mysql-connector-j-8.3.0-1.el9.noarch.rpm

# runs the map reduce job in scoop for table 1 "customerDetails"
hadoop> sqoop import --connect jdbc:mysql://retaildb.us-east-1.rds.amazonaws.com/retaildb --table customerDetails --username admin --  --target-dir SQP/input/sqoop1 -m 1

# do the same thing for table 2 "latePayments"
hadoop> sqoop import --connect jdbc:mysql://retaildb.us-east-1.rds.amazonaws.com/retaildb --table latePayments --username admin --  --target-dir SQP/input/sqoop2 -m 1

# then run these to print the data to the terminal (this proves that the tables were moved from rdbms/mysql to hdfs/hadoop)
`
hadoop fs -cat SQP/input/sqoop1/*
hadoop fs -cat SQP/input/sqoop2/*
`

################### < NUMBER 4 > ###################
# Using Hive, create a similar database and create and populate both tables.

#4. Using Hive, create a similar database and create and populate both tables (Tutorial B: Part 1)

hadoop fs -mkdir HV
hadoop fs -mkdir HV/input
hadoop fs -mkdir HV/output

hive
create database retail_H;
quit;

# copy table 1 "retaildb" from mysql to hive using sqoop - name it "customers_H"
sqoop import --connect jdbc:mysql://retaildb.us-east-1.rds.amazonaws.com/retaildb --username admin -P --table customerDetails --target-dir HV/input/customers --fields-terminated-by "," --split-by "ID" --hive-import --create-hive-table --hive-table retail_H.customers_H

# copy table 2 "latePayments" from mysql to hive using sqoop - name it "payments_H"
sqoop import --connect jdbc:mysql://retaildb.us-east-1.rds.amazonaws.com/retaildb --username admin -P --table latePayments --target-dir HV/input/payments --fields-terminated-by "," --split-by "ID" --hive-import --create-hive-table --hive-table retail_H.payments_H



################### < NUMBER 5 > ###################
# Perform the following queries and store the results in HDFS:

# create directory in HDFS/hadoop to store the results of your queries
hadoop fs -mkdir HV/output/fromHive5A
hadoop fs -mkdir HV/output/fromHive5B

# invoke hive and access the retail_H db
hive
use retail_H;

# a. Display customers (ID, name, year of birth, and zip code) from the state of New York
# store the results in HDFS
hive> insert overwrite directory 'HV/output/fromHive5A'
row format delimited fields terminated by ','
select * from customers_H where customerState like 'New York';


# b. Display the total number of customers who have late payments for each zip code.
# store the results in HDFS
hive> insert overwrite directory 'HV/output/fromHive5B'
row format delimited fields terminated by ','
select customerZip, count(customers_H.ID) from customers_H
left join payments_H on customers_H.ID = payments_H.ID
where paymentIsLate = TRUE
group by customerZip;

hive> quit;

# verify that the records are stored
`
hadoop fs -cat HV/output/fromHive5A/*
hadoop fs -cat HV/output/fromHive5B/*
`
################### < NUMBER 6 > ###################
# Move the results of the queries in 5. above from HDFS into the bank_db database in MySQL using the Sqoop export

# log into mysql to create database
mysql -u admin -h retaildb.us-east-1.rds.amazonaws.com -p

# code for creating a new database called "bank_db"
create database bank_db;


# create the table hive_results5A
use bank_db;

create table hive_results5A (
    customerBirthYear INT,
	customerCity VARCHAR(255),
	customerName VARCHAR(255),
	customerSex CHAR(1),
	customerState VARCHAR(255),
	customerStreet VARCHAR(255),
	customerZip VARCHAR(255),
  	ID INT
);


# create the table hive_results5B

create table hive_results5B (
    customerZip VARCHAR(255),
    latePaymentsPerZip INT
);

quit;

hadoop>	sqoop export --connect jdbc:mysql://retaildb.us-east-1.rds.amazonaws.com/bank_db --table hive_results5A --username admin -P --export-dir HV/output/fromHive5A/* -m 1
hadoop>	sqoop export --connect jdbc:mysql://retaildb.us-east-1.rds.amazonaws.com/bank_db --table hive_results5B --username admin -P --export-dir HV/output/fromHive5B/* -m 1


################### < NUMBER 7 > ###################
# Perform a select-all to verify that the results were exported correctly.

# log back into mysql to veryfy data
mysql -u admin -h retaildb.us-east-1.rds.amazonaws.com -p

use bank_db;

select * from hive_results5A;
select * from hive_results5B;




