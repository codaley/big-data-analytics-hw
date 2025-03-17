# 0. switch to 'Hadoop' admin-type account:
ssh -i ~/.ssh/labsuser.pem hadoop@ec2.compute-1.amazonaws.com






# 1.
# use filezilla to copy investor.txt and stockprices.txt to home/hadoop
# DONE.


# create our new directories
hadoop fs -mkdir PG
hadoop fs -mkdir PG/chex5
hadoop fs -mkdir PG/chex5/input

# copy from "local" hadoop machine, to our new directory
hadoop fs -copyFromLocal investor.txt PG/chex5/input
hadoop fs -copyFromLocal stockprice.txt PG/chex5/input




# check what is in this directory (filezilla level; not sure what this "level" is called)
ls

# check what is in this directory (hadoop level)
hadoop fs -ls

# now load pig
pig





# 2.
# load investor.txt with schema definition
investors = LOAD 'PG/chex5/input/investor.txt' USING PigStorage('\t') AS (investor_id:chararray, first_name:chararray, last_name:chararray, stock_symbol:chararray, quantity:int);

# load stockprice.txt with schema definition
stock_prices = LOAD 'PG/chex5/input/stockprice.txt' USING PigStorage('\t') AS (stock_symbol:chararray, stock_price:float);





# 3.
# display both files
DUMP investors;
DUMP stock_prices;





# 4.
# display structure of relations 'investors' and 'stock_prices'
DESCRIBE investors;
DESCRIBE stock_prices;





# 5.
# perform 'explain'  on 'stock_prices' and observe the logical, physical
# and MapReduce execution plans
EXPLAIN stock_prices;





# 6.
# group 'investors' by their stock symbols that they have purchased and
# display the results
grouped_investors = GROUP investors BY stock_symbol;
DUMP grouped_investors;





# 7.
# combine both relations with a union command and display the results
combined_data = UNION investors, stock_prices;
DUMP combined_data;





