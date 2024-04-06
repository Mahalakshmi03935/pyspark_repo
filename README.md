# pyspark_repo - This Repo consists of Pyspark Programs
**Question 1:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:
countDistinct(): Counts unique items in a dataset.
filter(): Selects specific rows based on certain conditions.
intersect(): Finds common elements between two datasets.
groupBy() and agg(): Used to group data and perform aggregate operations like counting.
show(): Displays the data in a clear format.

**Question 2:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:


createDataFrame(): Creates a DataFrame from provided data and schema.
show(): Displays the contents of a DataFrame.
read.csv(): Reads a CSV file into a DataFrame, inferring schema automatically.
printSchema(): Prints the schema of a DataFrame.
repartition(): Changes the number of partitions in a DataFrame.
coalesce(): Decreases the number of partitions in a DataFrame.
udf(): Registers a user-defined function.
withColumn(): Adds a new column to a DataFrame using specified expressions.

**Question 3:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:

createDataFrame(): Creates a DataFrame from provided data and schema.
show(): Displays the contents of a DataFrame.
to_date(): Converts a string column to a DateType column.
current_date(): Returns the current date.
date_sub(): Subtracts a specified number of days from a date.
col(): Allows referencing a DataFrame column by name.
groupBy() and count(): Used to group data and perform counting operations.

**Question 4:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:

read.json(): Reads a JSON file into a DataFrame.
select(): Selects specific columns from a DataFrame.
explode(), posexplode(), explode_outer(), posexplode_outer(): Used to handle arrays by exploding them into separate rows.
withColumnRenamed(): Renames columns in a DataFrame.
current_date(), year(), month(), day(): Functions for date manipulation.

**Question 5:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:

createDataFrame(): Creates a DataFrame from provided data and schema.
show(): Displays the contents of a DataFrame.
groupBy() and avg(): Used to group data and calculate aggregate functions like average.
filter(): Filters rows in a DataFrame based on a condition.
withColumn(): Adds a new column to a DataFrame using specified expressions.
select(): Selects specific columns from a DataFrame.
join(): Joins two DataFrames based on a condition.
when(): Provides conditional logic within DataFrame transformations.
withColumnRenamed(): Renames columns in a DataFrame.
current_date(): Returns the current date.
