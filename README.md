# pyspark_repo - This Repo consists of Pyspark Programs
**Question 1:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:
1.countDistinct(): Counts unique items in a dataset.

2.filter(): Selects specific rows based on certain conditions.

3.intersect(): Finds common elements between two datasets.

4.groupBy() and agg(): Used to group data and perform aggregate operations like counting.

5.show(): Displays the data in a clear format.

**Question 2:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:

1.createDataFrame(): Creates a DataFrame from provided data and schema.

2.show(): Displays the contents of a DataFrame.

3.read.csv(): Reads a CSV file into a DataFrame, inferring schema automatically.

4.printSchema(): Prints the schema of a DataFrame.

5.repartition(): Changes the number of partitions in a DataFrame.

6.coalesce(): Decreases the number of partitions in a DataFrame.

7.udf(): Registers a user-defined function.

8.withColumn(): Adds a new column to a DataFrame using specified expressions.

**Question 3:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:

1.createDataFrame(): Creates a DataFrame from provided data and schema.

2.show(): Displays the contents of a DataFrame.

3.to_date(): Converts a string column to a DateType column.

4.current_date(): Returns the current date.

5.date_sub(): Subtracts a specified number of days from a date.

6.col(): Allows referencing a DataFrame column by name.

7.groupBy() and count(): Used to group data and perform counting operations.

**Question 4:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:

1.read.json(): Reads a JSON file into a DataFrame.

2.select(): Selects specific columns from a DataFrame.

3.explode(), posexplode(), explode_outer(), posexplode_outer(): Used to handle arrays by exploding them into separate rows.

4.withColumnRenamed(): Renames columns in a DataFrame.

5.current_date(), year(), month(), day(): Functions for date manipulation.

**Question 5:**

Libraries Used:

PySpark: This library allows us to process large datasets efficiently using distributed computing.

Functions Used:

1.createDataFrame(): Creates a DataFrame from provided data and schema.

2.show(): Displays the contents of a DataFrame.

3.groupBy() and avg(): Used to group data and calculate aggregate functions like average.

4.filter(): Filters rows in a DataFrame based on a condition.

5.withColumn(): Adds a new column to a DataFrame using specified expressions.

6.select(): Selects specific columns from a DataFrame.

7.join(): Joins two DataFrames based on a condition.

8.when(): Provides conditional logic within DataFrame transformations.

9.withColumnRenamed(): Renames columns in a DataFrame.

10.current_date(): Returns the current date.
