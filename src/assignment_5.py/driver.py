from pyspark.sql import SparkSession
from util import (create_employee_df, create_department_df, create_country_df,
                  dynamic_join, update_country_name, column_to_lower)

spark = SparkSession.builder.appName('data').getOrCreate()

employee_df = create_employee_df(spark)
department_df = create_department_df(spark)
country_df = create_country_df(spark)

# 2.Find avg salary of each department
avg_salary = employee_df.groupBy("department").avg("salary")
avg_salary.show()

# 3.Find the employee’s name and department name whose name starts with ‘m’
employee_starts_with_m = employee_df.filter(employee_df["employee_name"].startswith('m'))
starts_with_m = employee_starts_with_m.join(department_df,
                                            employee_starts_with_m["department"] == department_df["dept_id"], "inner") \
    .select(employee_starts_with_m["employee_name"], department_df["dept_name"])
starts_with_m.show()

# 4.Create another new column in employee_df as a bonus by multiplying employee salary * 2
employee_bonus_df = employee_df.withColumn("bonus", col("salary") * 2)
employee_bonus_df.show()

# 5.Reorder the column names of employee_df columns
employee_df = employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department")
employee_df.show()

# 6.Joining DataFrames dynamically
inner_join_df = dynamic_join(employee_df, department_df, "inner")
inner_join_df.show()

left_join_df = dynamic_join(employee_df, department_df, "left")
left_join_df.show()

right_join_df = dynamic_join(employee_df, department_df, "right")
right_join_df.show()

# 7.Derive a new data frame with country_name instead of State in employee_df
country_name_df = update_country_name(employee_df)
country_name_df.show()

# 8.Convert all column names into lowercase and add the load_date column with the current date
lower_case_column_df = column_to_lower(country_name_df)
lower_case_column_df.show()
