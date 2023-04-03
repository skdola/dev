%python
# File location and name
#teams
#seasons
#appearences
#matches
#standings
file_location = "/FileStore/tables/standings.csv"
table_name = "standings"

# CSV options
file_type = "csv"
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#display(df)

df.createOrReplaceTempView(table_name)

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

df.write.format("parquet").mode("overwrite").saveAsTable(table_name)
