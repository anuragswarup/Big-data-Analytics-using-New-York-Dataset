from pyspark.sql.types import StructType, StructField, StringType,IntegerType,FloatType,DoubleType
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
import pyspark.pandas as pd
from pyspark.sql.window import Window


#Create a SparkSession

#spark = SparkSession.builder.appName("Anurag Big Data Project").getOrCreate()
sc=pyspark.SparkContext()
sqlContext=SQLContext(sc)

############################################################ DATA LOADING SECTION ###########################################################

# importing all the required  datasets from the shared folders.


##################### import crime data of ny

crime_chunk1=sc.textFile("file:///shared/chunk_1final.csv").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),str(x[1]),int(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(x[11]),str(x[12])))

crime_chunk2=sc.textFile("file:///shared/chunk_2final.csv").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),str(x[1]),int(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(x[11]),str(x[12])))

crime_chunk3=sc.textFile("file:///shared/chunk_3final.csv").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),str(x[1]),int(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(x[11]),str(x[12])))

crime_chunk4=sc.textFile("file:///shared/chunk_4final.csv").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),str(x[1]),int(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(x[11]),str(x[12])))

crime_chunk5=sc.textFile("file:///shared/chunk_5final.csv").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),str(x[1]),int(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(x[11]),str(x[12])))

crime_chunk6=sc.textFile("file:///shared/chunk_6final.csv").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),str(x[1]),int(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(x[11]),str(x[12])))

crime_chunk7=sc.textFile("file:///shared/chunk_7final.csv").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),str(x[1]),int(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(x[11]),str(x[12])))

crime_chunk8=sc.textFile("file:///shared/chunk_8final.csv").map(lambda x:x.split(',')).map(lambda x:(int(x[0]),str(x[1]),int(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(x[11]),str(x[12])))


schema = StructType([StructField("COMPLAINT_NO", IntegerType(), True),
StructField("DATE", StringType(), True),
StructField("YEAR", IntegerType(), True),
StructField("OFS_DESC", StringType(), True),
StructField("CRM_ATPT_CPTD_CD", StringType(), True),
StructField("LAW_CAT_CD", StringType(), True),
StructField("BORO_NM", StringType(), True),
StructField("PREM_TYP", StringType(), True),
StructField("SUSP_RACE", StringType(), True),
StructField("SUSP_SEX", StringType(), True),
StructField("VIC_AGE", StringType(), True),
StructField("VIC_RACE", StringType(), True),
StructField("VIC_SEX", StringType(), True)])

df_1=sqlContext.createDataFrame(crime_chunk1,schema)
df_2=sqlContext.createDataFrame(crime_chunk2)
df_3=sqlContext.createDataFrame(crime_chunk3)
df_4=sqlContext.createDataFrame(crime_chunk4)
df_5=sqlContext.createDataFrame(crime_chunk5)
df_6=sqlContext.createDataFrame(crime_chunk6)
df_7=sqlContext.createDataFrame(crime_chunk7)
df_8=sqlContext.createDataFrame(crime_chunk8)

ny_crime_data=df_1.union(df_2)
ny_crime_data=ny_crime_data.union(df_3)
ny_crime_data=ny_crime_data.union(df_4)
ny_crime_data=ny_crime_data.union(df_5)
ny_crime_data=ny_crime_data.union(df_6)
ny_crime_data=ny_crime_data.union(df_7)
ny_crime_data=ny_crime_data.union(df_8)


##################### import collision data
collision_data=sc.textFile("file:///shared/collisions.csv").map(lambda x:x.split(',')).map(lambda x:(str(x[0]),int(x[1]),float(x[2]),str(x[3]),int(x[4]),int(x[5]),str(x[6]),int(x[7]),str(x[8])))


schema_collisions = StructType([StructField("CRASH_DATE", StringType(), True),
StructField("CRASH YEAR", IntegerType(), True),
StructField("CRASH TIME", FloatType(), True),
StructField("BOROUGH", StringType(), True),
StructField("INJURED COUNT", IntegerType(), True),
StructField("KILLED COUNT", IntegerType(), True),
StructField("FACTOR", StringType(), True),
StructField("COLLISION_ID", IntegerType(), True),
StructField("VECHILE_TYPE", StringType(), True)])

collision_df=sqlContext.createDataFrame(collision_data,schema_collisions)


##################### import house sales data

house_sales_data=sc.textFile("file:///shared/House_Sales.csv").map(lambda x:x.split(',')).map(lambda x:(str(x[0]),str(x[1]),str(x[2]),int(x[3]),int(x[4]),int(x[5]),int(x[6]),int(x[7]),int(x[8])))

schema_house_sales = StructType([StructField("BOROUGH", StringType(), True),
StructField("NEIGHBORHOOD", StringType(), True),
StructField("TYPE_OF_HOME", StringType(), True),
StructField("NUMBER_OF_SALES", IntegerType(), True),
StructField("LOWEST_SALE_PRICE", IntegerType(), True),
StructField("AVERAGE_SALE_PRICE", IntegerType(), True),
StructField("MEDIAN_SALE_PRICE", IntegerType(), True),
StructField("HIGHEST_SALE_PRICE", IntegerType(), True),
StructField("YEAR", IntegerType(), True)])

house_sales_df=sqlContext.createDataFrame(house_sales_data,schema_house_sales)


##################### import population data

population_data=sc.textFile("file:///shared/population.csv").map(lambda x:x.split(',')).map(lambda x:(str(x[0]),int(x[1]),int(x[2]),float(x[3])))
schema_population = StructType([StructField("BOROUGH", StringType(), True),
StructField("YEAR", IntegerType(), True),
StructField("Total Population", IntegerType(), True),
StructField("% of share of NYC", FloatType(), True)])

population_df=sqlContext.createDataFrame(population_data,schema_population)


######################################################### FINDING BUSINESS ANSWERS ##############################################################

#1.What are the most expensive neighborhoods in New York City, and do these neighborhoods have higher or lower crime rates? (home_sales + crime)

average=house_sales_df.groupBy().avg("AVERAGE_SALE_PRICE").collect()[0][0]
result_set=house_sales_df.filter(house_sales_df['AVERAGE_SALE_PRICE']> average).select('borough','number_of_sales')
most_expensive_neighborhood= result_set.groupBy("BOROUGH").sum("NUMBER_OF_SALES").alias("NUMBER OF HOUSES")
most_expensive_neighborhood=most_expensive_neighborhood.orderBy(desc("sum(NUMBER_OF_SALES)"))

crime_rate=ny_crime_data.groupBy("BORO_NM").count().orderBy("count")
nyc_expensive_crime=most_expensive_neighborhood.join(crime_rate,most_expensive_neighborhood["BOROUGH"]==crime_rate["BORO_NM"])


nyc_expensive_crime=nyc_expensive_crime.orderBy("count")
business_answer1=nyc_expensive_crime.withColumnRenamed("sum(NUMBER_OF_SALES)","HOUSE_SALE_COUNT").withColumnRenamed("count","CRIME_CASES_COUNT")

population_yearwise=population_df.filter(population_df["YEAR"]=="2020").select ("borough","% of share of NYC")
business_answer1=business_answer1.join(population_yearwise,business_answer1["BOROUGH"]==population_yearwise["borough"])
business_answer1=business_answer1.orderBy("CRIME_CASES_COUNT")
column_to_remove=business_answer1.columns[-2]
business_answer1=business_answer1.drop(column_to_remove)

#mode("overwrite")
selected_columns=["BORO_NM","HOUSE_SALE_COUNT","CRIME_CASES_COUNT","% OF SHARE OF NYC"]
selected_business_answer1=business_answer1.select(selected_columns)
selected_business_answer1.show()
selected_business_answer1.write.format("csv").option("header","true").mode("overwrite").save("/home/anurag/Desktop/output/answer1.csv")

print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
print("^^^^^^^^^^^^^^^^^^ FILE GENERATED SUCESSFULLY ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")


#2. Which types of crimes are most commonly reported in each borough, and are there any patterns or trends that can be identified?

most_reported_crime_per_borough = ny_crime_data.filter((ny_crime_data.BORO_NM.isNotNull()) & (ny_crime_data.OFS_DESC.isNotNull()))
most_reported_crime_per_borough=most_reported_crime_per_borough.groupBy("BORO_NM","OFS_DESC").count().orderBy("count",ascending=False)

window_spec = Window.partitionBy("BORO_NM").orderBy(desc("count"))
ranked_df = most_reported_crime_per_borough.select("*", dense_rank().over(window_spec).alias("rank"))

# Filter for only the top offense in each borough
result_df = ranked_df.filter(ranked_df.rank == 1)

# Select the relevant columns
crime_df = ny_crime_data.select("BORO_NM", "OFS_DESC")

# Filter out null values in BORO_NM and OFNS_DESC
crime_df = crime_df.filter((crime_df.BORO_NM.isNotNull()) & (crime_df.OFS_DESC.isNotNull()))

# Group by borough and offense description, and count the number of occurrences
grouped_df = crime_df.groupBy("BORO_NM", "OFS_DESC").agg(count("*").alias("crime_count"))

# Rank the offenses in each borough by count
window_spec = Window.partitionBy("BORO_NM").orderBy(desc("crime_count"))
ranked_df = grouped_df.select("*", dense_rank().over(window_spec).alias("rank"))

# Filter for only the top offense in each borough
result_df = ranked_df.filter(ranked_df.rank == 1)
filtered_df=result_df.filter(result_df.BORO_NM.isin("BRONX","BROOKLYN","MANHATTAN","QUEENS","STATEN ISLAND")).orderBy(desc("crime_count"))
# Show the result
filtered_df.write.format("csv").option("header","true").mode("overwrite").save("/home/anurag/Desktop/output/answer2.csv")

#3.Are there any specific neighborhoods in New York City where both the number of crimes reported and the number of motor vehicle accidents where there is either injury or person killed are higher than average?

crime_counts = ny_crime_data.groupBy("BORO_NM").count()

# Group by borough and sum the number of injuries and fatalities
injury_counts = collision_df.groupBy("BOROUGH").sum("INJURED COUNT", "KILLED COUNT")

# Calculate the average number of crimes reported and injuries/fatalities
avg_crime_count = ny_crime_data.count() / crime_counts.count()
avg_injury_count = injury_counts.selectExpr("avg(`sum(INJURED COUNT)` + `sum(KILLED COUNT)`)").first()[0]

# Join the two datasets on the borough column
joined_df = crime_counts.join(injury_counts, crime_counts.BORO_NM == injury_counts.BOROUGH)

# Filter the joined dataset to keep only rows where both counts are higher than average
filtered_df = joined_df.filter((joined_df["count"] > avg_crime_count) & ((joined_df["sum(INJURED COUNT)"] + joined_df["sum(KILLED COUNT)"]) > avg_injury_count))

# Group by borough and count the number of neighborhoods meeting the criteria
result_df = filtered_df.groupBy("BORO_NM", "BOROUGH").count()
result_df=result_df.drop("BORO_NM")
# Show the result
result_df.show()
result_df.write.format("csv").option("header","true").mode("overwrite").save("/home/anurag/Desktop/output/answer3.csv")


---------------------------------------------------------------------------------------------- 
#4. Do certain types of crimes, such as theft or assault, occur more frequently in areas with a high number of motor vehicle collisions? IS there any relation between crime and the accidents happening?

#(i) is there any co-relation between crime and accidents happening?
---------------------------------------------------------------------------------------------- 
crime_counts = ny_crime_data.groupBy("BORO_NM").agg(count("COMPLAINT_NO").alias("crime_count"))
collissions_count = collision_df.groupBy("BOROUGH").agg(count("COLLISION_ID").alias("collision_count"))

# Calculate the average number of crimes and collisions per neighborhood
avg_crimes = crime_counts.groupBy("BORO_NM").agg(avg("crime_count").alias("avg_crimes"))
avg_collisions = collissions_count.groupBy("BOROUGH").agg(avg("collision_count").alias("avg_collisions"))

# Join the two datasets on the BOROUGH column
joined_df = avg_crimes.join(avg_collisions, avg_crimes.BORO_NM == avg_collisions.BOROUGH)
joined_df.write.format("csv").option("header","true").mode("overwrite").save("/home/anurag/Desktop/output/answer4.csv")
# Calculate the correlation between the average number of crimes and collisions per neighborhood
correlation = joined_df.corr("avg_crimes", "avg_collisions")

if correlation > 0:
    print("There is a positive correlation between the average number of crimes and collisions per neighborhood.")
else:
    print("There is no correlation between the average number of crimes and collisions per neighborhood.")

#----------------------------------------

#(ii)Do certain types of crimes, such as theft or assault, occur more frequently in areas with a high number of motor vehicle collisions?
---------------------------------------------------------------------------------------------- -------------------------------------------

# Count the number of crimes per borough and type of crime

crime_counts = ny_crime_data.groupBy("BORO_NM", "OFS_DESC").agg(count("COMPLAINT_NO").alias("crime_count"))

# Calculate the average number of crimes and collisions per neighborhood
avg_crimes = crime_counts.groupBy("BORO_NM").agg(avg("crime_count").alias("avg_crimes"))
avg_collisions = collision_df.groupBy("BOROUGH").agg(avg("COLLISION_ID").alias("avg_collisions"))

# Join the two datasets on the BOROUGH column
joined_df = avg_crimes.join(avg_collisions, avg_crimes.BORO_NM == avg_collisions.BOROUGH)


# Group the crimes by type and count the number of collisions per neighborhood
crime_collisions = crime_counts.join(collision_df, crime_counts.BORO_NM == collision_df.BOROUGH)
crime_collisions = crime_collisions.groupBy("BORO_NM", "OFS_DESC").agg(count("COLLISION_ID").alias("collision_count"))

# Calculate the average number of collisions per neighborhood
avg_collision_count = crime_collisions.groupBy("BORO_NM").agg(avg("collision_count").alias("avg_collision_count"))

# Join the crime and collision counts on the BORO_NM column
joined_df = avg_crimes.join(avg_collision_count, avg_crimes.BORO_NM == avg_collision_count.BORO_NM)


# Print the results
print("Average number of crimes and collisions per neighborhood:")
joined_df.show(1000)

print("Average number of collisions per crime type and neighborhood:")
crime_collisions.show()

crime_collisions.write.format("csv").option("header","true").mode("overwrite").save("/home/anurag/Desktop/output/answer4_2.csv")

#5.Which neighborhood in each borough had the highest number of sales in the given year?
---------------------------------------------------------------------------------------------- 

# Join sales_data and population_data dataframes based on borough and year
joined_df = house_sales_df.join(population_df, ["BOROUGH", "YEAR"])

# Group by borough and neighborhood and sum up the number of sales
sales_by_neighborhood = joined_df.groupBy("BOROUGH", "NEIGHBORHOOD").sum("NUMBER_OF_SALES")

# Rank the neighborhoods based on the number of sales in descending order
ranked_sales = sales_by_neighborhood.withColumn("rank", rank().over(Window.partitionBy("BOROUGH").orderBy(desc("sum(NUMBER_OF_SALES)"))))

# Filter the neighborhoods with rank 1, which means the highest number of sales in the given year
highest_sales_neighborhoods = ranked_sales.filter(col("rank") == 1).select("BOROUGH", "NEIGHBORHOOD", "sum(NUMBER_OF_SALES)")
highest_sales_neighborhoods.show()

highest_sales_neighborhoods.write.format("csv").option("header","true").mode("overwrite").save("/home/anurag/Desktop/output/answer5.csv")

#6. Is there any correlation between the population of a neighborhood and the average sale price of homes in that neighborhood?

joined_df = house_sales_df.join(population_df, ["BOROUGH", "YEAR"])

# Group by borough and neighborhood and calculate the average sale price and total population

avg_price_population = joined_df.groupBy("BOROUGH").agg(avg("AVERAGE_SALE_PRICE").alias("avg_sale_price"), sum("Total Population").alias("total_population"))

avg_price_population.show(150)
# Calculate the correlation between the average sale price and total population
correlation = avg_price_population.select(corr("avg_sale_price", "total_population")).collect()[0][0]

if correlation > 0:
    print("There is a positive correlation between the avg_sale_price and total_population.")
else:
    print("There is no correlation between the avg_sale_price and total_population.")


anurag_project.py
Displaying anurag_project.py.
