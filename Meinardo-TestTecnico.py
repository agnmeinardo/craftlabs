#Using PySpark 


# Read the CSV file inferring the schema and considering the file headers. 
df = spark.read.options(inferSchema='True',delimiter=',',header='True').csv("sample_data/california_housing_train.csv")
# Print the row count 
df.count()
# And display the top 20 rows values 
df.show(20)

# Group the content by median_income 
df_grp = df.groupBy("median_income")

# Calculating the average for median_house_value 
from pyspark.sql.functions import avg

df_result = df_grp.avg('median_house_value')
# Display the results 
df_result.show()

#Finally, save the results into a new csv file named  sample_data/california_housing_train_results.csv 
df_result.write.format("csv").save("sample_data/california_housing_train_results.csv ")
