df_with_counts = df.toDF().withColumn("non_null_count", sum(when(col(c).isNotNull(), 1).otherwise(0) for c in df.toDF().columns))
df_with_counts.select('non_null_count').show()
# Filter rows with more than one non-null value
filtered_df = df_with_counts.filter(col("non_null_count") > 6)

# Drop the "non_null_count" column if needed
filtered_df = filtered_df.drop("non_null_count")

def join_colunas_correlation(df):
    df_with_combined_column = df.withColumn(
        "combined_correlation_id",
        when(col("correlation_id").isNotNull(), col("correlation_id")).otherwise(col("correlationId"))
    )

    # Now, you can perform your join using the 'combined_correlation_id' column
    # For example, you can join the DataFrame with itself to find matching rows
    joined_df = df_with_combined_column.alias("df1").join(
        df_with_combined_column.alias("df2"),
        col("df1.combined_correlation_id") == col("df2.combined_correlation_id"),
        "inner"
    ).select(
        col("df1.*")
    )

    joined_df = joined_df.drop('correlation_id')\
        .drop('correlationId')

    joined_df = joined_df.withColumnRenamed('combined_correlation_id', 'correlationId')
    
#     joined_df = joined_df.drop('combined_correlation_id')
    
    return joined_df

#adicionar um if para checar se é necessario juntar colunas ou não
colunas = df.toDF().columns
colunas_correlation = ['correlation_id', 'correlationId'] 

if 'correlation_id' and 'correlationId' in colunas:
    filtered_df = join_colunas_correlation(df.toDF())
else:
    columns_to_check = ['correlationId', 'timestamp', 'request', 'response', 'status']

    # Filter out rows with null values in any of the specified columns
    filtered_df = filtered_df.dropna(subset=columns_to_check, how='any')

filtered_df.show()

null_columns = [col_name for col_name in filtered_df.columns if filtered_df.filter(col(col_name).isNotNull()).count() == 0]

filtered_df = filtered_df.drop(*null_columns)

filtered_df.show()

df_with_array_req = filtered_df.withColumn("struct_as_array_req", struct(col("request.headers")))
df_with_array_resp = df_with_array_req.withColumn("struct_as_array_resp", struct(col("response.headers")))

filtered_df = df_with_array_resp.withColumn("headers_final_req", to_json(col('struct_as_array_req.headers')))
filtered_df = filtered_df.withColumn("headers_final_resp", to_json(col('struct_as_array_resp.headers')))

filtered_df = filtered_df.drop('struct_as_array_req')\
            .drop('struct_as_array_resp')

# filtered_df.show()
# filtered_df.printSchema()

# json_df = df_with_array.select(to_json(col('struct_as_array')).alias("json_output"))

# json_df.show(truncate=False)
dynf = DynamicFrame.fromDF(filtered_df, glueContext, "dynamic_frame_name")
