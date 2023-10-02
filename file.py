import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col, when, to_json, struct

#args = getResolvedOptions(sys.argv, "job1")
glueContext = GlueContext(sc)
spark = glueContext.spark_session


df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-name/2023/09/28/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)
# o log lido pode conter erros junto com requisicoes validas
# após a leitura dos campos, é validado se o há muitos campos nulos e se sim,
# as colunas serão deletadas
df_with_counts = df.toDF().withColumn("non_null_count", sum(when(col(c).isNotNull(), 1).otherwise(0) for c in df.toDF().columns))

# Filtra colunas com poucos nulos
filtered_df = df_with_counts.filter(col("non_null_count") > 10)

# Drop da coluna de contabilização dos nulos
filtered_df = filtered_df.drop("non_null_count")

# caso existam colunas restantes com valores nulos, elas serão deletadas
null_columns = [col_name for col_name in filtered_df.columns if filtered_df.filter(col(col_name).isNotNull()).count() == 0]

filtered_df = filtered_df.drop(*null_columns)

filtered_df.show()

df_with_array_req = filtered_df.withColumn("struct_as_array_req", struct(col("request.headers")))
df_with_array_resp = df_with_array_req.withColumn("struct_as_array_resp", struct(col("response.headers")))

filtered_df = df_with_array_resp.withColumn("headers_final_req", to_json(col('struct_as_array_req.headers')))
filtered_df = filtered_df.withColumn("headers_final_resp", to_json(col('struct_as_array_resp.headers')))

filtered_df = filtered_df.drop('struct_as_array_req')\
            .drop('struct_as_array_resp')

filtered_df.show()

# json_df = df_with_array.select(to_json(col('struct_as_array')).alias("json_output"))

# json_df.show(truncate=False)
dynf = DynamicFrame.fromDF(filtered_df, glueContext, "dynamic_frame_name")


#final

erroneous_condition = (col("container_id").isNull()
                    & col("log").isNull()
                    & col("ecs_cluster").isNull()
                    & col("ecs_task_arn").isNull()
                    & col("ecs_task_definition").isNull()
                    & col("date").isNotNull())

# Filter the DataFrame to keep only the rows that do not match the erroneous schema
cleaned_df = df.toDF().filter(erroneous_condition)
not_cleaned_df = df.toDF().filter(~erroneous_condition)

# Show the cleaned DataFrame

null_columns = [col_name for col_name in cleaned_df.columns if cleaned_df.filter(col(col_name).isNotNull()).count() == 0]

# print(null_columns)

filtered_df = cleaned_df.drop(*null_columns)
