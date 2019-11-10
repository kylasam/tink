from pyspark.sql.types import *
from pyspark.sql.types import StructType

MessageSchmea=StructType([StructField("id", StringType())\
              ,StructField("first_name", StringType())\
              ,StructField("last_name", StringType())\
              ,StructField("email", StringType())\
              ,StructField("gender", StringType())\
              ,StructField("ip_address", StringType())\
              ,StructField("date", StringType())\
              ,StructField("country", StringType())])
