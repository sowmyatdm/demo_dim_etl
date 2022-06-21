from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr,input_file_name,split,substring
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

lineSep = "=============================================================================================================="
spark = SparkSession.builder.getOrCreate()

fileNameFormat = "csvInput_yyyyMMdd.csv"
dateFormat = "yyyyMMdd"
startingIndex = fileNameFormat.index(dateFormat)
endingIndex = startingIndex + len(dateFormat)

schema = StructType([StructField("id", IntegerType(), True),StructField("name",StringType(),True)])
landing = spark.read.option("header","true").schema(schema).csv("file:///C:\\Users\\Sowmya\\Studies\\PythonProjects\\Pyspark\\data\\*.csv")
landing_with_date = landing.withColumn("fullPath",input_file_name()).withColumn("filename",expr("split(fullPath,'/\')[size(split(fullPath,'/\'))-1]")).withColumn("effDateString",expr("substr(filename,"+str(startingIndex+1)+","+str(len(dateFormat))+")")).withColumn("effective_date",expr("cast(from_unixtime(unix_timestamp(effDateString, '"+dateFormat+"'), 'yyyy-MM-dd') as date)")).withColumn("insert_timestamp",expr("current_timestamp")).drop("fullPath","filename","effDateString")



#landing.createOrReplaceTempView("landing")
landing_with_date.createOrReplaceTempView("landing")
#landing_with_date.show()

current = spark.read.format("jdbc").option("driver","org.postgresql.Driver").option("url","jdbc:postgresql://localhost:5432/test").option("user","xxxxx").option("password","xxxxxx").option("query","select id,name,eff_from_date,eff_to_date,insert_timestamp from customer_current").load()
current.cache()
current.count()
current_with_date = current.withColumn("effective_date",col("eff_from_date")).drop("eff_from_date","eff_to_date")
current_with_date.createOrReplaceTempView("current")

curr_landing = spark.sql("select id,name,effective_date,insert_timestamp from landing union all select id,name,effective_date,insert_timestamp from current")
curr_landing_dis_mark = curr_landing.withColumn("distinct",expr("case when lag(name) over(partition by id order by effective_date)=name then 0 else 1 end"))
print("curr_landing_distinct_marked")
curr_landing_dis_mark.show() 
print(lineSep)
curr_landing_dis_rec = curr_landing_dis_mark.filter(expr("distinct=1")).drop("distinct")
print("curr_landing_distinct_record")
curr_landing_dis_rec.show()
print(lineSep)
curr_landing_eff_to = curr_landing_dis_rec.withColumn("eff_to_date",expr("date_sub(lead(effective_date) over(partition by id order by effective_date),1)")).withColumn("insert_timestamp",expr("current_timestamp"))
print("curr_landing_with_eff_to")
curr_landing_eff_to.show()
#curr_landing_eff_to.show()
print(lineSep)
to_curr = curr_landing_eff_to.filter(expr("eff_to_date is null")).drop("eff_to_date").withColumn("eff_to_date",expr("cast('9999-12-31' as date)")).withColumn("eff_from_date",col("effective_date")).drop("effective_date")
to_hist = curr_landing_eff_to.filter(expr("eff_to_date is not null")).withColumn("eff_from_date",col("effective_date")).drop("effective_date")
print("current")
to_curr.show()
print(lineSep)
print("history")
to_hist.show()

to_curr.write.format("jdbc").option("truncate", "true").mode("overwrite").option("driver","org.postgresql.Driver").option("url","jdbc:postgresql://localhost:5432/test").option("user","postgres").option("password","postgres").option("dbtable","customer_current").save()
to_hist.write.format("jdbc").mode("append").option("driver","org.postgresql.Driver").option("url","jdbc:postgresql://localhost:5432/test").option("user","postgres").option("password","postgres").option("dbtable","customer_history").save()

