# Copyright 2022 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https: // www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Importação das bibliotecas necessárias
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, year, lit
import datetime

if len(sys.argv) < 2:
    print("Please provide a dataset name.")
    sys.exit(1)

dataset = sys.argv[1]
table = "bigquery-public-data.new_york_citibike.citibike_trips"

# Inicialização da sessão Spark
spark = SparkSession.builder \
    .appName("pyspark-example") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
    .getOrCreate()

# Carregando o dataset do BigQuery
df = spark.read.format("bigquery").load(table)

# 1. Duração média das viagens - 10 mais lentas
avg_duration = df.groupBy("start_station_id", "end_station_id") \
    .agg(avg("tripduration").alias("avg_duration")) \
    .orderBy("avg_duration", ascending=False) \
    .limit(10)

avg_duration.show()

# Salvando o resultado no BigQuery
avg_duration.write.format('bigquery') \
    .option("writeMethod", "direct") \
    .option("table", f"{dataset}.citibikes_top_ten_slowest_trips") \
    .mode("overwrite") \
    .save()

# 2. Duração média das viagens - 10 mais rápidas (removendo as sem duração)
non_null_df = df.filter(col("tripduration").isNotNull())
avg_duration_fastest = non_null_df.groupBy("start_station_id", "end_station_id") \
    .agg(avg("tripduration").alias("avg_duration")) \
    .orderBy("avg_duration", ascending=True) \
    .limit(10)

avg_duration_fastest.show()

# Salvando o resultado no BigQuery
avg_duration_fastest.write.format('bigquery') \
    .option("writeMethod", "direct") \
    .option("table", f"{dataset}.citibikes_top_ten_fastest_trips") \
    .mode("overwrite") \
    .save()

# 3. Duração média das viagens por par de estações - 10 mais lentas
avg_duration_by_station = df.groupBy("start_station_id", "end_station_id") \
    .agg(avg("tripduration").alias("avg_duration")) \
    .orderBy("avg_duration", ascending=False) \
    .limit(10)

avg_duration_by_station.show()

# Salvando o resultado no BigQuery
avg_duration_by_station.write.format('bigquery') \
    .option("writeMethod", "direct") \
    .option("table", f"{dataset}.citibikes_top_ten_slowest_station_pairs") \
    .mode("overwrite") \
    .save()

# 4. Total de viagens por bicicleta - 10 mais utilizadas
total_trips_by_bike = df.groupBy("bikeid") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10)

total_trips_by_bike.show()

# Salvando o resultado no BigQuery
total_trips_by_bike.write.format('bigquery') \
    .option("writeMethod", "direct") \
    .option("table", f"{dataset}.citibikes_top_ten_most_used_bikes") \
    .mode("overwrite") \
    .save()

# 5. Média de idade dos clientes
current_year = datetime.datetime.now().year
age_avg = df.withColumn("age", lit(current_year) - col("birth_year")) \
    .agg(avg("age").alias("avg_age"))

age_avg.show()

# Salvando o resultado no BigQuery
age_avg.write.format('bigquery') \
    .option("writeMethod", "direct") \
    .option("table", f"{dataset}.citibikes_average_age") \
    .mode("overwrite") \
    .save()

# 6. Distribuição entre gêneros
gender_distribution = df.groupBy("gender") \
    .count()

gender_distribution.show()

# Salvando o resultado no BigQuery
gender_distribution.write.format('bigquery') \
    .option("writeMethod", "direct") \
    .option("table", f"{dataset}.citibikes_gender_distribution") \
    .mode("overwrite") \
    .save()

# Finalização da sessão Spark
spark.stop()

