# Databricks notebook source
# MAGIC %md
# MAGIC # LIMPIEZA

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE

# COMMAND ----------

# MAGIC %md
# MAGIC Accedemos al nombre de nuestra cuenta de almacenamiento a través de la variable de entorno que configuramos en nuestro cluster y creamos una variable apuntando al path de nuestro contenedor **datos** y a la carpeta **bronze**

# COMMAND ----------

container = "lakehouse"
account = spark.conf.get("adls.account.name")
bronze_path = f"abfss://{container}@{account}.dfs.core.windows.net/bronze"

# COMMAND ----------

print(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos que tenemos acceso a la cuenta de almacenamiento

# COMMAND ----------

display(dbutils.fs.ls(bronze_path + '/movielens/'))

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a crear Spark DataFrames a partir de los directorios de cada uno de los datatsets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movies

# COMMAND ----------

movies_path = f"{bronze_path}/movielens/movies/"
movies_brz = (spark.read.format("delta").load(movies_path))
display(movies_brz.limit(10))

# COMMAND ----------

movies_brz.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ratings

# COMMAND ----------

ratings_path = f"{bronze_path}/movielens/ratings/"
ratings_brz = spark.read.format("delta").load(ratings_path)
display(ratings_brz.limit(10))

# COMMAND ----------

ratings_brz.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Links

# COMMAND ----------

links_path = f"{bronze_path}/movielens/links/"
links_brz = spark.read.format("delta").load(links_path)

display(links_brz.limit(10))

# COMMAND ----------

links_brz.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trailers

# COMMAND ----------

trailers_path = f"{bronze_path}/movielens/trailers/"
trailers_brz = spark.read.format("delta").load(trailers_path)
display(trailers_brz.limit(10))

# COMMAND ----------

trailers_brz.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora vamos a ir limpiando dataset a dataset aplicando pequeñas transformaciones de limpieza sobre cada Spark DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movies

# COMMAND ----------

trailers_brz.where("_rescued_data IS NOT NULL").count()

# COMMAND ----------

import re
from pyspark.sql.functions import col,split,upper

@udf("array<string>")
def parse_title(t:str):
    titleRegex = re.compile(r'^(.+)\((\d{4})\)$')
    m = titleRegex.search(t.strip())
    if m:
        title,year= m.groups()
        return [title.strip(),year.strip()]
    else:
        return [t,None]
    

movies_slv = movies_brz.select(
    col("movieId").alias("movies_id"),
    upper(parse_title(col("title"))[0]).alias("title"),
    parse_title(col("title"))[1].cast("integer").alias("year"),
    split(upper("genres"),'\|').alias("genres")
)

display(movies_slv)

# COMMAND ----------

display(movies_slv.where(col("year").isNull()))

# COMMAND ----------

from pyspark.sql.functions import array_remove

movies_slv = movies_slv.withColumn("genres",array_remove(col("genres"),"(NO GENRES LISTED)"))

# COMMAND ----------

display(movies_slv.where(col("year").isNull()))

# COMMAND ----------

movies_slv = movies_slv.where("year IS NOT NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ratings

# COMMAND ----------

ratings_brz.where("_rescued_data IS NOT NULL").count()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, from_unixtime
ratings_slv = ratings_brz.select(
    col("userId").alias("user_id"),
    col("movieId").alias("movie_id"),
    col("rating"),    
    to_timestamp(from_unixtime("timestamp")).alias("rated_at")
)
display(ratings_slv)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Links

# COMMAND ----------

from pyspark.sql.functions import concat,lit

links_slv = links_brz.select(
    col("movieId").alias("movie_id"),
    col("imdbId").alias("imdb_id"),    
    concat(lit("http://www.imdb.com/title/tt"),col("imdbId"),lit("/")).alias("imdb_url"),
    col("tmdbId").alias("tmdb_id"),    
    concat(lit("https://www.themoviedb.org/movie/"),col("tmdbId"),lit("/")).alias("tmdb_url")
)

display(links_slv)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trailers

# COMMAND ----------

from pyspark.sql.functions import *

trailers_slv = trailers_brz.select(
    col("movieId").alias("movie_id"),
    col("youtubeId").alias("youtube_id"),    
    concat(lit("https://www.youtube.com/embed/"),col("youtubeId"),lit("/")).alias("youtube_url")
)
display(trailers_slv)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Salvamos todas los DataFrames tranformados. 
# MAGIC
# MAGIC En este caso vamos a salvarlos en la capa **silver**
# MAGIC
# MAGIC Además vamos a salvar los datos en formato **delta** que es un formato optimizado para big data y analítica
# MAGIC

# COMMAND ----------

silver_path = f"abfss://{container}@{account}.dfs.core.windows.net/silver/"

movies_slv.write.format("delta").mode("overwrite").save(f"{silver_path}/movielens/movies/")

ratings_slv.write.format("delta").mode("overwrite").save(f"{silver_path}/movielens/ratings/")

links_slv.write.format("delta").mode("overwrite").save(f"{silver_path}/movielens/links/")

trailers_slv.write.format("delta").mode("overwrite").save(f"{silver_path}/movielens/trailers/")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Los usuarios no suelen trabajar con ficheros ya que les implicaría recordar todas las rutas a cada uno de los ficheros.
# MAGIC
# MAGIC Publicamos los distintos datasets como tablas para facilitar su uso mediante SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.movielens;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.movielens.movies
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://lakehouse@${adls.account.name}.dfs.core.windows.net/silver/movielens/movies';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.movielens.ratings
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://lakehouse@${adls.account.name}.dfs.core.windows.net/silver/movielens/ratings';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.movielens.links
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://lakehouse@${adls.account.name}.dfs.core.windows.net/silver/movielens/links';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.movielens.trailers
# MAGIC LOCATION 'abfss://lakehouse@${adls.account.name}.dfs.core.windows.net/silver/movielens/trailers';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from hive_metastore.movielens.movies 
# MAGIC where year = 1995 and array_contains(genres,'ACTION');

# COMMAND ----------

# MAGIC %sql
# MAGIC --COMMENT ON TABLE hive_metastore.movielens.movies IS 'Esta tabla contienes los datos de las peliculas';
# MAGIC --ALTER TABLE hive_metastore.movielens.movies SET TBLPROPERTIES (quality = 'silver');
# MAGIC --ALTER TABLE hive_metastore.movielens.movies ALTER COLUMN movies_id COMMENT 'Identificador de la pelicula';
# MAGIC DESCRIBE TABLE EXTENDED movielens.movies;

# COMMAND ----------

