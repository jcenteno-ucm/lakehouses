# Databricks notebook source
# MAGIC %md
# MAGIC #TRANSFORMACIÓN
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos a crear un modelo de recomendación de películas usando el algoritmo **ALS** usando la funcionalidad de **Spark MLLib**
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/ml-collaborative-filtering.html

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos DataFrames a partir de los datos de las tablas

# COMMAND ----------

database = "movielens"
movies = spark.sql("select * from hive_metastore.movielens.movies").cache()
print(f"There are {movies.count()} movies in the datasets")

# COMMAND ----------

ratings = spark.sql("select * from hive_metastore.movielens.ratings").cache()
print(f"There are {ratings.count()} ratings in the datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC Configuramos una semilla para hacer reproducible nuestro entrenamiento

# COMMAND ----------

seed = 42

# COMMAND ----------

# MAGIC %md
# MAGIC Separamos el dataset en training y test

# COMMAND ----------

(training, test) = ratings.randomSplit([0.8, 0.2], seed=seed)
print(f"Training: {training.count()}")
print(f"Test: {test.count()}")


# COMMAND ----------

display(training.limit(5))
display(test.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una instancia del algoritmo

# COMMAND ----------

from pyspark.ml.recommendation import ALS

als = ALS(userCol="user_id",
          itemCol="movie_id",
          ratingCol="rating",
          maxIter=5,
          seed=seed,
          coldStartStrategy="drop",
          regParam=0.1,
          nonnegative=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Puesto que lo que estamos intentando predecir es la valoración de un usuario para cada película, es un problema de regresión y por lo tanto vamos a usar **RMSE** como nuestra métrica de evaluación.
# MAGIC
# MAGIC Además vamos a configurar un grid para hacer la búsqueda de hyperparametros. En este caso, es un grid muy pequeño, de tan solo dos parametros para evitar entrenar muchos modelos

# COMMAND ----------

from pyspark.ml.tuning import *
from pyspark.ml.evaluation import RegressionEvaluator

rmse_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse")

grid = (ParamGridBuilder().addGrid(als.rank, [5, 10]).build())

cv = CrossValidator(numFolds=3, estimator=als, estimatorParamMaps=grid, evaluator=rmse_evaluator, seed=seed)          

cv_model = cv.fit(training)

# COMMAND ----------

# MAGIC %md
# MAGIC Obtenemos las métricas de nuestros modelos

# COMMAND ----------

cv_model.avgMetrics

# COMMAND ----------

# MAGIC %md
# MAGIC el segundo modelo tiene un error menor

# COMMAND ----------

best_model = cv_model.bestModel
print(f"El mejor modelo se ha entrenado con rank = {best_model.rank}")

# COMMAND ----------

# MAGIC %md
# MAGIC Las métricas anteriores han sido calculadas con un pequeños subconjunto de datos al hacer validación cruzada.
# MAGIC
# MAGIC Vamos a evaluar nuestro modelo contra el conjunto de **test**

# COMMAND ----------

predictions= best_model.transform(test)
rmse = rmse_evaluator.evaluate(predictions)
print(f"ALS RMSE: {rmse:.3}")

# COMMAND ----------

# MAGIC %md
# MAGIC Nuestro error final es de **0.878**

# COMMAND ----------

# MAGIC %md
# MAGIC Este tipo de modelos crear las recomendaciones como resultado, por lo tanto vamos a volver a entrenarlo, esta vez, con todas las valoraciones.
# MAGIC
# MAGIC Voy a añadir datos de un usuario ficticio con id 0, incluyendo algunas valoraciones de películas.
# MAGIC
# MAGIC Para ello creamos un DataFrame a partir de una lista python

# COMMAND ----------

from datetime import datetime
myUserId = 0
now = datetime.now()
myRatedMovies = [
     (myUserId, 1214, 5, now), # Alien
     (myUserId, 480,  5, now), # Jurassic Park
     (myUserId, 260, 5, now),  # Star Wars: Episode IV - A New Hope
     (myUserId, 541, 5, now),  # Blade Runner
     (myUserId, 2571, 5, now), # Matrix, The
     (myUserId, 296,  5, now), # Pulp Fiction
     (myUserId, 356,  5, now), # Forrest Gump     
     (myUserId, 593, 5, now),  # Silence of the Lambs, The
]

myRatingsDF = spark.createDataFrame(myRatedMovies, ['user_id', 'movie_id', 'rating','rated_at'])
display(myRatingsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Juntamos los dos DataFrames.

# COMMAND ----------

allratings = ratings.unionByName(myRatingsDF)
als.setRank(10)
best_model = als.fit(allratings)

# COMMAND ----------

# MAGIC %md
# MAGIC # SERVICIO LAKEHOUSE

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC En esta primera versión, vamos a servir las recomendaciones directamente desde nuestro lakehouse / datalake
# MAGIC
# MAGIC Configuramos la ruta para escribir los datos en la capa **gold**
# MAGIC

# COMMAND ----------

container = "lakehouse"
account = spark.conf.get("adls.account.name")
gold_path = f"abfss://{container}@{account}.dfs.core.windows.net/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos un schema de base de datos dedicado para nuestras recomendaciones

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.recommender;

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos 10 recomendaciones (peliculas) para todos los usuarios

# COMMAND ----------

user_recs = best_model.recommendForAllUsers(5)
display(user_recs)

# COMMAND ----------

# MAGIC %md
# MAGIC Cuando hicimos la limpieza, escribimos el dato en el path de silver y después definimos las tablas para su consumo SQL
# MAGIC
# MAGIC Es posible hacer las dos cosas de una sola vez 

# COMMAND ----------

(user_recs
.write
.format("delta")
.mode("overwrite")
.option("path",f"{gold_path}/recommender/user_recommendations")
.saveAsTable("hive_metastore.recommender.user_recommendations")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos 5 recomendaciones (usuarios) para todas las películas

# COMMAND ----------

movie_recs = best_model.recommendForAllItems(5)
display(movie_recs)


# COMMAND ----------

(movie_recs
.write
.format("delta")
.mode("overwrite")
.option("path",f"{gold_path}/recommender/movie_recommendations")
.saveAsTable("hive_metastore.recommender.movie_recommendations"))