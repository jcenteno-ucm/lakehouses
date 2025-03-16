# Databricks notebook source
# MAGIC %md
# MAGIC #PARQUET PRIMER

# COMMAND ----------

# MAGIC %md
# MAGIC ## INTRODUCCIÓN
# MAGIC
# MAGIC Puesto que **Delta Lake** almacena los datos en ficheros **Parquet**, primeramente vamos a ver en detalle las principales características de este formato a través de un ejemplo.

# COMMAND ----------

# MAGIC %md
# MAGIC Definimos una función para usar durante los distintos escenarios

# COMMAND ----------

BASE_PATH= f"/FileStore/tmp/parquet-primer"
dbutils.fs.rm(BASE_PATH, recurse=True)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

def get_random_data(n:int=100000):
  data = (spark.range(0, n)  
               .withColumn("module10", expr("id % 10"))
               .coalesce(1)
  )
  return data

def save_data(df:DataFrame,
              format:str,
              mode:str='overwrite',
              partitionColumns:list=[],
              dropContents:bool=True):
  if partitionColumns:
    path = f"{BASE_PATH}/data_partitioned.{format}"
  else:
    path = f"{BASE_PATH}/data.{format}"
  if dropContents:
    print(f"Removing contents of {path}")
    dbutils.fs.rm(path, recurse=True)
  dfw = df.write
  if partitionColumns:
    dfw = dfw.partitionBy(partitionColumns)  
  (dfw
       .format(format)      
       .mode(mode)   
       .save(path)
  )    
  print(f"Spark Partitions: {df.rdd.getNumPartitions()}")
  print(f"Data {mode} in path: {path}")
  return path


# COMMAND ----------

# MAGIC %md
# MAGIC ##APACHE PARQUET

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src="https://www.casd.eu/wp/wp-content/uploads/parquet-logo.png" width=200/>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SIN PARTICIONAMIENTO

# COMMAND ----------

path = save_data(get_random_data(),"parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Veamos el contenido del directorio

# COMMAND ----------

display(dbutils.fs.ls(path))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC NOTAS:
# MAGIC
# MAGIC - El nombre de fichero que especificamos es en realidad un **directorio**. Los motores de procesamiento como Apache Spark distribuyen los datos entre los nodos del cluster (particiones) y al persistirlos, darán lugar a ficheros independientes.
# MAGIC
# MAGIC - Los ficheros y directorios que empiezan por _ se consideran **metadatos**. 
# MAGIC
# MAGIC - El fichero _SUCCESS es un fichero que usa Apache Spark para confirmar que todas las particiones se escribieron correctamente. 
# MAGIC
# MAGIC - Adicionalmente podemos ver un par de ficheros _started y _committed para indicar el inicio y fin de la operación de escritura.

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a listar solamente los ficheros de datos

# COMMAND ----------

data_files = [file for file in dbutils.fs.ls(path) if file.name.endswith(".parquet")]
display(data_files)

# COMMAND ----------

# MAGIC %md
# MAGIC Estructura del nombre de los ficheros:
# MAGIC
# MAGIC - part-XXXXX → Representa el id de la partición de datos en Apache Spark.
# MAGIC - tid-YYYYYYYYYYYYYYYY → Identificador único basado en el timestamp o UUID para evitar colisiones de nombre en los ficheros
# MAGIC - .snappy.parquet → Formato y compresor

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a ver el contenido de uno (el primero) de los ficheros de datos

# COMMAND ----------

display(spark.read.parquet(data_files[0].path))

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA SKIPPING - PREDICATE PUSHDOWN

# COMMAND ----------

# MAGIC %md
# MAGIC En este caso vamos a hacer una lectura de datos aplicando un predicado.
# MAGIC
# MAGIC Vamos a ver como Spark aplica esta optimización a través del plan físico de ejecución

# COMMAND ----------

spark.read.parquet(path).where("module10 between 3 and 5").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver que Spark empujó el predicado en la lectura de los datos:
# MAGIC
# MAGIC **PushedFilters: [IsNotNull(module), GreaterThanOrEqual(module,3), LessThanOrEqual(module,5)]**
# MAGIC
# MAGIC Si el filtro no apareciera en PushedFilters, significa que Spark está aplicando el filtro después de la lectura, lo que no es eficiente.

# COMMAND ----------

spark.read.parquet(path).where("(module10 between 3 and 5) and (id between 300 and 500)").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Podemos ver que Spark sigue empujando el predicado en la lectura de los datos:
# MAGIC
# MAGIC **PushedFilters: [IsNotNull(module), IsNotNull(id), GreaterThanOrEqual(module,3), LessThanOrEqual(module,5), GreaterThanOrEqual(id,300), LessThanOrEqual(id,500)]**

# COMMAND ----------

# MAGIC %md
# MAGIC #### PARQUET TO DELTA LAKE
# MAGIC
# MAGIC Muchos data lakes organizan sus datos en formato parquet. 
# MAGIC
# MAGIC ¿Cómo podemos migrar estos directorios a formato Delta Lake?

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`/FileStore/tmp/parquet-primer/data.parquet`

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.convertToDelta(spark, f"parquet.`{path}`")


# COMMAND ----------

# MAGIC %md
# MAGIC Observemos que ha ocurrido a nivel de almacenamiento

# COMMAND ----------

display(dbutils.fs.ls(path))

# COMMAND ----------

display(dbutils.fs.ls(path + "/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver que ha generado el subdirectorio _delta_log y ha creado la primera transacción

# COMMAND ----------

def get_download_url(path):
  workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
  return f"https://{workspace_url}/files/{path.replace('/FileStore/','')}"

transaction_path = f"{path}/_delta_log/00000000000000000000.json"

print(transaction_path)

get_download_url(transaction_path)  


# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tmp/parquet-primer/data.parquet/_delta_log/00000000000000000000.json
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### CON PARTICIONAMIENTO

# COMMAND ----------

# MAGIC %md
# MAGIC En este caso vamos a usar particionamiento. 
# MAGIC
# MAGIC Este particionamiento se refiere a organizar los ficheros en subcarpetas (NO se refiere a las particiones que maneja internamente en Spark)

# COMMAND ----------

path = save_data(get_random_data(),"parquet",partitionColumns=["module10"])

# COMMAND ----------

display(dbutils.fs.ls(path))

# COMMAND ----------

import glob

def get_data_files(path):
  return glob.glob(f"/dbfs{path}/**/*.parquet")  
  
def display_data_files(dat_files):  
  display(spark.createDataFrame(data_files, "string"))
  
data_files = get_data_files(path)

display_data_files(data_files)

# COMMAND ----------

# MAGIC %md
# MAGIC Veamos el contenido de uno los ficheros de datos:

# COMMAND ----------

display(spark.read.parquet(data_files[0].replace('/dbfs','')).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTAS** 
# MAGIC
# MAGIC - Podemos ver que los datos relativos a **la columna de particionado no están presente en los ficheros parquet**. La razón es que los valores de cada fila en ese fichero, siempre tendrán el mismo valor.
# MAGIC
# MAGIC - Para obtener el DataFrame con todas las columnas, tenemos que crear el DataFrame a partir del directorio raiz
# MAGIC

# COMMAND ----------

display(spark.read.parquet(path).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### PARTITION PRUNING

# COMMAND ----------

spark.read.parquet(path).where("module10 between 3 and 5").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver que Spark que Spark NO empujó el predicado sino que hace uso del filtrado de particiones (partition pruning):
# MAGIC
# MAGIC **PartitionFilters: [isnotnull(module#xxx), (module#xxx >= 3), (module#xxx <= 5)]**
# MAGIC
# MAGIC **PushedFilters: []**
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####PARTITION PRUNING & PREDICATE PUSHDOWN

# COMMAND ----------

spark.read.parquet(path).where("(module10 between 3 and 5) and (id between 300 and 500)").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver que Spark que Spark aplica ambas optimizaciones durante la lectura (partition pruning y predicate pushdown) uso del filtrado de particiones:
# MAGIC
# MAGIC  **PartitionFilters: [isnotnull(module#xxx), (module#xxx >= 3), (module#xxx <= 5)]**
# MAGIC  
# MAGIC  **PushedFilters: [IsNotNull(id), GreaterThanOrEqual(id,300), LessThanOrEqual(id,500)]**

# COMMAND ----------

# MAGIC %md
# MAGIC ####PREDICATE PUSHDOWN

# COMMAND ----------

spark.read.parquet(path).where("id between 300 and 500").explain('formatted')

# COMMAND ----------

# MAGIC %md
# MAGIC En ese caso Spark tiene empujar el predicado a todos los ficheros ya que no puedes descartar particiones al no usar la columna de particionado en el filtro
# MAGIC
# MAGIC **PushedFilters: [IsNotNull(id), GreaterThanOrEqual(id,300), LessThanOrEqual(id,500)]**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### PARQUET TO DELTA LAKE
# MAGIC
# MAGIC Al igual que antes, vamos a transformar el directorio en una tabla delta

# COMMAND ----------

deltaTable = DeltaTable.convertToDelta(spark,f"parquet.`{path}`",)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC En este caso recibimos un error
# MAGIC
# MAGIC **Expecting 0 partition column(s): [], but found 1 partition column(s): [`module`]** 
# MAGIC
# MAGIC from parsing the file name: 
# MAGIC
# MAGIC dbfs:/FileStore/tmp/delta-lake-primer/data_partitioned.parquet/module=1/part-00000-tid-***.snappy.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC Para que funcione correctamente debemos explicitar la columna y tipo de particion

# COMMAND ----------

deltaTable = DeltaTable.convertToDelta(spark, f"parquet.`{path}`","module10 int")

# COMMAND ----------

display(dbutils.fs.ls(path))

# COMMAND ----------

display(dbutils.fs.ls(path + "/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC Veamos en este caso el contenido del fichero checkpoint

# COMMAND ----------

display(spark.read.parquet(path + "/_delta_log/00000000000000000000.checkpoint.parquet"))

# COMMAND ----------

transaction_path = f"{path}/_delta_log/00000000000000000000.json"

get_download_url(transaction_path)  

# COMMAND ----------

