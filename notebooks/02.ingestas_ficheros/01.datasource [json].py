# Databricks notebook source
# MAGIC %md
# MAGIC # Datasource
# MAGIC Para hacer este ejercicio debes crear dos containers en tu cuenta de almacenamiento ADLS configurada (sólo si todavía no los tienes):
# MAGIC
# MAGIC - landing
# MAGIC - lakehouse
# MAGIC
# MAGIC Existen diversas aproximanciones sobre cómo estructurar una data lakehouse usando una o varias cuentas de almacenamiento, containers o directorios
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC A continuación configuramos variables a partir de la cuenta de almacenamiento (ADLS) configurada Spark

# COMMAND ----------

account = spark.conf.get("adls.account.name")
landing_path = f"abfss://landing@{account}.dfs.core.windows.net"

print(landing_path)


# COMMAND ----------

# MAGIC %md
# MAGIC # Generación de extracciones
# MAGIC
# MAGIC Vamos a simular la generación de datos por parte de otro sistema. 
# MAGIC
# MAGIC En muchas ocasiones de la vida real, los sistemas origen son los encargadados de generar extracciones de sus sistemas y empujarlos hasta la zona de landing.
# MAGIC
# MAGIC Databricks incluye varios conjuntos de datos para facilitar la práctica y aprendizaje de su plataforma.
# MAGIC
# MAGIC En este caso vamos a hacer uso de algunos de los dataset de una compañia ficticia de retail.
# MAGIC
# MAGIC Todas las empresas establecen una convención sobre como organizar la capa de landing. 
# MAGIC
# MAGIC En este caso vamos a organizarla en dos niveles:
# MAGIC
# MAGIC /**datasource**/**dataset**/

# COMMAND ----------

# MAGIC %md
# MAGIC En este caso, el origen de datos se llama **retail-org**

# COMMAND ----------

datasource = "retail-org"

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver todos los datasets disponibles de este datasource a través de la interfaz de DBFS o mediante la utilizad dbutils
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls(f"/databricks-datasets/"))

# COMMAND ----------

display(dbutils.fs.ls(f"/databricks-datasets/{datasource}/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Para este ejercio, vamos a usar el dataset **sales_orders** que está basado en un único fichero json

# COMMAND ----------

dataset = "sales_orders"

display(dbutils.fs.ls(f"/databricks-datasets/{datasource}/{dataset}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a crear un Spark DataFrame sobre esta carpeta para ver el schema de los datos:

# COMMAND ----------

df = spark.read.json(f"/databricks-datasets/{datasource}/{dataset}/")

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC como curiosidad, comentar que el orden de las columnas se hace de forma alfabética, esto es debido a que los campos de un objecto JSON no tienen orden predefinido, a diferencia de un registro de base de datos

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a simular la generación de varios ficheros
# MAGIC
# MAGIC Para ello crearemos varios tres dataframes que se corresponderán con cada uno de los ficheros que podremos en landing

# COMMAND ----------

file1 = df.where("customer_id <= 10000000").coalesce(1)
file2 = df.where("customer_id between 10000000 and 20000000").coalesce(1)
file3 = df.where("customer_id >= 20000000").coalesce(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Como hemos explicado, también es común definir una convenciónd de nombres para los ficheros que se reciben. De este modo es facil entender cual es el contenido de los ficheros y su origen

# COMMAND ----------

import datetime

def land_file(df,datasource,dataset,format='json'):
  """
    Guarda un DataFrame en un sistema de archivos distribuido con una estructura de directorios basada en la fecha actual,
    utilizando un formato específico (por defecto, JSON). La función escribe el DataFrame en una ubicación temporal,
    lo mueve a una ruta final organizada por fuente de datos, conjunto de datos y marca de tiempo, y luego elimina el
    directorio temporal.

    Parámetros:
        df (pyspark.sql.DataFrame): El DataFrame de Spark que se desea guardar.
        datasource (str): Nombre o identificador de la fuente de datos, usado para organizar la ruta final.
        dataset (str): Nombre o identificador del conjunto de datos, usado para organizar la ruta final.
        format (str, opcional): Formato en el que se guardará el archivo. Por defecto es 'json'. 
                                Otros formatos soportados dependen de Spark (e.g., 'parquet', 'csv').

    Comportamiento:
        1. Escribe el DataFrame en una carpeta temporal (`tmp_path`) usando el formato especificado, coalesciendo los datos en un solo archivo.
        2. Genera una ruta final basada en la fecha actual (`YYYY/MM/DD`), el nombre de la fuente de datos, el conjunto de datos y una marca de tiempo.
        3. Mueve el archivo generado desde la carpeta temporal a la ruta final.
        4. Imprime la ruta final del archivo guardado.
        5. Elimina la carpeta temporal.

    Variables externas utilizadas:
        - landing_path (str): Ruta base del sistema de archivos donde se almacenan los datos. Debe estar definida globalmente.
        - dbutils.fs: Utilidad de Databricks para manipular el sistema de archivos (ls, mv, rm).
        - datetime: Módulo de Python para manejar fechas y marcas de tiempo.

    Ejemplo:
        save_file(mi_dataframe, "ventas", "diarias", format="parquet")
        # Salida esperada: "dbfs:/landing/ventas/diarias/2025/03/14/ventas_diarias_20250314123045.parquet"

    Notas:
        - La función asume que está ejecutándose en un entorno compatible con Databricks (por el uso de `dbutils.fs`).
        - Si el formato especificado no es compatible con Spark, se generará un error.
    """
  tmp_path = f'{landing_path}/tmp/'
  df.coalesce(1).write.format(format).mode("overwrite").save(tmp_path)
  now = datetime.datetime.utcnow()
  date_path = now.strftime("%Y/%m/%d")
  timestamp = now.strftime("%Y%m%d%H%M%S") 
  for file in dbutils.fs.ls(tmp_path):
    if file.name.endswith(f'.{format}'):
      final_path = file.path.replace('tmp',f'{datasource}/{dataset}')
      final_path = final_path.replace(file.name, f'{date_path}/{datasource}-{dataset}-{timestamp}.{format}')
      dbutils.fs.mv(file.path, final_path)
      print(final_path)
  dbutils.fs.rm(tmp_path, True)
  

# COMMAND ----------

# MAGIC %md
# MAGIC De momento, vamos a escribir solamente el primer fichero en la zona de aterrizaje

# COMMAND ----------

import time 

land_file(file1,'retail','sales_orders')

time.sleep(5)

# COMMAND ----------

land_file(file2,'retail','sales_orders')

time.sleep(5)

# COMMAND ----------

land_file(file3,'retail','sales_orders')

time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a revisar la carpeta de landing. Deberia contener 3 ficheros
# MAGIC