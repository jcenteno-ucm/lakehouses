# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta Incremental V2
# MAGIC
# MAGIC En este caso vamos a hacer vamos a repetir el ejercicio anterior intentando hacer una generalización creando clases que nos faciliten la creación y escritura de los DataFrames
# MAGIC
# MAGIC Además, vamos a añadir otro paso durante la ingesta, que es el archivado de los ficheros procesados en la capa raw
# MAGIC
# MAGIC También vamos a registrar la carpeta de bronze, como una tabla en el catálogo

# COMMAND ----------

# MAGIC %md
# MAGIC definimos variables base de los paths de las 3 zonas involucradas en la ingesta

# COMMAND ----------

account = spark.conf.get("adls.account.name")

landing_container = f"abfss://landing@{account}.dfs.core.windows.net"
lakehouse_container = f"abfss://lakehouse@{account}.dfs.core.windows.net"

landing_path = landing_container
raw_path = f"{lakehouse_container}/raw"
bronze_path = f"{lakehouse_container}/bronze"

print(landing_path)
print(raw_path)
print(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC definimos variables base de los paths de las 3 zonas involucradas en la ingesta para este dataset, siguiendo la convención definida

# COMMAND ----------

datasource = 'retail'
dataset = "sales_orders"

dataset_landing_path = f"{landing_path}/{datasource}/{dataset}"
dataset_raw_path =  f"{raw_path}/{datasource}/{dataset}"
dataset_bronze_path = f"{bronze_path}/{datasource}/{dataset}"

print(dataset_landing_path)
print(dataset_raw_path)
print(dataset_bronze_path)

# COMMAND ----------

def list_all_files(path:str):
    """
    Lista recursivamente todos los archivos en una ruta y sus subcarpetas utilizando dbutils.fs.
    
    Parámetros:
        path (str): Ruta base en el sistema de archivos (e.g., 'dbfs:/mnt/mi_carpeta').
    
    Retorna:
        list: Lista de rutas completas de todos los archivos encontrados.
    """
    files_list = []
    try:
        # Listar contenidos de la ruta actual
        items = dbutils.fs.ls(path)
        
        for item in items:
            # Si es un archivo, añadirlo a la lista
            if item.isFile():
                files_list.append(item.path)
            # Si es un directorio, explorar recursivamente
            elif item.isDir():
                files_list.extend(list_all_files(item.path))
    except Exception as e:
        print(f"Error al listar {path}: {str(e)}")
    
    return files_list

for file in list_all_files(dataset_landing_path):
    print(file)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Definimos una clase que nos permita, a través de cierta parametrización, crear el DataFrame a partir del path de landing
# MAGIC
# MAGIC Esta clase no es suficientemente genérica pero es un buen punto de partida ya que solamente es capaz de trabajar con ficheros json

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, replace,lit

class LandingStreamReader:

    def __init__(self, builder):
        self.datasource = builder.datasource
        self.dataset = builder.dataset
        self.landing_path = builder.landing_path
        self.raw_path = builder.raw_path
        self.bronze_path = builder.bronze_path
        self.format = builder.format
        self.dataset_landing_path = f'{self.landing_path}/{self.datasource}/{self.dataset}'
        self.dataset_bronze_schema_location = f'{self.bronze_path}/{self.datasource}/{self.dataset}_schema'
        dbutils.fs.mkdirs(self.dataset_bronze_schema_location)
    
    def __str__(self):
        return (f"LandingStreamReader(datasource='{self.datasource}',dataset='{self.dataset}')")
        
    def add_metadata_columns(self,df):
      data_cols = df.columns
      
      metadata_cols = ['_ingested_at','_ingested_filename']

      df = (df.withColumn("_ingested_at",current_timestamp())
              .withColumn("_ingested_filename",replace(input_file_name(),lit(self.landing_path),lit(self.raw_path)))
      ) 
      
      #reordernamos columnas
      return df.select(metadata_cols + data_cols)  
    
    def read_json(self):
      return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
            .load(self.dataset_landing_path)
      )
    
    def read(self):
      df = None

      if (self.format == "json"):
        df = self.read_json()
      else:
        raise Exception(f"Format {self.format} not supported")

      if df:
        df = df.transform(self.add_metadata_columns)
      return df
    
    class Builder:
        def __init__(self):
            self.datasource = None
            self.dataset = None
            self.landing_path = None
            self.raw_path = None
            self.bronze_path = None
            self.format = None
        
        def set_datasource(self, datasource):
            self.datasource = datasource
            return self
        
        def set_dataset(self, dataset):
            self.dataset = dataset
            return self
        
        def set_landing_path(self, landing_path):
            self.landing_path = landing_path
            return self
        
        def set_raw_path(self, raw_path):
            self.raw_path = raw_path
            return self
        
        def set_bronze_path(self, bronze_path):
            self.bronze_path = bronze_path
            return self
          
        def set_format(self, format):
            self.format = format
            return self
        
        def build(self):
            return LandingStreamReader(self)
          


# COMMAND ----------

# MAGIC %md
# MAGIC Definimos una clase que nos permita, a través de cierta parametrización, salvar el DataFrame en la capa bronze
# MAGIC
# MAGIC Esta clase, en principio podria ser reutilizable para cualquier otro dataset a ingestar en bronze

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, replace,lit

class BronzeStreamWriter:
    def __init__(self, builder):
        self.datasource = builder.datasource
        self.dataset = builder.dataset
        self.landing_path = builder.landing_path
        self.raw_path = builder.raw_path
        self.bronze_path = builder.bronze_path
        self.dataset_landing_path = f"{self.landing_path}/{self.datasource}/{self.dataset}"
        self.dataset_raw_path =  f"{self.raw_path}/{self.datasource}/{self.dataset}"
        self.dataset_bronze_path = f"{self.bronze_path}/{self.datasource}/{self.dataset}"
        self.dataset_checkpoint_location = f'{dataset_bronze_path}_checkpoint'
        self.table = f'hive_metastore.bronze.{self.datasource}_{self.dataset}'
        self.query_name = f"bronze-{datasource}-{dataset}"
        dbutils.fs.mkdirs(self.dataset_raw_path)
        dbutils.fs.mkdirs(self.dataset_bronze_path)
        dbutils.fs.mkdirs(self.dataset_checkpoint_location)

    def __str__(self):
        return (f"BronzeStreamWriter(datasource='{self.datasource}',dataset='{self.dataset}')")
         
    def archive_raw_files(self,df):
      if "_ingested_filename" in df.columns:
        files = [row["_ingested_filename"] for row in df.select("_ingested_filename").distinct().collect()]
        for file in files:
          if file:
              file_landing_path = file.replace(self.dataset_raw_path,self.dataset_landing_path)
              dbutils.fs.mkdirs(file[0:file.rfind('/')+1])
              dbutils.fs.mv(file_landing_path,file)
    
    def write_data(self,df):
      spark.sql( 'CREATE DATABASE IF NOT EXISTS hive_metastore.bronze') 
      spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} USING DELTA LOCATION '{self.dataset_bronze_path}' ") 
      (df.write
          .format("delta")  
          .mode("append")
          .option("mergeSchema", "true")
          .option("path", self.dataset_bronze_path)
          .saveAsTable(self.table)
      )
        
    def append_2_bronze(self,batch_df, batch_id):
      batch_df.persist()
      self.write_data(batch_df)
      self.archive_raw_files(batch_df)
      batch_df.unpersist()
      

    class Builder:
        def __init__(self):
            self.datasource = None
            self.dataset = None
            self.landing_path = None
            self.raw_path = None
            self.bronze_path = None
        
        def set_datasource(self, datasource):
            self.datasource = datasource
            return self
        
        def set_dataset(self, dataset):
            self.dataset = dataset
            return self
        
        def set_landing_path(self, landing_path):
            self.landing_path = landing_path
            return self
        
        def set_raw_path(self, raw_path):
            self.raw_path = raw_path
            return self
        
        def set_bronze_path(self, bronze_path):
            self.bronze_path = bronze_path
            return self
        
        def build(self):
            return BronzeStreamWriter(self)

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos la instancia con los datos del dataset

# COMMAND ----------

format = "json"

reader = (LandingStreamReader.Builder()          
  .set_datasource(datasource)
  .set_dataset(dataset)
  .set_landing_path(landing_path)
  .set_raw_path(raw_path)
  .set_bronze_path(bronze_path)
  .set_format(format)
  .build()
)

print(reader)

# COMMAND ----------

#display(reader.read())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Igualmente para el writer.

# COMMAND ----------

writer = (BronzeStreamWriter.Builder()
  .set_datasource(datasource)
  .set_dataset(dataset)
  .set_landing_path(landing_path)
  .set_raw_path(raw_path)
  .set_bronze_path(bronze_path)
  .build()
)

print(writer)

# COMMAND ----------

# MAGIC %md
# MAGIC Cuando lancemos el stream en las celdas posteriores, se arrancará y empezará a procesar los ficheros. 
# MAGIC
# MAGIC Tras ingestar todo (ahora mismo solo un fichero) se detendrá. Este comportamiento lo conseguimos mediante el trigger **availableNow**, que es una evolución mejorada del trigger Once en la que procesa poco a poco, todo los ficheros desde la última vez que ejecutó

# COMMAND ----------

(reader
  .read()
  .writeStream
  .foreachBatch(writer.append_2_bronze)
  .trigger(availableNow=True)
  #.trigger(processingTime="60 seconds") # modo continuo
  .option("checkpointLocation", writer.dataset_checkpoint_location)
  .queryName(writer.query_name)
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC --create table if not exists hive_metastore.bronze.retail_sales_orders
# MAGIC --using delta
# MAGIC --location 'abfss://lakehouse@masterjcfsta001sta.dfs.core.windows.net/bronze/retail/sales_orders'

# COMMAND ----------

rawfiles = list_all_files(dataset_raw_path)
for file in rawfiles:
    print(file)

# COMMAND ----------

for file in list_all_files(landing_path):
    print(file)

# COMMAND ----------

if len(rawfiles) > 0:
  display(dbutils.fs.ls(dataset_bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a comprobar los datos:

# COMMAND ----------

query = f"""
select * 
from delta.`{writer.dataset_bronze_path}`
order by _ingested_at desc
limit 10
"""
display(spark.sql(query))

# COMMAND ----------

query = f"""
select distinct _ingested_filename 
from delta.`{writer.dataset_bronze_path}`
"""
display(spark.sql(query))

# COMMAND ----------

