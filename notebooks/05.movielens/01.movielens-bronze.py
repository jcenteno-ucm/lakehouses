# Databricks notebook source
# MAGIC %md
# MAGIC #Configuracion Lakehouse

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

bronze_spark_conf = {
    "format" : "delta",
    "options": {
        "mergeSchema": "true"
    },
    "path":bronze_path
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuración Ingesta

# COMMAND ----------

movielens_movies  = {
    "datasource": "movielens",
    "dataset": "movies", 
    "source": {
        "format" : "cloudFiles",
        "options" : {          
            "cloudFiles.format": "csv",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "header": "true",
            "cloudFiles.schemaHints": "movieId long, title string, genres string"
        },
        "path": f'{landing_path}/movielens/movies'
    },
    "sink": {        
        "layer": "bronze"
    }
}

movielens_ratings  = {
    "datasource": "movielens",
    "dataset": "ratings",
    "source": {
        "format" : "cloudFiles",
        "options" : {          
            "cloudFiles.format": "csv",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "header": "true",
            "cloudFiles.schemaHints": "userId long, movieId long, rating double, timestamp long"
        },
        "path": f'{landing_path}/movielens/ratings'
    },
    "sink": {        
        "layer": "bronze"
    }
}

movielens_trailers  = {
    "datasource": "movielens",
    "dataset": "trailers",
    "source": {
        "format" : "cloudFiles",
        "options" : {          
            "cloudFiles.format": "csv",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "header": "true",
            "cloudFiles.schemaHints": "youtubeId string, movieId long, title string"
        },
        "path": f'{landing_path}/movielens/trailers'
    },
    "sink": {        
        "layer": "bronze"
    }
}

movielens_links  = {
    "datasource": "movielens",
    "dataset": "links",
    "source": {
        "format" : "cloudFiles",
        "options" : {          
            "cloudFiles.format": "csv",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "header": "true",
            "cloudFiles.schemaHints": "movieId long, imdbId string, tmdbId string"
        },
        "path": f'{landing_path}/movielens/links'
    },
    "sink": {        
        "layer": "bronze"
    }
}

ingestion_configs = [movielens_movies, movielens_ratings, movielens_trailers, movielens_links]

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro


def read_stream(ingestion_config:dict) -> DataFrame:
    datasource = ingestion_config.get("datasource")
    dataset = ingestion_config.get("dataset")
    source = ingestion_config.get("source")
    format = source.get("format")
    df = None
    if "cloudFiles" == format:
        opts = source.get("options")
        landing_path = source.get("path")
        schema_evolution_path = f'{bronze_path}/{datasource}/{dataset}_schema'
        df = (spark.readStream
                .format(format)
                .options(**opts)
                .option("cloudFiles.schemaLocation",schema_evolution_path)
                .load(landing_path)
                .withColumn("_ingested_filename",F.input_file_name())
            )    
    else:
        raise Exception(f'No source format "{format}" implemented!')

    return df.withColumn("_ingested_at",F.current_timestamp()) 



# COMMAND ----------

from pyspark.sql.streaming import StreamingQuery

def write_stream(ingestion_config:dict,df:DataFrame) -> StreamingQuery:
    datasource = ingestion_config.get("datasource")
    dataset = ingestion_config.get("dataset")
    sink = ingestion_config.get("sink")
    layer = sink.get("layer")
    query = None
    if "bronze" == layer:
        format = bronze_spark_conf.get("format")
        opts = bronze_spark_conf.get("options")
        if sink.get("options"):
            opts.update(sink.get("options"))
        query = (df.writeStream
                    .format(format)
                    .options(**opts)
                    .option("checkpointLocation", f'{bronze_path}/{datasource}/{dataset}_checkpoint/')
                    .trigger(availableNow=True)
                    .queryName(f'{datasource} {dataset}')
                    .start(f'{bronze_path}/{datasource}/{dataset}')
                )
    else:
        raise Exception(f'layer {layer} not configured!')
    
    return query  

# COMMAND ----------

#creating dataframes
def read_streams(ingestion_configs:list[dict]) -> list[DataFrame]:
    return [read_stream(c) for c in ingestion_configs]

def write_streams(ingestion_configs:list[dict],dfs:list[DataFrame]) -> list[StreamingQuery]:
    queries = []
    for (c,df) in zip(ingestion_configs,dfs):
        queries.append(write_stream(c,df))
    return queries

# COMMAND ----------

# MAGIC %md
# MAGIC # Motor de Ingesta

# COMMAND ----------


#creating streaming dataframes
dfs = read_streams(ingestion_configs)
#creating streaming queries
queries = write_streams(ingestion_configs, dfs)
#waiting for queries
for q in queries:
  try: 
    q.awaitTermination()
    print(f"🟢 ingestion '{q.name}' completed")
  except:
    print(f"🟠 ingestion '{q.name}' failed")