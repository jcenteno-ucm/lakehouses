# Databricks notebook source
# MAGIC %md
# MAGIC # 🧊 Apache Iceberg 
# MAGIC
# MAGIC <img src="https://iceberg.apache.org/assets/images/Iceberg-logo.svg" width="400"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 🎯 Objetivo
# MAGIC Familiarizarse con las capacidades únicas de Apache Iceberg mediante ejemplos prácticos usando PySpark
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #1. 📦 Entorno
# MAGIC
# MAGIC Este notebook se ha ejecutado en el siguiente entorno
# MAGIC
# MAGIC - Runtime: DBR 16.4 LTS (Spark 3.5.2)
# MAGIC
# MAGIC - Librerias: 
# MAGIC
# MAGIC (org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Configuración Iceberg Catalog**
# MAGIC
# MAGIC En este caso, haremos uso de la implementación del catálogo basada en sistemas de ficheros (Hadoop)
# MAGIC
# MAGIC Para ello vamos a crear una carpeta dedicada en nuestra cuenta de almacenamiento 

# COMMAND ----------

catalog_name = "iceberg_catalog"

container = "datos"
account = spark.conf.get("adls.account.name")
storage_path = f"abfss://{container}@{account}.dfs.core.windows.net/{catalog_name}"

print(storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Configuramos el catálogo a través de la configuración de la SparkSession.
# MAGIC
# MAGIC No obstante, esta configuración se podría configurar directamente como propiedades en la configuración del cluster

# COMMAND ----------

spark.conf.set(f"spark.sql.catalog.{catalog_name}","org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{catalog_name}.type","hadoop")
spark.conf.set(f"spark.sql.catalog.{catalog_name}.warehouse",storage_path)

# COMMAND ----------

print(spark.conf.get("spark.sql.extensions"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS iceberg_catalog.default.empleados;
# MAGIC DROP DATABASE IF EXISTS iceberg_catalog.default;
# MAGIC CREATE SCHEMA IF NOT EXISTS iceberg_catalog.default;

# COMMAND ----------

# MAGIC %md
# MAGIC Verificamos que el catálogo iceberg está disponible

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC Seleccionamos el catalogo iceberg para que sea nuestro catalogo por defecto

# COMMAND ----------

# MAGIC %sql
# MAGIC USE iceberg_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos que el schema default existe

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC #2. 🧱 Crear una tabla Iceberg

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE default.empleados (
# MAGIC   id INT,
# MAGIC   nombre STRING,
# MAGIC   departamento STRING,
# MAGIC   salario DOUBLE,
# MAGIC   fecha_ingreso DATE
# MAGIC )
# MAGIC USING iceberg
# MAGIC PARTITIONED BY (YEAR(fecha_ingreso));

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Comprobación: Revisa que metadatos se han creado
# MAGIC
# MAGIC - v1.metadata.json
# MAGIC - version-hint.text
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED default.empleados;

# COMMAND ----------

# MAGIC %md
# MAGIC #3. 📝 Insertar datos

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO default.empleados VALUES
# MAGIC (1, 'Ana', 'IT', 50000,    TO_DATE('2025-06-01','yyyy-MM-dd')),
# MAGIC (2, 'Luis', 'RRHH', 40000, TO_DATE('2025-06-12','yyyy-MM-dd')),
# MAGIC (3, 'Marta', 'IT', 52000,  TO_DATE('2025-06-10','yyyy-MM-dd'));
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Comprobación: Revisa que metadatos y los datos se han creado

# COMMAND ----------

# MAGIC %md
# MAGIC #4. 🔍 Select
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre, salario 
# MAGIC FROM default.empleados 
# MAGIC WHERE departamento = 'IT';

# COMMAND ----------

# MAGIC %md
# MAGIC #5. ♻️ Update y Delete

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE default.empleados
# MAGIC SET salario = salario * 1.10
# MAGIC WHERE departamento = 'IT';
# MAGIC
# MAGIC DELETE FROM default.empleados
# MAGIC WHERE nombre = 'Luis';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.empleados

# COMMAND ----------

# MAGIC %md
# MAGIC #6. 🧮 Merge Into (upserts)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO default.empleados t
# MAGIC USING (
# MAGIC   SELECT 2 as id, 
# MAGIC         'Luis' as nombre, 
# MAGIC         'Ventas' as departamento, 
# MAGIC         43000 as salario,  
# MAGIC         TO_DATE('2025-06-20','yyyy-MM-dd') as fecha_ingreso
# MAGIC ) s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM default.empleados

# COMMAND ----------

# MAGIC %md
# MAGIC #7. 🔄 Schema Evolution

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE default.empleados ADD COLUMN edad INT;
# MAGIC
# MAGIC INSERT INTO default.empleados VALUES (4, 'Carlos', 'Marketing', 48000, TO_DATE('2023-01-01','yyyy-MM-dd'), 35);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.empleados;

# COMMAND ----------

# MAGIC %md 
# MAGIC #8. ⚡️ Unificacion Batch y Streaming
# MAGIC

# COMMAND ----------

# Acceder a la clase Trigger a través de JVM
Trigger = spark._jvm.org.apache.spark.sql.streaming.Trigger

empleados_stream = (spark.readStream
                          .option("stream-from", "LATEST")
                          .option("streaming-skip-overwrite-snapshots","true")
                          .table("iceberg_catalog.default.empleados")
                    )

empleados_stream.createOrReplaceTempView("empleados_stream")

# Kick off the stream
display(spark.sql("""SELECT departamento, sum(`salario`) AS total FROM empleados_stream GROUP BY departamento"""),
        streamName="salarios_por_departamento", 
        trigger = Trigger.ProcessingTime("5 seconds")
)

# COMMAND ----------

# MAGIC %md **Esperamos** hasta que el stream arranque para ejecutar la siguiente celda

# COMMAND ----------

import time
import random

id = spark.sql("select max(id) as id from default.empleados").head()['id']

nombres = ["Jorge", "Sofia", "Alberto", "Pablo"]
departamentos = ['IT','Marketing']

for i in range (1, 6):
  dml_sql = f"""
    INSERT INTO default.empleados VALUES ({id}, '{random.choice(nombres)}', '{random.choice(departamentos)}', 48000, CURRENT_DATE(), 35);    ;
  """
  spark.sql(dml_sql)
  print(f"loop: [{i}]")    
  time.sleep(5)
  id+=1

# COMMAND ----------

# MAGIC %md
# MAGIC Observa como la streaming query devuelve los datos actualizados

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos los registros añadidos a la tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.empleados order by 1;

# COMMAND ----------

# MAGIC %md 
# MAGIC **NOTA**: No te olvides de para la celda para detener la streaming query.

# COMMAND ----------

# MAGIC %md 
# MAGIC #9. 🕖 Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC Consulta los **snapshots** actuales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.empleados.history
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md 
# MAGIC **Time Travel via snapshot**
# MAGIC
# MAGIC Seleccionemos uno de los snapshots id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.empleados
# MAGIC VERSION AS OF 8157665910848104786;

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel via timestamp**
# MAGIC
# MAGIC Seleccionemos mediante un instante concreto
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.empleados
# MAGIC TIMESTAMP AS OF '2025-06-26 17:23:30.000'

# COMMAND ----------

# MAGIC %md
# MAGIC # 10. 📊 Metadata Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Consulta las **particiones**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.empleados.partitions;

# COMMAND ----------

# MAGIC %md
# MAGIC Consulta los **snapshots**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.empleados.snapshots;

# COMMAND ----------

# MAGIC %md
# MAGIC Consulta los **manifests**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.empleados.manifests;

# COMMAND ----------

# MAGIC %md
# MAGIC Consultas los metadatos **log entries**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.empleados.metadata_log_entries;

# COMMAND ----------

# MAGIC %md
# MAGIC Listemos el contenido de la carpeta de metadatos

# COMMAND ----------

metadata_table_path = storage_path + "/default/empleados/metadata"
display(dbutils.fs.ls(metadata_table_path))

# COMMAND ----------

snapshots = [e.name for e in dbutils.fs.ls(metadata_table_path) if "snap" in e.name]

print(snapshots)

# COMMAND ----------

snapshot_file = random.choice(snapshots)
snapshot_path = f"abfss://{container}@{account}.dfs.core.windows.net/{catalog_name}/default/empleados/metadata/{snapshot_file}"
dbutils.fs.ls(snapshot_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Inspeccion del contenido del **snapshot**

# COMMAND ----------

display(spark.read.format("avro").load(snapshot_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Inspección del contenido **manifest**

# COMMAND ----------

manifests = [e.name for e in dbutils.fs.ls(metadata_table_path) if ".avro" in e.name and not e.name.lower().startswith("snap")]

print(manifests)


# COMMAND ----------

manifest_file = random.choice(manifests)
manifest_path = f"abfss://{container}@{account}.dfs.core.windows.net/{catalog_name}/default/empleados/metadata/{manifest_file}"

display(spark.read.format("avro").load(manifest_path))

# COMMAND ----------

display(spark.read.format("avro").load(manifest_path).select("data_file.*"))

# COMMAND ----------

# MAGIC %md
# MAGIC #11. 📚 Comparativa con Delta Lake y Hive
# MAGIC
# MAGIC
# MAGIC | Feature                | Iceberg | Delta  | Hive |
# MAGIC | ---------------------- | --------- | ------- | ------ |
# MAGIC | Schema Evolution | ✅         | ✅       | ⚠️     |
# MAGIC | Time travel            | ✅         | ✅       | ❌      |
# MAGIC | Update/Delete ACID     | ✅         | ✅       | ⚠️     |
# MAGIC | Partition Evolution    | ✅         | ❌       | ❌      |
# MAGIC | Metadata en Catálogo   | ✅         | ❌       | ❌      |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #12. 🧹Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.empleados;

# COMMAND ----------

dbutils.fs.rm(f'{storage_path}/',True)