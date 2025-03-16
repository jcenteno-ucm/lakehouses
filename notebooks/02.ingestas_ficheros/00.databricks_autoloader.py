# Databricks notebook source
# MAGIC %md
# MAGIC # 📌 Databricks Auto Loader
# MAGIC
# MAGIC ## 🔹 ¿Qué es Auto Loader?
# MAGIC Auto Loader es una herramienta en Databricks que permite la ingesta continua de datos desde almacenamiento en la nube (Azure Blob Storage, AWS S3, Google Cloud Storage, etc.). Utiliza **esquema evolutivo**, procesamiento incremental y es altamente escalable.
# MAGIC
# MAGIC [Opciones] (https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)
# MAGIC
# MAGIC
# MAGIC ## ⚙️ **Opciones principales de Auto Loader**
# MAGIC
# MAGIC ### 1️⃣ **Opciones de origen (`source`)**
# MAGIC Auto Loader admite múltiples formatos de archivos:
# MAGIC - **`cloudFiles.format`** → Define el formato de los archivos.
# MAGIC   - Valores: `csv`, `json`, `parquet`, `avro`, `orc`, `text`.
# MAGIC   
# MAGIC Ejemplo:
# MAGIC ```python
# MAGIC spark.readStream.format("cloudFiles") \
# MAGIC   .option("cloudFiles.format", "json") \
# MAGIC   .load("s3://bucket/data/")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 2️⃣ **Opciones de detección de archivos (`file detection`)**
# MAGIC
# MAGIC - **`cloudFiles.schemaLocation`** → Ubicación donde se guarda el esquema detectado.
# MAGIC - **`cloudFiles.maxFilesPerTrigger`** → Número máximo de archivos procesados en cada batch.
# MAGIC - **`cloudFiles.includeExistingFiles`** → Si `true`, procesa archivos existentes al inicio.
# MAGIC - **`cloudFiles.allowOverwrites`** → Si `true`, permite la sobreescritura de archivos.
# MAGIC
# MAGIC Ejemplo:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaLocation", "dbfs:/schemas/ingestion/") \
# MAGIC .option("cloudFiles.includeExistingFiles", "true")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 3️⃣ **Opciones de inferencia de esquema (`schema inference`)**
# MAGIC
# MAGIC - **`cloudFiles.inferColumnTypes`** → Convierte automáticamente valores numéricos y booleanos detectados como string.
# MAGIC - **`cloudFiles.schemaEvolutionMode`** → Permite manejar cambios en el esquema.
# MAGIC   - `failOnNewColumns`: Falla si se detectan nuevas columnas.
# MAGIC   - `addNewColumns`: Agrega nuevas columnas automáticamente.
# MAGIC   
# MAGIC Ejemplo:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 4️⃣ **Opciones de filtrado y exclusión de archivos (`file filtering`)**
# MAGIC
# MAGIC - **`cloudFiles.pathGlobFilter`** → Filtra archivos por nombre usando expresiones regulares.
# MAGIC - **`cloudFiles.excludePattern`** → Excluye archivos que coincidan con un patrón específico.
# MAGIC
# MAGIC Ejemplo:
# MAGIC ```python
# MAGIC .option("cloudFiles.pathGlobFilter", "*.json") \
# MAGIC .option("cloudFiles.excludePattern", "*_backup.json")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### 5️⃣ **Opciones de rendimiento (`performance tuning`)**
# MAGIC
# MAGIC - **`cloudFiles.useNotifications`** → Usa eventos de almacenamiento para detectar cambios en lugar de escanear.
# MAGIC - **`cloudFiles.queueName`** → Nombre de la cola de mensajes para recibir eventos de cambios en el almacenamiento.
# MAGIC - **`cloudFiles.backfillInterval`** → Intervalo de reanálisis de archivos nuevos.
# MAGIC
# MAGIC Ejemplo:
# MAGIC ```python
# MAGIC .option("cloudFiles.useNotifications", "true") \
# MAGIC .option("cloudFiles.queueName", "storage-event-queue")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### 6️⃣ **Opciones de manejo de errores (`error handling`)**
# MAGIC
# MAGIC - **`cloudFiles.schemaHints`** → Define manualmente el esquema esperado.
# MAGIC - **`cloudFiles.ignoreCorruptFiles`** → Ignora archivos corruptos.
# MAGIC - **`cloudFiles.ignoreMissingFiles`** → Ignora archivos eliminados mientras el proceso de ingesta está en curso.
# MAGIC
# MAGIC Ejemplo:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaHints", "id LONG, name STRING, timestamp TIMESTAMP") \
# MAGIC .option("cloudFiles.ignoreCorruptFiles", "true")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ## ✅ **Ejemplo completo de Auto Loader**
# MAGIC
# MAGIC ```python
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC   .option("cloudFiles.format", "json") \
# MAGIC   .option("cloudFiles.schemaLocation", "dbfs:/schemas/ingestion/") \
# MAGIC   .option("cloudFiles.inferColumnTypes", "true") \
# MAGIC   .option("cloudFiles.includeExistingFiles", "true") \
# MAGIC   .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
# MAGIC   .load("s3://my-bucket/data/")
# MAGIC
# MAGIC df.writeStream.format("delta") \
# MAGIC   .option("checkpointLocation", "dbfs:/checkpoints/data/") \
# MAGIC   .start("dbfs:/delta/data/")
# MAGIC ```
# MAGIC