# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Proyecto BASAV — Componente Big Data (PySpark / Databricks)
# MAGIC Pipeline Medallion (Bronze → Silver → Gold) sobre el dataset Online Retail, segmentacion RFM de clientes y 5 experimentos de rendimiento (EXP-02 a EXP-06).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze — Ingesta del dataset crudo
# MAGIC Lectura del archivo `Online_Retail.csv` desde el Volumen y guardado como tabla Delta sin transformar.

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/online_retail/a/Onlineretail/Online_Retail.csv

# COMMAND ----------

dfOnlineRetail= spark.read.csv("/Volumes/workspace/online_retail/a/Onlineretail/Online_Retail.csv", header=True, inferSchema=True, sep= ";")

# COMMAND ----------

display(dfOnlineRetail)

# COMMAND ----------

dfOnlineRetail.write.saveAsTable("online_retail.retail")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. EDA — Exploracion inicial
# MAGIC Conteos de filas, valores nulos y revision de formatos antes de limpiar.

# COMMAND ----------

df = spark.table("online_retail.retail")

df.printSchema()
df.describe().display()

from pyspark.sql.functions import col, count, when
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).display()

df.select("Country").distinct().count()
df.select("StockCode").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver — Limpieza del dataset
# MAGIC Se eliminan filas con precio unitario menor o igual a cero y se guarda como `retail_clean`.

# COMMAND ----------

from pyspark.sql.functions import col, when, substring, to_timestamp

df = spark.table("online_retail.retail")

df_clean = (df
    .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "d/M/yyyy H:mm"))
    .withColumn("tipo_transaccion", when(substring(col("InvoiceNo"), 1, 1) == "C", "Cancelacion").otherwise("Venta"))
    .withColumn("CustomerID", when(col("CustomerID").isNull(), "Desconocido").otherwise(col("CustomerID").cast("string")))
    .na.fill("Sin descripcion", subset=["Description"])
    .withColumn("importe", col("Quantity") * col("UnitPrice"))
    .filter(col("UnitPrice") > 0)
    .withColumnRenamed("CustomerID", "id_cliente")
)

df_clean.write.mode("overwrite").saveAsTable("online_retail.retail_clean")

df_clean.printSchema()
df_clean.filter(col("InvoiceDate").isNull()).count()
df_clean.count()

# COMMAND ----------

print(df_clean.filter(col("InvoiceDate").isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold — Segmentacion RFM de clientes
# MAGIC Calculo de Recencia, Frecuencia y Monto por cliente, scoring por cuartiles (`ntile`) y asignacion de segmento. Resultado guardado como `retail_rfm`.

# COMMAND ----------

from pyspark.sql.functions import col, max as _max, countDistinct, sum as _sum, datediff, lit

df_ventas = df_clean.filter((col("tipo_transaccion") == "Venta") & (col("id_cliente") != "Desconocido"))

fecha_maxima = df_ventas.select(_max("InvoiceDate")).first()[0]

df_rfm = (df_ventas
    .groupBy("id_cliente")
    .agg(
        datediff(lit(fecha_maxima), _max("InvoiceDate")).alias("recencia_dias"),
        countDistinct("InvoiceNo").alias("frecuencia"),
        _sum("importe").alias("monto")
    )
)

df_rfm.display()

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import ntile, col, when

w_recencia = Window.orderBy(col("recencia_dias").asc())
w_frecuencia = Window.orderBy(col("frecuencia").desc())
w_monto = Window.orderBy(col("monto").desc())

df_rfm_score = (df_rfm
    .withColumn("r_score", 5 - ntile(4).over(w_recencia))
    .withColumn("f_score", 5 - ntile(4).over(w_frecuencia))
    .withColumn("m_score", 5 - ntile(4).over(w_monto))
    .withColumn("score_total", (5 - ntile(4).over(w_recencia)) + (5 - ntile(4).over(w_frecuencia)) + (5 - ntile(4).over(w_monto)))
)

df_rfm_final = df_rfm_score.withColumn("segmento",
    when(col("score_total") >= 10, "Campeones")
    .when(col("score_total") >= 7, "Leales")
    .when(col("score_total") >= 4, "En riesgo")
    .otherwise("Perdidos")
)

df_rfm_final.write.mode("overwrite").saveAsTable("online_retail.retail_rfm")
df_rfm_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Visualizaciones RFM — Dashboard
# MAGIC
# MAGIC

# COMMAND ----------

df_resumen_segmentos = (df_rfm_final
    .groupBy("segmento")
    .count()
    .orderBy(col("count").desc())
)

df_resumen_segmentos.display()

# COMMAND ----------

from pyspark.sql.functions import avg, round as _round

df_monto_segmento = (df_rfm_final
    .groupBy("segmento")
    .agg(_round(avg("monto"), 2).alias("monto_promedio"))
    .orderBy(col("monto_promedio").desc())
)

df_monto_segmento.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Experimentos de rendimiento (EXP-02 a EXP-06)
# MAGIC Medicion de tiempo antes y despues de cada cambio. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 EXP-02 — Volumen de datos (x1 vs x5)

# COMMAND ----------

import time
from pyspark.sql.functions import col, max as _max, countDistinct, sum as _sum, datediff, lit

inicio = time.time()

df_ventas_base = df_clean.filter((col("tipo_transaccion") == "Venta") & (col("id_cliente") != "Desconocido"))
fecha_maxima_base = df_ventas_base.select(_max("InvoiceDate")).first()[0]

df_rfm_base = (df_ventas_base
    .groupBy("id_cliente")
    .agg(
        datediff(lit(fecha_maxima_base), _max("InvoiceDate")).alias("recencia_dias"),
        countDistinct("InvoiceNo").alias("frecuencia"),
        _sum("importe").alias("monto")
    )
)
df_rfm_base.count()

fin = time.time()
t_vol_x1 = fin - inicio
print(f"Volumen x1 ({df_clean.count()} filas): {t_vol_x1:.2f} segundos")

# COMMAND ----------

from pyspark.sql.functions import concat, lit

def duplicar_con_sufijo(df, sufijo):
    return (df
        .withColumn("InvoiceNo", concat(col("InvoiceNo"), lit(sufijo)))
        .withColumn("id_cliente", concat(col("id_cliente"), lit(sufijo)))
    )

df_x5 = df_clean
for i in range(1, 5):
    df_x5 = df_x5.unionByName(duplicar_con_sufijo(df_clean, f"_v{i}"))

df_x5.write.mode("overwrite").saveAsTable("online_retail.retail_clean_x5")
print(f"Volumen x5: {df_x5.count()} filas")

# COMMAND ----------

inicio = time.time()

df_ventas_x5 = df_x5.filter((col("tipo_transaccion") == "Venta") & (col("id_cliente") != "Desconocido"))
fecha_maxima_x5 = df_ventas_x5.select(_max("InvoiceDate")).first()[0]

df_rfm_x5 = (df_ventas_x5
    .groupBy("id_cliente")
    .agg(
        datediff(lit(fecha_maxima_x5), _max("InvoiceDate")).alias("recencia_dias"),
        countDistinct("InvoiceNo").alias("frecuencia"),
        _sum("importe").alias("monto")
    )
)
df_rfm_x5.count()

fin = time.time()
t_vol_x5 = fin - inicio
print(f"Volumen x5 ({df_x5.count()} filas): {t_vol_x5:.2f} segundos")

# COMMAND ----------

display(spark.createDataFrame([("x1", t_vol_x1), ("x5", t_vol_x5)], ["condicion", "tiempo_segundos"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 EXP-03 — Uso de cache (.cache())

# COMMAND ----------

import time
from pyspark.sql.functions import col, max as _max, countDistinct, sum as _sum, datediff, lit

df_ventas = df_clean.filter((col("tipo_transaccion") == "Venta") & (col("id_cliente") != "Desconocido"))

inicio = time.time()
for repeticion in range(3):
    fecha_maxima = df_ventas.select(_max("InvoiceDate")).first()[0]
    df_rfm = (df_ventas
        .groupBy("id_cliente")
        .agg(
            datediff(lit(fecha_maxima), _max("InvoiceDate")).alias("recencia_dias"),
            countDistinct("InvoiceNo").alias("frecuencia"),
            _sum("importe").alias("monto")
        )
    )
    df_rfm.count()
fin = time.time()
t_sin_cache = fin - inicio
print(f"SIN cache, 3 repeticiones: {t_sin_cache:.2f} segundos")

# COMMAND ----------

df_ventas_cache = (df_clean
    .filter((col("tipo_transaccion") == "Venta") & (col("id_cliente") != "Desconocido"))
    .cache()
)
df_ventas_cache.count()  # esto fuerza a que se guarde en memoria ahora, no despues

inicio = time.time()
for repeticion in range(3):
    fecha_maxima = df_ventas_cache.select(_max("InvoiceDate")).first()[0]
    df_rfm_cache = (df_ventas_cache
        .groupBy("id_cliente")
        .agg(
            datediff(lit(fecha_maxima), _max("InvoiceDate")).alias("recencia_dias"),
            countDistinct("InvoiceNo").alias("frecuencia"),
            _sum("importe").alias("monto")
        )
    )
    df_rfm_cache.count()
fin = time.time()
t_con_cache = fin - inicio
print(f"CON cache, 3 repeticiones: {t_con_cache:.2f} segundos")

df_ventas_cache.unpersist()

# COMMAND ----------

display(spark.createDataFrame([("Sin cache", t_sin_cache), ("Con cache", t_con_cache)], ["condicion", "tiempo_segundos"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 EXP-04 — Window sin particion (x1 vs x5 clientes)

# COMMAND ----------

import time
from pyspark.sql import Window
from pyspark.sql.functions import ntile, col, when, max as _max, countDistinct, sum as _sum, datediff, lit

def calcular_rfm_base(df, nombre):
    df_ventas = df.filter((col("tipo_transaccion") == "Venta") & (col("id_cliente") != "Desconocido"))
    fecha_maxima = df_ventas.select(_max("InvoiceDate")).first()[0]
    df_rfm = (df_ventas
        .groupBy("id_cliente")
        .agg(
            datediff(lit(fecha_maxima), _max("InvoiceDate")).alias("recencia_dias"),
            countDistinct("InvoiceNo").alias("frecuencia"),
            _sum("importe").alias("monto")
        )
    )
    print(f"{nombre}: {df_rfm.count()} clientes")
    return df_rfm

df_rfm_x1 = calcular_rfm_base(df_clean, "Volumen x1")

df_x5 = spark.table("online_retail.retail_clean_x5")
df_rfm_x5 = calcular_rfm_base(df_x5, "Volumen x5")

# COMMAND ----------

def medir_segmentacion(df_rfm, nombre):
    inicio = time.time()

    w_recencia = Window.orderBy(col("recencia_dias").asc())
    w_frecuencia = Window.orderBy(col("frecuencia").desc())
    w_monto = Window.orderBy(col("monto").desc())

    df_rfm_score = df_rfm.withColumn(
        "score_total",
        (5 - ntile(4).over(w_recencia)) + (5 - ntile(4).over(w_frecuencia)) + (5 - ntile(4).over(w_monto))
    )
    df_rfm_score.count()

    fin = time.time()
    tiempo = fin - inicio
    print(f"{nombre} ({df_rfm.count()} clientes): {tiempo:.2f} segundos")
    return tiempo

t_window_x1 = medir_segmentacion(df_rfm_x1, "Segmentacion Window, volumen x1")
t_window_x5 = medir_segmentacion(df_rfm_x5, "Segmentacion Window, volumen x5")

# COMMAND ----------

display(spark.createDataFrame([("x1 (4,338 clientes)", t_window_x1), ("x5 (21,694 clientes)", t_window_x5)], ["condicion", "tiempo_segundos"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 EXP-05 — Formato de almacenamiento (CSV vs Delta)

# COMMAND ----------

import time

inicio = time.time()
df_csv = spark.read.csv("/Volumes/workspace/online_retail/a/Onlineretail/Online_Retail.csv", header=True, inferSchema=True, sep=";")
conteo_csv = df_csv.count()
fin = time.time()
t_csv = fin - inicio
print(f"Lectura CSV crudo: {t_csv:.2f} segundos ({conteo_csv} filas)")

# COMMAND ----------

inicio = time.time()
df_delta = spark.table("online_retail.retail_clean")
conteo_delta = df_delta.count()
fin = time.time()
t_delta = fin - inicio
print(f"Lectura tabla Delta: {t_delta:.2f} segundos ({conteo_delta} filas)")

# COMMAND ----------

display(spark.createDataFrame([("CSV crudo", t_csv), ("Tabla Delta", t_delta)], ["condicion", "tiempo_segundos"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 EXP-06 — Numero de particiones (5 vs 2)

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

df_x5 = spark.table("online_retail.retail_clean_x5")
particiones_default = df_x5.select(spark_partition_id()).distinct().count()
print(f"Particiones por defecto: {particiones_default}")

# COMMAND ----------

import time
from pyspark.sql.functions import col, max as _max, countDistinct, sum as _sum, datediff, lit, spark_partition_id

def calcular_rfm_tiempo(df, nombre):
    inicio = time.time()
    df_ventas = df.filter((col("tipo_transaccion") == "Venta") & (col("id_cliente") != "Desconocido"))
    fecha_maxima = df_ventas.select(_max("InvoiceDate")).first()[0]
    df_rfm = (df_ventas
        .groupBy("id_cliente")
        .agg(
            datediff(lit(fecha_maxima), _max("InvoiceDate")).alias("recencia_dias"),
            countDistinct("InvoiceNo").alias("frecuencia"),
            _sum("importe").alias("monto")
        )
    )
    df_rfm.count()
    fin = time.time()
    tiempo = fin - inicio
    particiones = df.select(spark_partition_id()).distinct().count()
    print(f"{nombre} ({particiones} particiones): {tiempo:.2f} segundos")
    return tiempo

t_part_default = calcular_rfm_tiempo(df_x5, "Particiones por defecto")

# COMMAND ----------

df_x5_pocas_particiones = df_x5.repartition(2)
t_part_2 = calcular_rfm_tiempo(df_x5_pocas_particiones, "Forzado a 2 particiones")

# COMMAND ----------

display(spark.createDataFrame([("5 particiones (defecto)", t_part_default), ("2 particiones (forzado)", t_part_2)], ["condicion", "tiempo_segundos"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificacion final
# MAGIC Consultas de control sobre `retail_rfm` para confirmar que la segmentacion quedo correcta antes de cerrar el notebook.

# COMMAND ----------

spark.table("online_retail.retail_rfm").groupBy("segmento").count().orderBy(col("count").desc()).display()