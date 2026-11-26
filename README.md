# Base de Datos Avanzada y Big Data 🗃️

Proyecto final del curso BASAV (Base de Datos Avanzadas y Big Data), UPN. Implementa dos soluciones complementarias sobre el dataset público **Online Retail** (UCI Machine Learning Repository): una de Inteligencia de Negocios con un datamart en SQL Server, un proceso ETL propio y un tablero en Power BI, y una de Big Data con un pipeline en Databricks (PySpark) para segmentación de clientes a mayor volumen.

La solución transforma los registros transaccionales de una tienda minorista en línea (facturas, productos, clientes, países) en indicadores verificables, para responder qué productos y clientes generan más valor y detectar a tiempo el riesgo de abandono, tanto con herramientas tradicionales de BI como con procesamiento distribuido.

## Características Principales ✨

- **Datamart en SQL Server:** esquema estrella con una tabla de hechos y cinco dimensiones, diseñado con la metodología de Kimball, acotado al proceso de negocio de ventas.
- **ETL en T-SQL:** extracción del dataset a staging, limpieza, transformación y carga de dimensiones y hechos, verificado sobre el total de 541.909 registros.
- **Dashboard en Power BI:** 10 KPI de negocio distribuidos en 3 páginas (desempeño general, productos y mercados, clientes), con slicers de tiempo, país y producto para análisis OLAP.
- **Pipeline Big Data en Databricks:** arquitectura Medallion (Bronze/Silver/Gold) sobre PySpark, con segmentación de clientes tipo RFM (Recencia, Frecuencia, Monto) y un dashboard propio con 7 visualizaciones.
- **Experimentación reproducible:** 10 experimentos documentados en total, 5 sobre el datamart de SQL Server (volumen, tabla de agregados, índice por fecha, modelo estrella vs. tabla plana, e índices generales) y 5 sobre el pipeline de Databricks (volumen, caché, funciones Window, formato de almacenamiento y número de particiones), uno por integrante del equipo en cada solución.

## Tecnologías Utilizadas 🛠️

- **Motor de base de datos:** Microsoft SQL Server 2022
- **Administración:** SQL Server Management Studio (SSMS) 20
- **ETL:** T-SQL (procedimientos almacenados)
- **Visualización:** Power BI Desktop
- **Modelado dimensional:** esquema estrella (metodología Kimball)
- **Big Data:** Databricks (Serverless, Unity Catalog), PySpark, Delta Lake
- **Dataset de origen:** [Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) (UCI Machine Learning Repository), licencia CC BY 4.0

## Modelo de Datos 🗂️

Esquema estrella con una tabla de hechos y cinco dimensiones:

- **fac_ventas** (hechos): grano de línea de factura consolidada por producto y precio unitario. Medidas: `cantidadVendida`, `precioUnitarioVenta`, `importeVendido`. Incluye la dimensión degenerada `numeroFacturaVenta`.
- **dim_cliente, dim_producto, dim_pais, dim_tiempo, dim_tipotransaccion**: atributos descriptivos de cliente, producto, país, fecha y tipo de transacción (venta o cancelación).

El diagrama completo del modelo está en el informe (sección 3.4.2). La contraparte en Databricks sigue la arquitectura Medallion: Bronze (online_retail.retail, datos crudos), Silver (online_retail.retail_clean, datos limpios y tipados) y Gold (online_retail.retail_rfm, segmentación RFM final).

## Estructura del Repositorio 📁

```
/dataset
    Online Retail.xlsx           Dataset original completo, tal como se descargó de UCI
    onlineretail.txt             Archivo usado en el BULK INSERT del Script 2

/scripts
    script_1_creacion_bd.sql     Creación de la base dw_onlineretail y las 7 tablas
    script_2_etl.sql             ETL completo: staging, carga de dimensiones y de fac_ventas
    experimentos_bi.sql          Consultas de los 5 experimentos BI (EXP-01, EXP-07 a EXP-10)

/power-bi
    dashboard_bi.pbix            Tablero de Power BI conectado a dw_onlineretail

/documentacion
    informe_final.docx                               Informe completo del proyecto
    informe_final.pdf                                Mismo informe, exportado a PDF
    Word_Evidencias_Proyecto.docx                    Capturas y evidencias de ejecución, organizadas por código (E-01 a E-16)
    Procedimiento_de_Replicacion_BASAV_Completo.docx Guía paso a paso para replicar ambas soluciones (BI y Big Data), con las capturas que confirman cada paso
    Explicacion_Experimentos_BI_BASAV.docx           Objetivo, procedimiento, condiciones, resultados y conclusión de cada experimento BI, para exponer en clase

/big-data
    OnlineRetailCompleto.py      Notebook de Databricks, ordenado por secciones (Bronze, EDA, Silver, Gold/RFM, visualizaciones y los 5 experimentos EXP-02 a EXP-06)

README.md
```

## Requisitos 📋

- SQL Server 2019 o superior (o SQL Server Express).
- SQL Server Management Studio (SSMS) para correr los scripts.
- Power BI Desktop para abrir el dashboard.
- Cuenta de Databricks (Free Edition o superior) para importar y ejecutar el notebook de Big Data.

## Configuración 🚀

### Solución de Inteligencia de Negocios

1. Clonar el repositorio:
   ```bash
   git clone https://github.com/Juanhp07/Base-de-datos-Avanzada-y-Big-Data-Proyecto.git
   ```
2. Ejecutar `scripts/script_1_creacion_bd.sql` en SSMS: crea la base `dw_onlineretail` y las 7 tablas.
3. Ejecutar `scripts/script_2_etl.sql`: extrae el dataset a staging, carga las 5 dimensiones y después `fac_ventas`.
4. Abrir `power-bi/dashboard_bi.pbix` en Power BI Desktop y actualizar la conexión a la base local.
5. Opcional: ejecutar `scripts/experimentos_bi.sql` para reproducir los 5 experimentos de rendimiento BI.

### Solución Big Data

1. En Databricks, crear el catálogo `workspace` (si no existe por defecto), el schema `online_retail`, el Volumen `a` y, dentro de este, la carpeta `Onlineretail`.
2. Subir a esa carpeta del Volumen un CSV exportado desde `dataset/Online Retail.xlsx`, nombrado exactamente `Online_Retail.csv` (la ruta final debe quedar `/Volumes/workspace/online_retail/a/Onlineretail/Online_Retail.csv`).
3. Importar `big-data/OnlineRetailCompleto.py` como notebook y ejecutar "Run all".
4. El notebook crea las tablas `retail`, `retail_clean`, `retail_clean_x5` y `retail_rfm`, genera las visualizaciones RFM y corre los 5 experimentos de rendimiento (EXP-02 a EXP-06). Si al reejecutar aparece un error de tabla ya existente en el paso de `saveAsTable`, es esperado y no afecta el resto del notebook.

Para el detalle completo de ambos procesos, con capturas de cada paso, ver `documentacion/Procedimiento_de_Replicacion_BASAV_Completo.docx`.

## KPI del Dashboard 📊

El dashboard de Power BI responde 10 KPI de negocio: ingresos brutos y netos, monto cancelado, ticket promedio, tasa de cancelación, top 10 productos, clientes activos, tasa de recompra, ingresos por país y riesgo de abandono por recencia. El detalle de cada uno, con su fórmula DAX y el gráfico elegido, está documentado para el equipo en una guía interna aparte (no incluida en este repositorio público).

El dashboard de Databricks ("Online retail dashboard") agrega 7 visualizaciones: 2 de la segmentación RFM (clientes por segmento y monto promedio por segmento) y 5 con los resultados de los experimentos EXP-02 a EXP-06.

## Dataset de Origen 📦

Online Retail Dataset, UCI Machine Learning Repository, licencia CC BY 4.0. Se usa con fines académicos, citando la fuente original en el informe (sección 1.6).

En `dataset/Online Retail.xlsx` está el archivo original completo, tal como se descargó de UCI, con todas sus columnas y registros. De ahí sale el `onlineretail.txt` que se usa en el BULK INSERT del Script 2, y el CSV que se exporta y se sube al Volumen de Databricks. Ambas soluciones, el modelo dimensional y el ETL de SQL Server y el pipeline de PySpark, parten únicamente de esta información, sin datos agregados ni modificados a mano.

## Equipo 👥

Proyecto colaborativo, curso BASAV, UPN.
