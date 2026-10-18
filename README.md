# Base de Datos Avanzada y Big Data 🗃️

Proyecto final del curso BASAV (Base de Datos Avanzadas y Big Data), UPN. Implementa una solución de Inteligencia de Negocios sobre el dataset público **Online Retail** (UCI Machine Learning Repository), con un datamart en SQL Server, un proceso ETL propio y un tablero en Power BI.

La solución transforma los registros transaccionales de una tienda minorista en línea (facturas, productos, clientes, países) en indicadores verificables, para responder qué productos y clientes generan más valor y detectar a tiempo el riesgo de abandono.

## Características Principales ✨

- **Datamart en SQL Server:** esquema estrella con una tabla de hechos y cinco dimensiones, diseñado con la metodología de Kimball, acotado al proceso de negocio de ventas.
- **ETL en T-SQL:** extracción del dataset a staging, limpieza, transformación y carga de dimensiones y hechos, verificado sobre el total de 541.909 registros.
- **Dashboard en Power BI:** 10 KPI de negocio distribuidos en 3 páginas (desempeño general, productos y mercados, clientes), con slicers de tiempo, país y producto para análisis OLAP.
- **Experimentación reproducible:** diseño experimental para medir tiempos de ETL y desempeño de consultas analíticas sobre el datamart cargado.
- **Big Data (fase posterior):** segmentación de clientes tipo RFM sobre un volumen mayor de datos, a desarrollar cuando el curso cubra esa unidad.

## Tecnologías Utilizadas 🛠️

- **Motor de base de datos:** Microsoft SQL Server 2022
- **Administración:** SQL Server Management Studio (SSMS) 20
- **ETL:** T-SQL (procedimientos almacenados)
- **Visualización:** Power BI Desktop
- **Modelado dimensional:** esquema estrella (metodología Kimball)
- **Dataset de origen:** [Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) (UCI Machine Learning Repository), licencia CC BY 4.0

## Modelo de Datos 🗂️

Esquema estrella con una tabla de hechos y cinco dimensiones:

- **fac_ventas** (hechos): grano de línea de factura consolidada por producto y precio unitario. Medidas: `cantidadVendida`, `precioUnitarioVenta`, `importeVendido`. Incluye la dimensión degenerada `numeroFacturaVenta`.
- **dim_cliente, dim_producto, dim_pais, dim_tiempo, dim_tipotransaccion**: atributos descriptivos de cliente, producto, país, fecha y tipo de transacción (venta o cancelación).

El diagrama completo del modelo está en el informe (sección 3.4.2).

## Estructura del Repositorio 📁

```
/dataset
    Online Retail.xlsx           Dataset original completo, tal como se descargó de UCI

/scripts
    script_1_creacion_bd.sql     Creación de la base dw_onlineretail y las 7 tablas
    script_2_etl.sql             ETL completo: staging, carga de dimensiones y de fac_ventas

/power-bi
    dashboard_bi.pbix            Tablero de Power BI conectado a dw_onlineretail

/documentacion
    informe_final.docx           Informe completo del proyecto
    informe_final.pdf            Mismo informe, exportado a PDF
    word_evidencias.docx         Capturas y evidencias de ejecución, organizadas por código (E-01 a E-16)

/big-data
    (fase posterior del curso)

README.md
```

## Requisitos 📋

- SQL Server 2019 o superior (o SQL Server Express).
- SQL Server Management Studio (SSMS) para correr los scripts.
- Power BI Desktop para abrir el dashboard.

## Configuración 🚀

1. Clonar el repositorio:
   ```bash
   git clone https://github.com/Juanhp07/Base-de-datos-Avanzada-y-Big-Data-Proyecto.git
   ```
2. Ejecutar `scripts/script_1_creacion_bd.sql` en SSMS: crea la base `dw_onlineretail` y las 7 tablas.
3. Ejecutar `scripts/script_2_etl.sql`: extrae el dataset a staging, carga las 5 dimensiones y después `fac_ventas`.
4. Abrir `power-bi/dashboard_bi.pbix` en Power BI Desktop y actualizar la conexión a la base local.

## KPI del Dashboard 📊

El dashboard responde 10 KPI de negocio: ingresos brutos y netos, monto cancelado, ticket promedio, tasa de cancelación, top 10 productos, clientes activos, tasa de recompra, ingresos por país y riesgo de abandono por recencia. El detalle de cada uno, con su fórmula DAX y el gráfico elegido, está documentado para el equipo en una guía interna aparte (no incluida en este repositorio público).

## Dataset de Origen 📦

Online Retail Dataset, UCI Machine Learning Repository, licencia CC BY 4.0. Se usa con fines académicos, citando la fuente original en el informe (sección 1.6).

En `dataset/Online Retail.xlsx` está el archivo original completo, tal como se descargó de UCI, con todas sus columnas y registros. De ahí sale el .txt que se usa en el BULK INSERT del Script 2, y es la fuente en la que se basa todo el proyecto: el modelo dimensional, el ETL, los KPI y el dashboard parten únicamente de esta información, sin datos agregados ni modificados a mano.

## Equipo 👥

Proyecto colaborativo, curso BASAV, UPN.
