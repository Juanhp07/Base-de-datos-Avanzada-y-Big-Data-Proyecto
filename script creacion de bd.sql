-- =========================================================
-- Script 1. Creacion de base de datos, dimensiones,
-- staging y tabla de hechos - dw_onlineretail
-- =========================================================

use master
go
create database dw_onlineretail
go

use dw_onlineretail
go

-- ---------- DIMENSIONES ----------

create table dim_cliente
(id_cliente int primary key identity,
pk_cliente  int,             -- CustomerID del dataset de origen
esDesconocido bit default 0  -- 1 = fila reservada para ventas sin CustomerID
)
go

create table dim_producto
(id_producto int primary key identity,
pk_producto  varchar(20),    -- StockCode del dataset (alfanumerico)
nombre       varchar(200)    -- Description representativa del producto
)
go

create table dim_pais
(id_pais int primary key identity,
nombrePais varchar(100)
)
go

create table dim_tiempo
(fecha date primary key,     -- la fecha es la propia llave, sin SK aparte
anio smallint,
mes smallint,
diaMes smallint,
diaSemana varchar(15),
trimestre smallint
)
go

create table dim_tipotransaccion
(id_tipotransaccion int primary key identity,
pk_tipotransaccion  char(1),   -- V = Venta, C = Cancelacion
nombre               varchar(20)
)
go

-- ---------- STAGING ----------

create table stg_onlineretail
(numeroFactura        varchar(10),
codigoProducto        varchar(20),
descripcionProducto   varchar(200),
cantidad              int,
fechaFactura          varchar(30),  -- texto: BULK INSERT no siempre interpreta bien la fecha
precioUnitario        money,
idCliente             int null,
pais                  varchar(100)
)
go

-- ---------- TABLA DE HECHOS ----------
-- Grano: una linea de factura consolidada por producto y precio unitario
-- dentro de esa factura (ver Script 2: el INSERT agrupa por cliente,
-- producto, pais, tipo de transaccion, fecha, numeroFactura y precioUnitario,
-- sumando cantidad e importe, para no duplicar filas cuando un producto
-- aparece mas de una vez en la misma factura al mismo precio).
-- Las medidas y el atributo degenerado llevan el sufijo "Venta"/"Vendida"
-- para que cada columna indique a que se refiere sin depender de la tabla.

create table fac_ventas
(
id_venta            int primary key identity,
id_cliente           int references dim_cliente,
id_producto          int references dim_producto,
id_pais              int references dim_pais,
id_tipotransaccion   int references dim_tipotransaccion,
fecha                 date references dim_tiempo,
numeroFacturaVenta     varchar(10),   -- dimension degenerada
cantidadVendida        int,            -- medida
precioUnitarioVenta    money,          -- medida
importeVendido          money           -- medida (cantidadVendida x precioUnitarioVenta)
)
go
