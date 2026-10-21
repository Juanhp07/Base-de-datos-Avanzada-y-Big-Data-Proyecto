--CREACION DE LA BASE DE DATOS ONLINE RETAIL

use master
go
create database dw_onlineretail
go

use dw_onlineretail
go

-- DIMENSIONES

create table dim_cliente
(id_cliente int primary key identity,
pk_cliente  int,  
esDesconocido bit default 0
)
go

create table dim_producto
(id_producto int primary key identity,
pk_producto  varchar(20),
nombre       varchar(200)
)
go

create table dim_pais
(id_pais int primary key identity,
nombrePais varchar(100)
)
go

create table dim_tiempo
(fecha date primary key,
anio smallint,
mes smallint,
diaMes smallint,
diaSemana varchar(15),
trimestre smallint
)
go

create table dim_tipotransaccion
(id_tipotransaccion int primary key identity,
pk_tipotransaccion  char(1),
nombre               varchar(20)
)
go

-- STAGING

create table stg_onlineretail
(numeroFactura        varchar(10),
codigoProducto        varchar(20),
descripcionProducto   varchar(200),
cantidad              int,
fechaFactura          varchar(30),
precioUnitario        money,
idCliente             int null,
pais                  varchar(100)
)
go

-- TABLA DE HECHOS
create table fac_ventas
(
id_venta            int primary key identity,
id_cliente           int references dim_cliente,
id_producto          int references dim_producto,
id_pais              int references dim_pais,
id_tipotransaccion   int references dim_tipotransaccion,
fecha                 date references dim_tiempo,
numeroFacturaVenta     varchar(10),
cantidadVendida        int,
precioUnitarioVenta    money,
importeVendido          money
)
go
