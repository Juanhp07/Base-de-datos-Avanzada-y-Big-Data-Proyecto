--Proceso ETL COMPLETO

use dw_onlineretail
go

-- PASO 1: reseteo de tablas

delete from fac_ventas
if @@ROWCOUNT <> 0
    DBCC CHECKIDENT ('fac_ventas', RESEED, 0)
go
delete from dim_cliente
if @@ROWCOUNT <> 0
    DBCC CHECKIDENT ('dim_cliente', RESEED, 0)
go
delete from dim_producto
if @@ROWCOUNT <> 0
    DBCC CHECKIDENT ('dim_producto', RESEED, 0)
go
delete from dim_pais
if @@ROWCOUNT <> 0
    DBCC CHECKIDENT ('dim_pais', RESEED, 0)
go
delete from dim_tipotransaccion
if @@ROWCOUNT <> 0
    DBCC CHECKIDENT ('dim_tipotransaccion', RESEED, 0)
go
delete from dim_tiempo
go
truncate table stg_onlineretail
go

-- PASO 2: extraccion a staging (BULK INSERT)


BEGIN
  DECLARE @ARCHIVO VARCHAR(200)
  SET @ARCHIVO = 'C:\txt\onlineretail.txt'

  DECLARE @sql VARCHAR(MAX)
  SET @sql = 'BULK INSERT dw_onlineretail.dbo.stg_onlineretail' +
             ' FROM ''' + @ARCHIVO + ''' WITH (FIELDTERMINATOR = ''' + CHAR(9) + ''', ROWTERMINATOR = ''\n'', FIRSTROW = 2, CODEPAGE=''65001'')'
  EXEC(@sql)
END
go

select count(*) as totalStaging from stg_onlineretail
go

-- PASO 3: transformacion y carga de dimensiones

insert into dim_tipotransaccion (pk_tipotransaccion, nombre)
values ('V', 'Venta'), ('C', 'Cancelacion')
go

insert into dim_cliente (pk_cliente, esDesconocido)
select distinct idCliente, 0
from stg_onlineretail
where idCliente is not null
go

insert into dim_cliente (pk_cliente, esDesconocido)
values (null, 1)
go

insert into dim_producto (pk_producto, nombre)
select codigoProducto, max(descripcionProducto)
from stg_onlineretail
where codigoProducto is not null
group by codigoProducto
go

insert into dim_pais (nombrePais)
select distinct pais
from stg_onlineretail
where pais is not null
go

declare @inicio date, @fin date
select @inicio = min(COALESCE(
                        TRY_CONVERT(date, fechaFactura, 103),
                        CASE WHEN ISNUMERIC(fechaFactura) = 1
                             THEN CAST(DATEADD(DAY, CAST(fechaFactura AS FLOAT), '1899-12-30') AS DATE)
                        END)),
       @fin    = max(COALESCE(
                        TRY_CONVERT(date, fechaFactura, 103),
                        CASE WHEN ISNUMERIC(fechaFactura) = 1
                             THEN CAST(DATEADD(DAY, CAST(fechaFactura AS FLOAT), '1899-12-30') AS DATE)
                        END))
from stg_onlineretail

WHILE (@inicio <= @fin)
    BEGIN
        INSERT INTO dim_tiempo
        SELECT @inicio fecha, DATEPART(yyyy, @inicio) anio, DATEPART(m, @inicio) mes,
            DATEPART(d, @inicio) diaMes,
            CASE DATEPART(dw, @inicio)
                    WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes'
                    WHEN 4 THEN 'Miercoles' WHEN 5 THEN 'Jueves' WHEN 6 THEN 'Viernes'
                    WHEN 7 THEN 'Sabado'
                END AS diaSemana,
                DATEPART(q, @inicio) AS trimestre

        SET @inicio = DATEADD(d, 1, @inicio);
    end
go

-- PASO 4: carga de la tabla de hechos

insert into fac_ventas
    (id_cliente, id_producto, id_pais, id_tipotransaccion, fecha,
     numeroFacturaVenta, cantidadVendida, precioUnitarioVenta, importeVendido)
select
    coalesce(D_CL.id_cliente, D_CLU.id_cliente) id_cliente,
    D_PR.id_producto,
    D_PA.id_pais,
    D_TT.id_tipotransaccion,
    FC.fecha,
    S.numeroFactura as numeroFacturaVenta,
    sum(S.cantidad) as cantidadVendida,
    S.precioUnitario as precioUnitarioVenta,
    sum(S.cantidad * S.precioUnitario) as importeVendido
from stg_onlineretail S
cross apply (
    select COALESCE(
        TRY_CONVERT(date, S.fechaFactura, 103),
        CASE WHEN ISNUMERIC(S.fechaFactura) = 1
             THEN CAST(DATEADD(DAY, CAST(S.fechaFactura AS FLOAT), '1899-12-30') AS DATE)
        END) AS fecha
) FC
join dim_producto D_PR on D_PR.pk_producto = S.codigoProducto
join dim_pais D_PA on D_PA.nombrePais = S.pais
join dim_tipotransaccion D_TT
    on D_TT.pk_tipotransaccion = case when left(S.numeroFactura, 1) = 'C' then 'C' else 'V' end
left join dim_cliente D_CL on D_CL.pk_cliente = S.idCliente
cross join (select id_cliente from dim_cliente where esDesconocido = 1) D_CLU
where S.precioUnitario > 0
  and FC.fecha is not null
group by coalesce(D_CL.id_cliente, D_CLU.id_cliente), D_PR.id_producto, D_PA.id_pais,
    D_TT.id_tipotransaccion, FC.fecha, S.numeroFactura, S.precioUnitario
go

-- PASO 5: verificacion de la carga

select
    (select count(*) from stg_onlineretail) as total_staging,
    (select sum(cantidadVendida) from fac_ventas) as total_unidades_cargadas
go

select
    year(F.fecha) anio, month(F.fecha) mes,
    sum(F.importeVendido) as ingresosNetos
from fac_ventas F
join dim_tipotransaccion T on T.id_tipotransaccion = F.id_tipotransaccion
where T.pk_tipotransaccion = 'V'
group by year(F.fecha), month(F.fecha)
order by anio, mes
go
