USE AdventureWorks2019
GO


--Create Spatial index on Address table

DROP INDEX IF EXISTS SIndx_address_Slocation ON Person.Address
GO

CREATE SPATIAL INDEX SIndx_address_Slocation ON Person.Address(SpatialLocation)
GO

-- View table: Stores with spatial location - US only

IF OBJECT_ID('dbo.vw_stores_SpatialLocation', 'v') IS NOT NULL
	DROP VIEW dbo.vw_stores_SpatialLocation
GO

CREATE VIEW vw_stores_SpatialLocation AS
	SELECT s.BusinessEntityID, s.Name, a.AddressLine1, a.City, a.CountryRegionCode, a.SpatialLocation
	FROM
		(SELECT a.AddressID, a.AddressLine1, a.City, sp.CountryRegionCode, a.SpatialLocation
		FROM Person.Address as a
			JOIN Person.StateProvince as sp
			ON a.StateProvinceID = sp.StateProvinceID
		WHERE sp.CountryRegionCode='US') as a
		JOIN
		Person.BusinessEntityAddress AS bea
		ON a.AddressID = bea.AddressID
		JOIN Sales.Store AS s
		ON bea.BusinessEntityID = s.BusinessEntityID
GO

--View table: On-Line orders with shipment spatial location - US only

IF OBJECT_ID('dbo.vw_orders_SpatialLocation', 'v') IS NOT NULL
	DROP VIEW dbo.vw_orders_SpatialLocation
GO

CREATE VIEW vw_orders_SpatialLocation AS
	SELECT soh.SalesOrderID, a.*
	FROM (
			SELECT a.AddressID, a.AddressLine1, a.City, sp.CountryRegionCode, a.SpatialLocation
			FROM Person.Address as a
				JOIN Person.StateProvince as sp
				ON a.StateProvinceID = sp.StateProvinceID
			WHERE sp.CountryRegionCode='US') as a
		JOIN
			(SELECT SalesOrderID, ShipToAddressID
			FROM Sales.SalesOrderHeader
			WHERE OnlineOrderFlag = 1) as soh
		ON a.AddressID = soh.ShipToAddressID
GO

-- No. of On-Line purchase

SELECT CASE OnlineOrderFlag
			WHEN 1 THEN 'On-Line'
			WHEN 0 THEN 'In Store'
		END as OrderMethod,
		NoOfOrders,
	CONCAT(ROUND(CAST(NoOfOrders as float) / SUM(NoOfOrders) OVER() * 100, 1), '%') as Perc
FROM (
	SELECT OnlineOrderFlag, COUNT(*) as NoOfOrders
	FROM Sales.SalesOrderHeader
	GROUP BY OnlineOrderFlag) a


/* 
OrderMethod NoOfOrders  Perc
----------- ----------- ------------------------
In Store    3806        12.1%
On-Line     27659       87.9%
*/

-- Calculating the distance from each On-Line order's shipment location to the nearest store

SELECT *
INTO temp_table_distance   --Create temp table for later use (comment this line for table results)
FROM (
	SELECT o.SalesOrderID, o.AddressLine1 as OrderAddress,
		s.AddressLine1 as StoreAddress, s.name,
		[Distance (m)],
		ROW_NUMBER() OVER (PARTITION BY o.SalesOrderID ORDER BY [Distance (m)]) as rn
	FROM vw_orders_SpatialLocation as o
	CROSS APPLY 
		(SELECT name, AddressLine1,
			o.SpatialLocation.STDistance(SpatialLocation) as [Distance (m)]
		FROM vw_stores_SpatialLocation) as s
	   ) a
WHERE rn=1
ORDER BY [Distance (m)]


-- Calculating the Average, Max and Median distances
SELECT 
	ROUND(AVG([Distance (m)])/1000, 2) as [Average Distance (km)],
	ROUND(MAX([Distance (m)])/1000, 2) as [Max Distance (km)]
FROM temp_table_distance

/*
Average Distance (km)  Max Distance (km)
---------------------- ----------------------
10.59                  92.84
*/


SELECT DISTINCT
	   ROUND(PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY [Distance (m)]) OVER() / 1000, 2) as [Median Distance(km)]
FROM temp_table_distance

/*
Median Distance(km)
----------------------
7.74
*/

DROP TABLE temp_table_distance