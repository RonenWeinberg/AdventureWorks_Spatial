USE AdventureWorks2019
GO

--Find table name and column name with geometry / geography data type
SELECT 
    TABLE_NAME, 
    COLUMN_NAME, 
    DATA_TYPE 
FROM 
    INFORMATION_SCHEMA.COLUMNS 
WHERE 
    DATA_TYPE IN ('geometry', 'geography')
GO

--Checking the geography data (coordinates + SRID):
SELECT AddressID, AddressLine1, City, SpatialLocation,
	SpatialLocation.STAsText() AS WKT,
	SpatialLocation.Long AS Longitude,
	SpatialLocation.Lat AS Latitude,
	SpatialLocation.STSrid AS SRID
FROM Person.Address
GO


--Create view table of stores and spatial location 
IF OBJECT_ID('dbo.vw_stores_SpatialLocation', 'v') IS NOT NULL
	DROP VIEW dbo.vw_stores_SpatialLocation
GO

CREATE VIEW vw_stores_SpatialLocation AS
SELECT s.BusinessEntityID, s.Name, a.AddressLine1, a.City, a.SpatialLocation
FROM Person.Address AS a JOIN
	Person.BusinessEntityAddress AS bea
	ON a.AddressID = bea.AddressID
	JOIN Sales.Store AS s
	ON bea.BusinessEntityID = s.BusinessEntityID
WHERE SpatialLocation.Lat!=0 AND SpatialLocation.Long!=0   --Filtering out (0 0) coordinates
	  
GO

-- Stores query: total sales, total orders and avg sale per store
-- Adding the spatial data

WITH customer_total_order AS
	(SELECT soh.CustomerID,
		SUM(sod.OrderQty * sod.UnitPrice) as TotalOrders
	FROM Sales.SalesOrderHeader as soh
		JOIN Sales.SalesOrderDetail as sod
		ON soh.SalesOrderID = sod.SalesOrderID
	GROUP BY soh.CustomerID),

customer_store AS
	(SELECT c.CustomerID, c.StoreID,
		COUNT(soh.SalesOrderID) as NoSales
	 FROM Sales.Customer as c
		LEFT JOIN Sales.SalesOrderHeader as soh
		ON c.CustomerID = soh.CustomerID
	 GROUP BY c.CustomerID, c.StoreID)


SELECT a.*, 
	a.TotalOrders/a.TotalSales as SaleAvg,
	ss.SpatialLocation.STAsText() as WKT,
	ss.SpatialLocation
FROM (
	SELECT s.BusinessEntityID, s.Name as StoreName,
			COUNT(cs.CustomerID) as NoCustomers,
			SUM(cs.NoSales) as TotalSales,
			SUM(cto.TotalOrders) as TotalOrders
	FROM Sales.Store as s
		LEFT JOIN customer_store as cs
		ON s.BusinessEntityID = cs.StoreID
		LEFT JOIN customer_total_order as cto
		ON cs.CustomerID = cto.CustomerID
	GROUP BY s.BusinessEntityID, s.Name) a
	INNER JOIN dbo.vw_stores_SpatialLocation as ss
	ON a.BusinessEntityID = ss.BusinessEntityID
ORDER BY SaleAvg DESC;


-- Stores' most sold Product (SubCategory)

WITH stores_products AS
(
SELECT *
FROM (
SELECT s.BusinessEntityID,
	   s.Name as Store_Name,
	   sc.Name as SubCategory_Name,
	   COUNT(sc.Name) as Category_count,
	   pc.name as Category_Name,
	   ROW_NUMBER() OVER (PARTITION BY s.BusinessEntityID ORDER BY COUNT(sc.Name) DESC) as rn
FROM Sales.SalesOrderDetail as sod
	JOIN Production.Product as p
		ON sod.ProductID = p.ProductID
	JOIN Production.ProductSubcategory as sc
		ON p.ProductSubcategoryID = sc.ProductSubcategoryID
	JOIN Production.ProductCategory as pc
		ON sc.ProductCategoryID = pc.ProductCategoryID
	JOIN Sales.SalesOrderHeader as soh
		ON sod.SalesOrderID = soh.SalesOrderID
	JOIN Sales.Customer as c
		ON soh.CustomerID = c.CustomerID
	JOIN Sales.Store as s
		ON c.StoreID = s.BusinessEntityID
GROUP BY s.BusinessEntityID, s.Name, sc.Name, pc.Name) a
WHERE rn = 1)

SELECT sp.*, ss.SpatialLocation, ss.SpatialLocation.STAsText() as WKT
FROM stores_products as sp
JOIN dbo.vw_stores_SpatialLocation as ss
ON sp.BusinessEntityID = ss.BusinessEntityID;