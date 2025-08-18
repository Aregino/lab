CREATE TABLE [dbo].[DimProduct] (

	[ProductKey] int NOT NULL, 
	[ProductAltKey] varchar(25) NULL, 
	[ProductName] varchar(50) NOT NULL, 
	[Category] varchar(50) NULL, 
	[ListPrice] decimal(18,0) NULL
);


GO
ALTER TABLE [dbo].[DimProduct] ADD CONSTRAINT FK_00cf5cba_3717_4a25_b847_b7f86ad7d5bf FOREIGN KEY ([ProductKey]) REFERENCES [dbo].[FactSalesOrder]([ProductKey]);