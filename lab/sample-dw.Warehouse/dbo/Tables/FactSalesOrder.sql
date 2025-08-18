CREATE TABLE [dbo].[FactSalesOrder] (

	[SalesOrderKey] int NOT NULL, 
	[SalesOrderDateKey] int NOT NULL, 
	[ProductKey] int NOT NULL, 
	[CustomerKey] int NOT NULL, 
	[Quantity] int NULL, 
	[SalesTotal] decimal(18,0) NULL
);


GO
ALTER TABLE [dbo].[FactSalesOrder] ADD CONSTRAINT UQ_4154ce6c_a80c_48a0_8010_9df9be0d99a9 unique NONCLUSTERED ([SalesOrderDateKey]);
GO
ALTER TABLE [dbo].[FactSalesOrder] ADD CONSTRAINT UQ_da4a00a4_9214_4dff_a3bb_b75a21d682fa unique NONCLUSTERED ([CustomerKey]);
GO
ALTER TABLE [dbo].[FactSalesOrder] ADD CONSTRAINT UQ_e47156d8_3688_41fd_b2ae_fe6d5efbe414 unique NONCLUSTERED ([ProductKey]);