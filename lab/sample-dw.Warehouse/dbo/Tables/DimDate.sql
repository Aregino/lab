CREATE TABLE [dbo].[DimDate] (

	[DateKey] int NOT NULL, 
	[DateAltKey] date NOT NULL, 
	[DayOfWeek] int NOT NULL, 
	[WeekDayName] varchar(10) NULL, 
	[DayOfMonth] int NOT NULL, 
	[Month] int NOT NULL, 
	[MonthName] varchar(12) NULL, 
	[Year] int NOT NULL
);


GO
ALTER TABLE [dbo].[DimDate] ADD CONSTRAINT FK_49ce9af1_e3c3_4a71_8c9e_f226a7d1a69d FOREIGN KEY ([DateKey]) REFERENCES [dbo].[FactSalesOrder]([SalesOrderDateKey]);