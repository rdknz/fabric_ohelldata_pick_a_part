CREATE TABLE [raw].[vehicles_with_parts2] (

	[vehicle_id] varchar(8000) NULL, 
	[yard] varchar(8000) NULL, 
	[series_url] varchar(8000) NULL, 
	[url] varchar(8000) NULL, 
	[scrape_run_id] varchar(8000) NULL, 
	[vehicle_ref] varchar(8000) NULL, 
	[acq] varchar(8000) NULL, 
	[yard_location] varchar(8000) NULL, 
	[make_model] varchar(8000) NULL, 
	[engine] varchar(8000) NULL, 
	[transmission] varchar(8000) NULL, 
	[year] int NULL, 
	[odometer] bigint NULL, 
	[arrival_date] date NULL, 
	[scraped_at] datetime2(6) NULL, 
	[scraped_at_date] date NULL, 
	[part_name] varchar(8000) NULL
);