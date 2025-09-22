-- Auto Generated (Do not modify) 5B98ED455FA68AAAECF423363C6FF070F4669CCACA4AFCE3C777675D9E241A1B
-- Create a table from your vehicle JSONL files
--CREATE TABLE dbo.vehicles
--WITH (DISTRIBUTION = ROUND_ROBIN)
--AS

CREATE VIEW raw.vw_vehicles AS

SELECT
    TRY_CONVERT(date,   updated_date)              AS updated_date,
    TRY_CONVERT(date,   arrival_date)              AS arrival_date,
    TRY_CONVERT(int,    year)                      AS year,
    CAST(id          AS varchar(64))               AS id,
    CAST(stock_no    AS varchar(64))               AS stock_no,
    CAST(country     AS varchar(100))              AS country,
    CAST(yard        AS varchar(100))              AS yard,
    CAST(make        AS varchar(100))              AS make,
    CAST(model       AS varchar(120))              AS model,
    CAST(series      AS varchar(120))              AS series,
    CAST(body        AS varchar(120))              AS body,
    CAST(colour      AS varchar(120))              AS colour,
    CAST(fuel        AS varchar(50))               AS fuel,
    CAST(transmission AS varchar(80))              AS transmission,
    CAST(url         AS varchar(512))              AS url,
    CAST(source      AS varchar(50))               AS source,
    CAST(scrape_run_id AS varchar(200))            AS scrape_run_id

--select *
FROM
OPENROWSET(
    BULK 'abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/a743e7da-a128-4060-a81f-4d6844a04abd/Files/pickapart/raw/vehicle/*/*.json',
    FORMAT = 'JSONL'
    -- https://app.powerbi.com/groups/0cd3d9a9-0414-44eb-91ad-5516f97df911/lakehouses/a743e7da-a128-4060-a81f-4d6844a04abd?experience=power-bi
)

WITH (
    updated_date    varchar(50)    '$.updated_date',
    arrival_date    varchar(50)    '$.arrival_date',
    year            varchar(10)    '$.year',
    id              varchar(64)    '$.id',
    stock_no        varchar(64)    '$.stock_no',
    country         varchar(100)   '$.country',
    yard            varchar(100)   '$.yard',
    make            varchar(100)   '$.make',
    model           varchar(120)   '$.model',
    series          varchar(120)   '$.series',
    body            varchar(120)   '$.body',
    colour          varchar(120)   '$.colour',
    fuel            varchar(50)    '$.fuel',
    transmission    varchar(80)    '$.transmission',
    url             varchar(512)   '$.url',
    source          varchar(50)    '$.source',
    scrape_run_id   varchar(200)   '$.scrape_run_id',
    parts           nvarchar(max)  '$.parts',
    images          nvarchar(max)  '$.images'
) AS src;