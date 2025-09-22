-- Auto Generated (Do not modify) DE917EA4D76767B67FD6939CA2DB5D4D5FA8B0580E40C3566EE93C00436EDBDE
CREATE   VIEW raw.vw_vehicles_with_parts
AS
SELECT
    TRY_CONVERT(date,   s.updated_date)                                    AS updated_date,
    TRY_CONVERT(date,   s.arrival_date)                                    AS arrival_date,
    CAST(s.id            AS varchar(64))                                   AS vehicle_id,
    CAST(s.stock_no      AS varchar(64))                                   AS stock_no,
    CAST(s.yard          AS varchar(100))                                  AS yard,
    CAST(s.make          AS varchar(100))                                  AS make,
    CAST(s.model         AS varchar(120))                                  AS model,
    CAST(s.series        AS varchar(120))                                  AS series,
    CAST(s.body          AS varchar(120))                                  AS body,
    CAST(s.colour        AS varchar(120))                                  AS colour,
    CAST(s.fuel          AS varchar(50))                                   AS fuel,
    CAST(s.transmission  AS varchar(80))                                   AS transmission,
    CAST(s.url           AS varchar(512))                                  AS url,
    CAST(s.source        AS varchar(50))                                   AS source,
    CAST(s.scrape_run_id AS varchar(200))                                  AS scrape_run_id,
    JSON_VALUE(p.value, '$.code')                                          AS part_code,
    JSON_VALUE(p.value, '$.name')                                          AS part_name,
    JSON_VALUE(p.value, '$.condition')                                     AS part_condition,
    TRY_CONVERT(decimal(18,2), JSON_VALUE(p.value, '$.price'))             AS part_price,
    TRY_CONVERT(int,            JSON_VALUE(p.value, '$.quantity'))         AS part_quantity
FROM
OPENROWSET(
    BULK 'abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/a743e7da-a128-4060-a81f-4d6844a04abd/Files/pickapart/raw/vehicle/*/*.json',
    FORMAT = 'JSONL'
)
WITH (
    updated_date    varchar(50)   '$.updated_date',
    arrival_date    varchar(50)   '$.arrival_date',
    id              varchar(64)   '$.id',
    stock_no        varchar(64)   '$.stock_no',
    yard            varchar(100)  '$.yard',
    make            varchar(100)  '$.make',
    model           varchar(120)  '$.model',
    series          varchar(120)  '$.series',
    body            varchar(120)  '$.body',
    colour          varchar(120)  '$.colour',
    fuel            varchar(50)   '$.fuel',
    transmission    varchar(80)   '$.transmission',
    url             varchar(512)  '$.url',
    source          varchar(50)   '$.source',
    scrape_run_id   varchar(200)  '$.scrape_run_id',
    parts           nvarchar(max) '$.parts'
) AS s
CROSS APPLY OPENJSON(s.parts) AS p;