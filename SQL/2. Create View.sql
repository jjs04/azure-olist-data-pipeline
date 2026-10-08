CREATE SCHEMA gold

CREATE VIEW gold.final
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://olistecommbrazsa.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result1


select * from gold.final