CREATE VIEW gold.final2
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://olistecommbrazsa.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result2
WHERE order_status = 'delivered'

SELECT * from gold.final2