SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://olistecommbrazsa.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result1

---https://olistecommbrazsa.blob.core.windows.net/olistdata/silver/