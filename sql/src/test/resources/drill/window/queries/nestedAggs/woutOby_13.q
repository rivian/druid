SELECT c2, AVG(SUM(c1)) OVER ( PARTITION BY c2 ), MAX(MIN(c2)) OVER ( PARTITION BY c2 ) FROM (SELECT * FROM "tblWnulls.parquet" WHERE c2 IS NOT NULL) sub_query GROUP BY c2
