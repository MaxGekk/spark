-- VARKA-262 reproducer
-- status: regression
-- note: seed 20261104 iteration 2199, found by VARKA-298, fixed by VARKA-380: a fused conjunct ran ahead of a residual one that raises in Spark
-- ansi: true
-- kind: only the row engine throws: DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION
-- select: l AS c0
-- where: ((date_from_unix_date(i) >= make_date(i, i, i))) AND (d IN (DATE '2021-01-01', DATE '2021-06-01'))
-- fixture
SELECT d, d2, i,
       CAST(m AS INTERVAL MONTH) AS ymm,
       CASE WHEN 12L * y BETWEEN -2147483648L AND 2147483647L
            THEN CAST(y AS INTERVAL YEAR) END AS ymy,
       CASE WHEN 12L * y + mm BETWEEN -2147483648L AND 2147483647L
            THEN make_ym_interval(y, mm) END AS ym,
       l, l2, CAST(t AS TIME(6)) AS t, CAST(t2 AS TIME(6)) AS t2, dt, dt2
FROM VALUES (DATE'2236-08-13', DATE'1622-05-27', -2654, 19, 45, 11, CAST('612618103218' AS BIGINT), CAST('-920673488439' AS BIGINT), TIME'09:30:00', TIME'00:00:00', INTERVAL '1 02:03:04.5' DAY TO SECOND, INTERVAL '100000 00:00:00' DAY TO SECOND),
  (DATE'1703-09-06', DATE'1769-02-19', 10, -987, 40, 0, CAST('-667748565992' AS BIGINT), CAST('252049668493' AS BIGINT), TIME'00:00:00.000001', TIME'05:32:12.506570', INTERVAL '0 00:00:01' DAY TO SECOND, INTERVAL '885 06:58:03.296662' DAY TO SECOND),
  (DATE'2190-08-04', DATE'2024-01-31', 3840, 66, 4, 7, CAST('-2147483648' AS BIGINT), CAST('-751483597083' AS BIGINT), TIME'06:00:00', TIME'00:00:00.000001', INTERVAL '-1094 10:37:52.767159' DAY TO SECOND, INTERVAL '-0 00:00:00.5' DAY TO SECOND)
AS v(d, d2, i, m, y, mm, l, l2, t, t2, dt, dt2)
-- query
SELECT l AS c0 FROM fz WHERE ((date_from_unix_date(i) >= make_date(i, i, i))) AND (d IN (DATE '2021-01-01', DATE '2021-06-01'))
