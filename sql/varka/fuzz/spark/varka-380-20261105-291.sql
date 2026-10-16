-- VARKA-262 reproducer
-- status: known 380
-- note: seed 20261105 iteration 291
-- ansi: true
-- kind: only the row engine throws: ARITHMETIC_OVERFLOW
-- select: weekofyear(d) AS c0
-- where: (((12 + l) = 5000000000)) AND (l = l2)
-- fixture
SELECT d, d2, i,
       CAST(m AS INTERVAL MONTH) AS ymm,
       CASE WHEN 12L * y BETWEEN -2147483648L AND 2147483647L
            THEN CAST(y AS INTERVAL YEAR) END AS ymy,
       CASE WHEN 12L * y + mm BETWEEN -2147483648L AND 2147483647L
            THEN make_ym_interval(y, mm) END AS ym,
       l, l2, CAST(t AS TIME(6)) AS t, CAST(t2 AS TIME(6)) AS t2, dt, dt2
FROM VALUES (DATE'2021-06-01', CAST(NULL AS DATE), 5, 67, 4, 4, CAST('-9223372036854775808' AS BIGINT), CAST('9223372036854775807' AS BIGINT), TIME'01:02:03.456', TIME'06:00:00', INTERVAL '1 02:03:04.5' DAY TO SECOND, INTERVAL '0 00:00:00' DAY TO SECOND)
AS v(d, d2, i, m, y, mm, l, l2, t, t2, dt, dt2)
-- query
SELECT weekofyear(d) AS c0 FROM fz WHERE (((12 + l) = 5000000000)) AND (l = l2)
