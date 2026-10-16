-- VARKA-262 reproducer
-- status: regression
-- note: seed 20261102 iteration 4775
-- ansi: true
-- kind: only the row engine throws: INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION
-- select: dt AS c0
-- where: NOT (((ym + ym) IS NOT NULL)) AND (d = d2)
-- fixture
SELECT d, d2, i,
       CAST(m AS INTERVAL MONTH) AS ymm,
       CASE WHEN 12L * y BETWEEN -2147483648L AND 2147483647L
            THEN CAST(y AS INTERVAL YEAR) END AS ymy,
       CASE WHEN 12L * y + mm BETWEEN -2147483648L AND 2147483647L
            THEN make_ym_interval(y, mm) END AS ym,
       l, l2, CAST(t AS TIME(6)) AS t, CAST(t2 AS TIME(6)) AS t2, dt, dt2
FROM VALUES (DATE'1999-12-31', DATE'1969-12-31', 13, 5, 178956969, 8, CAST('0' AS BIGINT), CAST('-5000000000' AS BIGINT), TIME'09:30:00', TIME'00:00:00', INTERVAL '0 00:00:00' DAY TO SECOND, INTERVAL '-100000 00:00:00' DAY TO SECOND)
AS v(d, d2, i, m, y, mm, l, l2, t, t2, dt, dt2)
-- query
SELECT dt AS c0 FROM fz WHERE NOT (((ym + ym) IS NOT NULL)) AND (d = d2)
