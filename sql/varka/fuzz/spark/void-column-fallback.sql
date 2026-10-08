-- VARKA-262 reproducer
-- status: known VARKA-299
-- note: found by the first 20000-composition run (seed 20261008, iteration 93); a relation with a VOID column and a batch the kernel declines
-- ansi: false
-- kind: only Varka throws: UNSUPPORTED_DATATYPE
-- select: greatest(l, l2) AS c0, date_add(d, i) AS c1
-- where: NOT (i = 5)
-- fixture
SELECT d, d2, i,
       CAST(m AS INTERVAL MONTH) AS ymm,
       CAST(y AS INTERVAL YEAR) AS ymy,
       make_ym_interval(y, mm) AS ym,
       l, l2, CAST(t AS TIME(6)) AS t, CAST(t2 AS TIME(6)) AS t2, dt, dt2
FROM VALUES (DATE'0001-01-15', DATE'0001-01-15', 100000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
AS v(d, d2, i, m, y, mm, l, l2, t, t2, dt, dt2)
-- query
SELECT greatest(l, l2) AS c0, date_add(d, i) AS c1 FROM fz WHERE NOT (i = 5)
