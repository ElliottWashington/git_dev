DROP TABLE IF EXISTS temp_table1;
DROP TABLE IF EXISTS temp_table2;
DROP TABLE IF EXISTS temp_table3;

CREATE TEMP TABLE temp_table1 AS
SELECT tag11, tag100, tag41
FROM fixmsg 
WHERE tag52 >= TIMESTAMP '2023-02-21'
AND tag52 < TIMESTAMP '2023-02-22'
AND tag56 = 'INCAPNS'
AND tag35 = 'AB';

CREATE TEMP TABLE temp_table2 AS
SELECT a.tag11, a.tag52 AS cancel_52, b.tag52 AS cancelack_52, a.tag41
FROM fixmsg a
JOIN fixmsg b ON a.tag11 = b.tag11
WHERE a.tag52 >= TIMESTAMP '2023-02-21'
AND a.tag52 < TIMESTAMP '2023-02-22'
AND b.tag52 >= TIMESTAMP '2023-02-21'
AND b.tag52 < TIMESTAMP '2023-02-22'
AND b.tag35 = '8'
AND b.tag39 = '4'
AND a.tag35 = 'F'
AND a.tag56 = 'INCAPNS';

CREATE TEMP TABLE temp_table3 AS
SELECT AVG(b.cancelack_52 - b.cancel_52) AS avg_difference, COUNT(b.cancelack_52) AS counts, a.tag100
FROM temp_table1 a
JOIN temp_table2 b ON a.tag11 = b.tag41
GROUP BY a.tag100;