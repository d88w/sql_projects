------ GROSS ------
------ GROSS ------
------ GROSS ------
------ GROSS ------
------ GROSS ------
------ GROSS ------
------ GROSS ------
------ GROSS ------
------ GROSS ------
------ GROSS ------


WITH T3 AS
(
WITH T2 AS
(
WITH T1 AS
(
---by segment
SELECT
TO_CHAR(mrscohortdate, 'yyyy-mm') AS cohort_month,
salesdivision AS orig_seg,
CEILING(DATEDIFF(month, mrscohortdate, dt)) AS months_since_cohort_date,
SUM(mrs_total_new) AS mrr_gross
FROM dm_sales_airflow.v_zr_mrs_by_ad_growth_combined
WHERE acct_channel NOT IN ('ISV')
AND mrscohortdate >= '2017-01-01'
AND mrs_total_new + mrs_backlog_net_new <> 0
GROUP BY
1,2,3

---all
UNION ALL
SELECT TO_CHAR(mrscohortdate, 'yyyy-mm') AS cohort_month,
'0. All' AS orig_seg,
CEILING(DATEDIFF(month, mrscohortdate, dt)) AS months_since_cohort_date,
SUM(mrs_total_new) AS mrr_gross
FROM dm_sales_airflow.v_zr_mrs_by_ad_growth_combined
WHERE acct_channel NOT IN ('ISV')
AND mrscohortdate >= '2017-01-01'
AND mrs_total_new + mrs_backlog_net_new <> 0
GROUP BY
1,2,3
)

SELECT T1.*,
SUM(T1.mrr_gross) OVER (PARTITION BY T1.orig_seg, T1.cohort_month ORDER BY T1.months_since_cohort_date ROWS BETWEEN UNBOUNDED PRECEDING and current row) AS mrr_agg,
CASE WHEN T1.months_since_cohort_date = 0 
       THEN (SUM(T1.mrr_gross) OVER (PARTITION BY T1.orig_seg, T1.cohort_month ORDER BY T1.months_since_cohort_date ROWS BETWEEN UNBOUNDED PRECEDING and current row)) 
       ELSE 0 END flag
FROM T1 
WHERE T1.months_since_cohort_date >= 0
ORDER BY 1,2) 

SELECT
T2.*, max(flag) OVER(PARTITION BY T2.orig_seg, T2.cohort_month) AS mrr_month0
FROM T2)

----monthly average
SELECT
'Monthly Avg' AS cohort_month,
T3.orig_seg,
T3.months_since_cohort_date,
avg(T3.mrr_gross) AS mrr_gross,
avg(T3.mrr_agg) AS mrr_agg,
avg(CASE WHEN T3.mrr_month0 <> 0 THEN T3.mrr_agg/T3.mrr_month0 ELSE 0 END) AS ret_rate
FROM T3
WHERE T3.orig_seg IS NOT NULL
AND T3.orig_seg <> 'Exclude'
GROUP BY 2,3

----monthly cohorts
UNION ALL 
SELECT 
T3.cohort_month,
T3.orig_seg,
T3.months_since_cohort_date,
T3.mrr_gross,
T3.mrr_agg,
CASE WHEN T3.mrr_month0 <> 0 THEN T3.mrr_agg/T3.mrr_month0 ELSE 0 END AS ret_rate
FROM T3
WHERE T3.orig_seg IS NOT NULL
AND T3.orig_seg <> 'Exclude'
ORDER BY 1,2,3;




-------- NET --------
-------- NET --------
-------- NET --------
-------- NET --------
-------- NET --------
-------- NET --------
-------- NET --------
-------- NET --------
-------- NET --------
-------- NET --------

WITH T3 AS
(
WITH T2 AS
(
WITH T1 AS
(
---by segment
SELECT
TO_CHAR(mrscohortdate, 'yyyy-mm') AS cohort_month,
salesdivision AS orig_seg,
CEILING(DATEDIFF(month, mrscohortdate, dt)) AS months_since_cohort_date,
SUM(mrs_net_new) AS mrr_net
FROM dm_sales_airflow.v_zr_mrs_by_ad_growth_combined
WHERE acct_channel NOT IN ('ISV')
AND mrscohortdate >= '2017-01-01'
AND mrs_net_new + mrs_backlog_net_new <> 0
GROUP BY
1,2,3

---all
UNION ALL
SELECT TO_CHAR(mrscohortdate, 'yyyy-mm') AS cohort_month,
'0. All' AS orig_seg,
CEILING(DATEDIFF(month, mrscohortdate, dt)) AS months_since_cohort_date,
SUM(mrs_net_new) AS mrr_net
FROM dm_sales_airflow.v_zr_mrs_by_ad_growth_combined
WHERE acct_channel NOT IN ('ISV')
AND mrscohortdate >= '2017-01-01'
AND mrs_net_new + mrs_backlog_net_new <> 0
GROUP BY
1,2,3
)

SELECT T1.*,
SUM(T1.mrr_net) OVER (PARTITION BY T1.orig_seg, T1.cohort_month ORDER BY T1.months_since_cohort_date ROWS BETWEEN UNBOUNDED PRECEDING and current row) AS mrr_agg,
CASE WHEN T1.months_since_cohort_date = 0 
       THEN (SUM(T1.mrr_net) OVER (PARTITION BY T1.orig_seg, T1.cohort_month ORDER BY T1.months_since_cohort_date ROWS BETWEEN UNBOUNDED PRECEDING and current row)) 
       ELSE 0 END flag
FROM T1 
WHERE T1.months_since_cohort_date >= 0
ORDER BY 1,2) 

SELECT
T2.*, max(flag) OVER(PARTITION BY T2.orig_seg, T2.cohort_month) AS mrr_month0
FROM T2)

----monthly average
SELECT
'Monthly Avg' AS cohort_month,
T3.orig_seg,
T3.months_since_cohort_date,
avg(T3.mrr_net) AS mrr_net,
avg(T3.mrr_agg) AS mrr_agg,
avg(CASE WHEN T3.mrr_month0 <> 0 THEN T3.mrr_agg/T3.mrr_month0 ELSE 0 END) AS ret_rate
FROM T3
WHERE T3.orig_seg IS NOT NULL
AND T3.orig_seg <> 'Exclude'
GROUP BY 2,3

----monthly cohorts
UNION ALL 
SELECT 
T3.cohort_month,
T3.orig_seg,
T3.months_since_cohort_date,
T3.mrr_net,
T3.mrr_agg,
CASE WHEN T3.mrr_month0 <> 0 THEN T3.mrr_agg/T3.mrr_month0 ELSE 0 END AS ret_rate
FROM T3
WHERE T3.orig_seg IS NOT NULL
AND T3.orig_seg <> 'Exclude'
ORDER BY 1,2,3;