------Waterfall by region-----POSEIDON------


---------------------------------------
-------Dynamic with region-----------
---------------------------------------

WITH TA AS
(
WITH 
T1 AS
(SELECT 
   to_char(dateadd(mm, 1, mrs_date), 'yyyy-mm') AS month_exit, 
   SUM(mrs_exit) AS mrr_exit_prev_month, 
   COUNT(DISTINCT zoom_account_no) AS no_of_exit_accts_prev_m,
   case when billing_country_region is null then 'US & CAN' else billing_country_region end AS region
FROM rpt.domo_v_zr_mrs_by_ad_growth
WHERE to_char(mrs_date, 'mm-dd') in ('01-31', '02-28', '03-31', '04-30', '05-31', '06-30', '07-31', '08-31', '09-30', '10-31', '11-30', '12-31')
GROUP BY to_char(dateadd(mm, 1, mrs_date), 'yyyy-mm'), 4),

T2 AS
(SELECT 
   to_char(mrs_date, 'yyyy-mm') AS month_exit, 
   SUM(mrs_exit) AS mrr_exit, 
   COUNT(DISTINCT zoom_account_no) AS no_of_exit_accts,
   case when billing_country_region is null then 'US & CAN' else billing_country_region end AS region,
SUM(CASE WHEN mrs_backlog IS NULL THEN 0 ELSE mrs_backlog END) AS mrr_backlog
FROM rpt.domo_v_zr_mrs_by_ad_growth
WHERE to_char(mrs_date, 'mm-dd') in ('01-31', '02-28', '03-31', '04-30', '05-31', '06-30', '07-31', '08-31', '09-30', '10-31', '11-30', '12-31')
GROUP BY to_char(mrs_date, 'yyyy-mm'), 4),

T3 AS
(SELECT to_char(mrs_date, 'yyyy-mm') AS month, 
   fiscal_quarter, 
   SUM(mrs_upsell) AS mrr_upsell, 
   SUM(mrs_new) AS mrr_new, 
   SUM(mrs_total_new) AS mrr_gross_billed, 
   SUM(mrs_net_new) AS mrr_net_billed, 
   SUM(mrs_downsell) AS mrr_downsell, 
   SUM(mrs_cancel) AS mrr_cancel, 
   SUM(mrs_total_churn + CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_gross_churn, 
   SUM(mrs_net_churn + CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_net_churn, 
   SUM(CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_adjustment,
   SUM(mrs_backlog_net_new) AS mrr_backlog_net_new,
   case when billing_country_region is null then 'US & CAN' else billing_country_region end AS region
FROM rpt.domo_v_zr_mrs_by_ad_growth
GROUP BY fiscal_quarter, to_char(mrs_date, 'yyyy-mm'), 13),

T4 AS
(SELECT 
   to_char(mrs_date, 'yyyy-mm') 
   AS month, fiscal_quarter, 
   COUNT(DISTINCT zoom_account_no) AS no_of_cancels,
   case when billing_country_region is null then 'US & CAN' else billing_country_region end AS region
FROM rpt.domo_v_zr_mrs_by_ad_growth
WHERE mrs_cancel < 0
GROUP BY fiscal_quarter, to_char(mrs_date, 'yyyy-mm'), 4)

SELECT 
T3.month, 
T3.fiscal_quarter, 
T3.region, 
T1.mrr_exit_prev_month AS mrr_exit_billed_prev_month, 
T2.mrr_exit AS mrr_exit_billed, 
T2.mrr_backlog,
T2.mrr_exit + T2.mrr_backlog AS mrr_exit_booked, 
T3.mrr_upsell, 
T3.mrr_new, 
T3.mrr_gross_billed, 
T3.mrr_gross_billed + T3.mrr_backlog_net_new AS mrr_gross_booked,
T3.mrr_net_billed, 
T3.mrr_net_billed + T3.mrr_backlog_net_new AS mrr_net_booked,
T3.mrr_backlog_net_new,
T3.mrr_downsell, 
T3.mrr_cancel, 
T3.mrr_adjustment, 
T3.mrr_gross_churn, 
T3.mrr_net_churn,
T3.mrr_gross_churn/T1.mrr_exit_prev_month*(-100) AS gross_churn_rate, 
T3.mrr_net_churn/T1.mrr_exit_prev_month*(-100) AS net_churn_rate, 
T2.no_of_exit_accts,
T4.no_of_cancels, 
T1.no_of_exit_accts_prev_m, 
CAST(T4.no_of_cancels AS DECIMAL(38,2))/CAST(T1.no_of_exit_accts_prev_m AS DECIMAL(38,2)) * 100 AS account_churn_rate,
CASE WHEN SPLIT_PART(T3.month,'-',2) IN ('04', '07', '10', '01') THEN 'Yes' ELSE 'No' END AS end_of_f_qtr

FROM T2 
LEFT JOIN T1 ON T1.month_exit = T2.month_exit AND T1.region = T2.region
LEFT JOIN T3 ON T2.month_exit = T3.month AND T2.region = T3.region
LEFT JOIN T4 ON T2.month_exit = T4.month AND T4.region = T2.region

ORDER BY month, fiscal_quarter, region),


---------------------------------------
-------Dynamic for all zoom------------
---------------------------------------


TB AS

(
WITH 
T1 AS
(SELECT to_char(dateadd(mm, 1, mrs_date), 'yyyy-mm') AS month_exit, SUM(mrs_exit) AS mrr_exit_prev_month, COUNT(DISTINCT zoom_account_no) AS no_of_exit_accts_prev_m
FROM rpt.domo_v_zr_mrs_by_ad_growth
WHERE to_char(mrs_date, 'mm-dd') in ('01-31', '02-28', '03-31', '04-30', '05-31', '06-30', '07-31', '08-31', '09-30', '10-31', '11-30', '12-31')
GROUP BY to_char(dateadd(mm, 1, mrs_date), 'yyyy-mm')),

T2 AS
(SELECT to_char(mrs_date, 'yyyy-mm') AS month_exit, SUM(mrs_exit) AS mrr_exit, COUNT(DISTINCT zoom_account_no) AS no_of_exit_accts,
SUM(CASE WHEN mrs_backlog IS NULL THEN 0 ELSE mrs_backlog END) AS mrr_backlog
FROM rpt.domo_v_zr_mrs_by_ad_growth
WHERE to_char(mrs_date, 'mm-dd') in ('01-31', '02-28', '03-31', '04-30', '05-31', '06-30', '07-31', '08-31', '09-30', '10-31', '11-30', '12-31')
GROUP BY to_char(mrs_date, 'yyyy-mm')),

T3 AS
(SELECT to_char(mrs_date, 'yyyy-mm') AS month, 
   fiscal_quarter, SUM(mrs_upsell) AS mrr_upsell, 
   SUM(mrs_new) AS mrr_new, 
   SUM(mrs_total_new) AS mrr_gross_billed, 
   SUM(mrs_net_new) AS mrr_net_billed, 
   SUM(mrs_downsell) AS mrr_downsell, 
   SUM(mrs_cancel) AS mrr_cancel, 
   SUM(mrs_total_churn + CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_gross_churn, 
   SUM(mrs_net_churn + CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_net_churn, 
   SUM(CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_adjustment,
   SUM(mrs_backlog_net_new) AS mrr_backlog_net_new
FROM rpt.domo_v_zr_mrs_by_ad_growth
GROUP BY fiscal_quarter, to_char(mrs_date, 'yyyy-mm')),

T4 AS
(SELECT to_char(mrs_date, 'yyyy-mm') AS month, fiscal_quarter, COUNT(DISTINCT zoom_account_no) AS no_of_cancels
FROM rpt.domo_v_zr_mrs_by_ad_growth
WHERE mrs_cancel < 0
GROUP BY fiscal_quarter, to_char(mrs_date, 'yyyy-mm'))

SELECT 
T3.month, 
T3.fiscal_quarter, 
'All Zoom' AS region, 
T1.mrr_exit_prev_month AS mrr_exit_billed_prev_month, 
T2.mrr_exit AS mrr_exit_billed, 
T2.mrr_backlog,
T2.mrr_exit + T2.mrr_backlog AS mrr_exit_booked, 
T3.mrr_upsell, 
T3.mrr_new, 
T3.mrr_gross_billed, 
T3.mrr_gross_billed + T3.mrr_backlog_net_new AS mrr_gross_booked,
T3.mrr_net_billed, 
T3.mrr_net_billed + T3.mrr_backlog_net_new AS mrr_net_booked,
T3.mrr_backlog_net_new,
T3.mrr_downsell, 
T3.mrr_cancel, 
T3.mrr_adjustment, 
T3.mrr_gross_churn, 
T3.mrr_net_churn,
T3.mrr_gross_churn/T1.mrr_exit_prev_month*(-100) AS gross_churn_rate, 
T3.mrr_net_churn/T1.mrr_exit_prev_month*(-100) AS net_churn_rate, 
T2.no_of_exit_accts,
T4.no_of_cancels, 
T1.no_of_exit_accts_prev_m, 
CAST(T4.no_of_cancels AS DECIMAL(38,2))/CAST(T1.no_of_exit_accts_prev_m AS DECIMAL(38,2)) * 100 AS account_churn_rate,
CASE WHEN SPLIT_PART(T3.month,'-',2) IN ('04', '07', '10', '01') THEN 'Yes' ELSE 'No' END AS end_of_f_qtr
FROM T2 
LEFT JOIN T1 ON T1.month_exit = T2.month_exit
LEFT JOIN T3 ON T2.month_exit = T3.month
LEFT JOIN T4 ON T2.month_exit = T4.month

ORDER BY month, fiscal_quarter, region)

SELECT * FROM TA 
UNION ALL 
SELECT * FROM TB

ORDER BY month, fiscal_quarter, region
