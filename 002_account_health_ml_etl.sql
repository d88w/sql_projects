---create schema acct_health


------------------------------------------------------
------------------------------------------------------
------------------------------------------------------
---------------ETL Stage 1 ---- usage-----------------
------------------------------------------------------ 
------------------------------------------------------
------------------------------------------------------

DELETE FROM etl_acct_health.ahs_1_usage_ad WHERE ahs_date >= (CURRENT_DATE - 92);

INSERT INTO etl_acct_health.ahs_1_usage_ad

WITH 
T1 AS 
(SELECT DATE(a.mrs_date) AS ahs_date,
  a.zoom_account_no, 
  a.account_name,
  DATE(a.accountcreateddate) AS account_created_date,
  DATEDIFF(days, DATE(a.accountcreateddate), a.mrs_date) AS account_age,
  a.sales_group_exit AS sales_group,
  a.employee_count,
  a.coreproduct,
  a.currentterm,
  a.data_com_industry,
  a.billtocountry,
  a.billtostate,
  
  (CAST(c.billing_hosts AS DECIMAL (16,2))) AS billing_hosts,
    
  (CAST(c.paid_user_count AS DECIMAL (16,2))) AS paid_user_count,

  (CAST(c.paid_user_count AS DECIMAL (16,2))) / 
    (CASE WHEN (CAST(c.billing_hosts AS DECIMAL (16,2))) = 0 THEN NULL ELSE (CAST(c.billing_hosts AS DECIMAL (16,2))) END) AS billing_hosts_utilization,

  ---(CAST(c.billing_hosts_utilization AS DECIMAL (16,2))) AS billing_hosts_utilization,
    
  (CAST(c.paid_user_mau AS DECIMAL (16,2))) AS paid_user_mau,

  (CAST(c.paid_user_mau AS DECIMAL (16,2))) / 
    (CASE WHEN (CAST(c.paid_user_count AS DECIMAL (16,2))) = 0 THEN NULL ELSE (CAST(c.paid_user_count AS DECIMAL (16,2))) END) AS paid_user_utilization,

  --- (CAST(c.paid_user_utilization AS DECIMAL (16,2))) AS paid_user_utilization,
  
  (CAST(c.free_user_count AS DECIMAL (16,2))) AS free_user_count,
    
  (CAST(c.free_user_mau AS DECIMAL (16,2))) AS free_user_mau,

  (CAST(c.free_user_mau AS DECIMAL (16,2))) / 
    (CASE WHEN (CAST(c.free_user_count AS DECIMAL (16,2))) = 0 THEN NULL ELSE (CAST(c.free_user_count AS DECIMAL (16,2))) END) AS free_user_utilization,

  --- (CAST(c.free_user_utilization AS DECIMAL (16,2))) AS free_user_utilization,  
   
  CASE WHEN b.total_zr_deployed IS NULL THEN '0' ELSE b.total_zr_deployed END AS total_zr_deployed,
  
  SUM(CAST(b.daily_meeting AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS total_meetings_last_30,
    
  SUM(CAST(b.daily_meeting_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS total_minutes_last_30,

  SUM(CAST(b.daily_meeting AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    / (CASE WHEN((CAST(c.paid_user_count AS DECIMAL (16,2))) + (CAST(c.free_user_count AS DECIMAL (16,2))))=0 THEN NULL
    ELSE ((CAST(c.paid_user_count AS DECIMAL (16,2))) + (CAST(c.free_user_count AS DECIMAL (16,2)))) END)
    AS meetings_per_user_last_30,
    
  SUM(CAST(b.daily_meeting_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    / (CASE WHEN((CAST(c.paid_user_count AS DECIMAL (16,2))) + (CAST(c.free_user_count AS DECIMAL (16,2))))=0 THEN NULL
    ELSE ((CAST(c.paid_user_count AS DECIMAL (16,2))) + (CAST(c.free_user_count AS DECIMAL (16,2)))) END)
    AS minutes_per_user_last_30,
    
  SUM(CAST(b.daily_meeting_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    /NULLIF(SUM(b.daily_meeting) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW),0)
    AS total_minutes_per_meeting_last_30,
    
  SUM(CAST(b.daily_paid_user_meeting AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS paid_meetings_last_30,
    
  SUM(CAST(b.daily_paid_user_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS paid_minutes_last_30,
    
  SUM(CAST(b.daily_paid_user_meeting AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    / (CASE WHEN((CAST(c.paid_user_count AS DECIMAL (16,2))))=0 THEN NULL ELSE ((CAST(c.paid_user_count AS DECIMAL (16,2)))) END)
    AS paid_meetings_per_user_last_30,
    
  SUM(CAST(b.daily_paid_user_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    / (CASE WHEN((CAST(c.paid_user_count AS DECIMAL (16,2))))=0 THEN NULL ELSE ((CAST(c.paid_user_count AS DECIMAL (16,2)))) END)
    AS paid_minutes_per_user_last_30,

  SUM(CAST(b.daily_paid_user_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    /NULLIF(SUM(b.daily_paid_user_meeting) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW),0) 
    AS paid_minutes_per_meeting_last_30,
    
  SUM(CAST(b.daily_free_user_meetings AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS free_meetings_last_30,
    
  SUM(CAST(b.daily_free_user_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS free_minutes_last_30,

  SUM(CAST(b.daily_free_user_meetings AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    / (CASE WHEN((CAST(c.free_user_count AS DECIMAL (16,2))))=0 THEN NULL ELSE (MAX(CAST(c.free_user_count AS DECIMAL (16,2)))) END)
    AS free_meetings_per_user_last_30,
    
  SUM(CAST(b.daily_free_user_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    / (CASE WHEN((CAST(c.free_user_count AS DECIMAL (16,2))))=0 THEN NULL ELSE (MAX(CAST(c.free_user_count AS DECIMAL (16,2)))) END)
    AS free_minutes_per_user_last_30,

  SUM(CAST(b.daily_free_user_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    /NULLIF(SUM(b.daily_free_user_meetings) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW),0)
    AS free_minutes_per_meeting_last_30, 
    
  SUM(CAST(b.daily_zr_meeting AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS zr_meetings_last_30,
    
  SUM(CAST(b.daily_zr_meeting_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS zr_minutes_last_30,

  SUM(CAST(b.daily_zr_meeting AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    / (CASE WHEN b.total_zr_deployed = 0 THEN NULL ELSE b.total_zr_deployed END)
    AS zr_meetings_per_zr_last_30,
    
  SUM(CAST(b.daily_zr_meeting_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
    / (CASE WHEN b.total_zr_deployed = 0 THEN NULL ELSE b.total_zr_deployed END)
    AS zr_minutes_per_zr_last_30,
    
  SUM(CAST(b.daily_zr_meeting_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    /NULLIF(SUM(b.daily_zr_meeting) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW),0)
    AS zr_minutes_per_meeting_last_30, 
    
  (SUM(CAST(b.daily_zr_meeting AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))
    /NULLIF((SUM(CAST(b.daily_meeting AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)),0)
    AS daily_zr_meeting_attach_rate,
  
  (SUM(CAST(b.daily_zr_meeting_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))
    /NULLIF((SUM(CAST(b.daily_meeting_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)),0)
    AS daily_zr_minutes_attach_rate,
  
  SUM(CAST(b.daily_webinars_meetings AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS webinar_meetings_last_30,
    
  SUM(CAST(b.daily_webinars_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    AS webinar_minutes_last_30,
 
  SUM(CAST(b.daily_webinars_minutes AS DECIMAL (16,2))) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
    /NULLIF(SUM(b.daily_webinars_meetings) 
    OVER (PARTITION BY a.zoom_account_no ORDER BY a.zoom_account_no, ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW),0)
    AS webinar_minutes_per_meeting_last_30, 

  (c.free_user_mau + c.paid_user_mau) AS unique_host_last_30days, 
  b.unique_logins_last_30days

FROM dm_sales.mrs_by_ad_growth_single a 
---FROM dm_sales.v_mrs_by_ad_growth_merge a 
LEFT OUTER JOIN rpt.zmbi_usage_datamart b ON a.zoom_account_no = b.zoom_account_number AND a.mrs_date = b.aggregation_date
LEFT OUTER JOIN rpt.zmbi_account_utilization_mrs c ON a.zoom_account_no = c.zoom_account_number AND a.mrs_date = c.date
WHERE a.mrs_date >= (CURRENT_DATE - 92)
--- AND a.mrs_date >= '2016-04-01'
AND a.through_reseller <> 'true'
AND a.is_reseller <> 'yes'
AND a.sales_group_exit <> '14.99'
AND a.zoom_account_no IS NOT NULL
GROUP BY 
a.mrs_date,
a.zoom_account_no, 
a.account_name, 
a.accountcreateddate, 
a.sales_group_exit, 
a.employee_count, 
a.coreproduct, 
a.currentterm,
a.data_com_industry,
a.billtocountry,
a.billtostate,
c.billing_hosts, 
c.paid_user_count, 
c.billing_hosts_utilization, 
c.paid_user_mau, 
c.paid_user_utilization, 
c.free_user_count, 
c.free_user_mau, 
c.free_user_utilization, 
b.total_zr_deployed, 
b.daily_meeting, 
b.daily_meeting_minutes, 
b.daily_paid_user_meeting, 
b.daily_paid_user_minutes, 
b.daily_free_user_meetings, 
b.daily_free_user_minutes, 
b.daily_zr_meeting, 
b.daily_zr_meeting_minutes, 
b.daily_webinars_meetings, 
b.daily_webinars_minutes, 
b.unique_logins_last_30days
)

SELECT T1.* from T1;

------------------------------------------------------
------------------------------------------------------
------------------------------------------------------
-----ETL Stage 2 ---- growth and everthing else-------
------------------------------------------------------
------------------------------------------------------
------------------------------------------------------

DELETE FROM etl_acct_health.ahs_2_usage_growth_ad WHERE ahs_date >= (CURRENT_DATE - 92);

INSERT INTO etl_acct_health.ahs_2_usage_growth_ad

WITH T2 AS 
(SELECT l.zoom_account_no,l.mrs_date AS opp_date, SUM(m.opp_amount) AS opp_amount FROM dm_sales.mrs_by_ad_growth_single l
LEFT JOIN
    (SELECT substring(sf_a.zoom_account_number__c from 1 for 6) AS zoom_account_number__c, SUM(sf_o.amount) AS opp_amount, sf_o.closedate, sf_o.stagename, DATE(sf_o.createddate) AS create_date
    FROM  src_sfdc.opportunity sf_o
    LEFT JOIN src_sfdc.account sf_a ON sf_a.id = sf_o.accountid
    WHERE sf_o.amount > 0
    GROUP BY sf_a.zoom_account_number__c, sf_o.closedate, sf_o.stagename,create_date) m
ON l.zoom_account_no = m.zoom_account_number__c
WHERE m.create_date < l.mrs_date AND m.closedate > l.mrs_date
GROUP BY l.zoom_account_no,l.mrs_date),

T3 AS 
(SELECT h.dt, MAX(h.termenddate) AS termenddate, i.zoom_account_number__c
FROM src_zuora.subscription_history h, src_zuora.account i
WHERE h.accountid = i.id
AND h.status = 'Active'
GROUP BY h.dt, i.zoom_account_number__c),

T4 AS
(SELECT j.*, k.zoom_account_number__c
FROM etl_acct_health.mrr_4_agg_waterfall_ad j, src_zuora.account k
WHERE j.acc_number = k.accountnumber)


SELECT 
 T1.ahs_date,
 T1.zoom_account_no,
 T1.account_name,
 T1.account_created_date,
 T1.account_age,
 T1.sales_group,
 T1.employee_count,
 T1.coreproduct,
 T1.currentterm,
 T1.data_com_industry,
 T1.billtocountry,
 T1.billtostate,
 T1.billing_hosts,
 T1.paid_user_count,
 T1.billing_hosts_utilization,
 T1.paid_user_mau,
 T1.paid_user_utilization,
 T1.free_user_count,
 T1.free_user_mau,
 T1.free_user_utilization,
 T1.total_zr_deployed,
 T1.total_meetings_last_30,
 T1.total_minutes_last_30,
 T1.meetings_per_user_last_30,
 T1.minutes_per_user_last_30,
 T1.total_minutes_per_meeting_last_30,
 T1.paid_meetings_last_30,
 T1.paid_minutes_last_30,
 T1.paid_meetings_per_user_last_30,
 T1.paid_minutes_per_user_last_30,
 T1.paid_minutes_per_meeting_last_30,
 T1.free_meetings_last_30,
 T1.free_minutes_last_30,
 T1.free_meetings_per_user_last_30,
 T1.free_minutes_per_user_last_30,
 T1.free_minutes_per_meeting_last_30,
 T1.zr_meetings_last_30,
 T1.zr_minutes_last_30,
 T1.zr_meetings_per_zr_last_30,
 T1.zr_minutes_per_zr_last_30,
 T1.zr_minutes_per_meeting_last_30,
 T1.daily_zr_meeting_attach_rate,
 T1.daily_zr_minutes_attach_rate,
 T1.webinar_meetings_last_30,
 T1.webinar_minutes_last_30,
 T1.webinar_minutes_per_meeting_last_30, 

 CASE WHEN T1.unique_host_last_30days IS NULL THEN 
  (MAX(CAST(T1.unique_host_last_30days AS DECIMAL (16,2))) 
  OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))
  ELSE T1.unique_host_last_30days END, 

 CASE WHEN T1.unique_logins_last_30days IS NULL THEN
  (MAX(CAST(T1.unique_logins_last_30days AS DECIMAL (16,2))) 
  OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)) 
  ELSE T1.unique_logins_last_30days END,

----mrr growth----

  T4.mrr_entry AS mrr_entry,
  T4.mrr_new AS mrr_new,
  T4.mrr_upsell AS mrr_upsell,
  T4.mrr_new + T4.mrr_upsell AS mrr_total_new,
  T4.mrr_downsell AS mrr_downsell,
  T4.mrr_cancel AS mrr_cancel,
  T4.mrr_downsell + T4.mrr_cancel AS mrr_total_churn,
  T4.mrr_exit AS mrr_exit, 

  SUM(T4.mrr_exit)/(CASE WHEN (LAG(SUM(T4.mrr_exit),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T4.mrr_exit),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) END)-1 AS mrr_growth_30,
  
  SUM(T4.mrr_exit)/(CASE WHEN (LAG(SUM(T4.mrr_exit),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T4.mrr_exit),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) END)-1 AS mrr_growth_60,
    
  SUM(T4.mrr_exit)/(CASE WHEN (LAG(SUM(T4.mrr_exit),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T4.mrr_exit),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) END)-1 AS mrr_growth_90,
    
  SUM(T4.mrr_exit)/(CASE WHEN (LAG(SUM(T4.mrr_exit),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T4.mrr_exit),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) END)-1 AS mrr_growth_180,

----usage growth----

  SUM(T1.billing_hosts)/(CASE WHEN (LAG(SUM(T1.billing_hosts),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.billing_hosts),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS billing_hosts_growth_30,
  
  SUM(T1.billing_hosts)/(CASE WHEN (LAG(SUM(T1.billing_hosts),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.billing_hosts),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS billing_hosts_growth_60,
    
  SUM(T1.billing_hosts)/(CASE WHEN (LAG(SUM(T1.billing_hosts),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.billing_hosts),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS billing_hosts_growth_90,
    
  SUM(T1.billing_hosts)/(CASE WHEN (LAG(SUM(T1.billing_hosts),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.billing_hosts),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS billing_hosts_growth_180,


  SUM(T1.paid_user_count)/(CASE WHEN (LAG(SUM(T1.paid_user_count),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_count),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_count_growth_30,
  
  SUM(T1.paid_user_count)/(CASE WHEN (LAG(SUM(T1.paid_user_count),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_count),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_count_growth_60,
    
  SUM(T1.paid_user_count)/(CASE WHEN (LAG(SUM(T1.paid_user_count),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_count),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_count_growth_90,
    
  SUM(T1.paid_user_count)/(CASE WHEN (LAG(SUM(T1.paid_user_count),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_count),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_count_growth_180,
  

  SUM(T1.free_user_count)/(CASE WHEN (LAG(SUM(T1.free_user_count),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_count),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_count_growth_30,
  
  SUM(T1.free_user_count)/(CASE WHEN (LAG(SUM(T1.free_user_count),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_count),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_count_growth_60,
    
  SUM(T1.free_user_count)/(CASE WHEN (LAG(SUM(T1.free_user_count),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_count),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_count_growth_90,
    
  SUM(T1.free_user_count)/(CASE WHEN (LAG(SUM(T1.free_user_count),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_count),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_count_growth_180,
  

  SUM(T1.paid_user_mau)/(CASE WHEN (LAG(SUM(T1.paid_user_mau),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_mau),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_mau_growth_30,
  
  SUM(T1.paid_user_mau)/(CASE WHEN (LAG(SUM(T1.paid_user_mau),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_mau),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_mau_growth_60,
    
  SUM(T1.paid_user_mau)/(CASE WHEN (LAG(SUM(T1.paid_user_mau),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_mau),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_mau_growth_90,
    
  SUM(T1.paid_user_mau)/(CASE WHEN (LAG(SUM(T1.paid_user_mau),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_mau),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_mau_growth_180,


  SUM(T1.free_user_mau)/(CASE WHEN (LAG(SUM(T1.free_user_mau),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_mau),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_mau_growth_30,
  
  SUM(T1.free_user_mau)/(CASE WHEN (LAG(SUM(T1.free_user_mau),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_mau),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_mau_growth_60,
    
  SUM(T1.free_user_mau)/(CASE WHEN (LAG(SUM(T1.free_user_mau),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_mau),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_mau_growth_90,
    
  SUM(T1.free_user_mau)/(CASE WHEN (LAG(SUM(T1.free_user_mau),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_mau),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_mau_growth_180,
 

  SUM(T1.billing_hosts_utilization)/(CASE WHEN (LAG(SUM(T1.billing_hosts_utilization),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.billing_hosts_utilization),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS billing_hosts_util_growth_30,
  
  SUM(T1.billing_hosts_utilization)/(CASE WHEN (LAG(SUM(T1.billing_hosts_utilization),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.billing_hosts_utilization),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS billing_hosts_util_growth_60,
    
  SUM(T1.billing_hosts_utilization)/(CASE WHEN (LAG(SUM(T1.billing_hosts_utilization),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.billing_hosts_utilization),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS billing_hosts_util_growth_90,
    
  SUM(T1.billing_hosts_utilization)/(CASE WHEN (LAG(SUM(T1.billing_hosts_utilization),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.billing_hosts_utilization),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS billing_hosts_util_growth_180,


  SUM(T1.paid_user_utilization)/(CASE WHEN (LAG(SUM(T1.paid_user_utilization),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_utilization),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_util_growth_30,
  
  SUM(T1.paid_user_utilization)/(CASE WHEN (LAG(SUM(T1.paid_user_utilization),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_utilization),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_util_growth_60,
    
  SUM(T1.paid_user_utilization)/(CASE WHEN (LAG(SUM(T1.paid_user_utilization),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_utilization),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_util_growth_90,
    
  SUM(T1.paid_user_utilization)/(CASE WHEN (LAG(SUM(T1.paid_user_utilization),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_user_utilization),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_user_util_growth_180,


  SUM(T1.free_user_utilization)/(CASE WHEN (LAG(SUM(T1.free_user_utilization),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_utilization),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_util_growth_30,
  
  SUM(T1.free_user_utilization)/(CASE WHEN (LAG(SUM(T1.free_user_utilization),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_utilization),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_util_growth_60,
    
  SUM(T1.free_user_utilization)/(CASE WHEN (LAG(SUM(T1.free_user_utilization),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_utilization),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_util_growth_90,
    
  SUM(T1.free_user_utilization)/(CASE WHEN (LAG(SUM(T1.free_user_utilization),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_user_utilization),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_user_util_growth_180,


  SUM(T1.total_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.total_meetings_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_meetings_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS total_mthly_mtgs_growth_30,
  
  SUM(T1.total_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.total_meetings_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_meetings_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS total_mthly_mtgs_growth_60,
    
  SUM(T1.total_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.total_meetings_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_meetings_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS total_mthly_mtgs_growth_90,
    
  SUM(T1.total_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.total_meetings_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_meetings_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS total_mthly_mtgs_growth_180, 
  

  SUM(T1.total_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.total_minutes_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_minutes_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS total_mthly_min_growth_30,
  
  SUM(T1.total_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.total_minutes_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_minutes_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS total_mthly_min_growth_60,
    
  SUM(T1.total_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.total_minutes_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_minutes_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS total_mthly_min_growth_90,
    
  SUM(T1.total_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.total_minutes_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_minutes_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS total_mthly_min_growth_180, 
  

  SUM(T1.meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.meetings_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.meetings_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_mtgs_per_user_growth_30,
  
  SUM(T1.meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.meetings_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.meetings_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_mtgs_per_user_growth_60,
    
  SUM(T1.meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.meetings_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.meetings_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_mtgs_per_user_growth_90,
    
  SUM(T1.meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.meetings_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.meetings_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_mtgs_per_user_growth_180, 
  

  SUM(T1.minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.minutes_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.minutes_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_min_per_user_growth_30,
  
  SUM(T1.minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.minutes_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.minutes_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_min_per_user_growth_60,
    
  SUM(T1.minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.minutes_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.minutes_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_min_per_user_growth_90,
    
  SUM(T1.minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.minutes_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.minutes_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_min_per_user_growth_180, 
  

  SUM(T1.total_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.total_minutes_per_meeting_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_minutes_per_meeting_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_min_per_mtg_growth_30,
  
  SUM(T1.total_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.total_minutes_per_meeting_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_minutes_per_meeting_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_min_per_mtg_growth_60,
    
  SUM(T1.total_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.total_minutes_per_meeting_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_minutes_per_meeting_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_min_per_mtg_growth_90,
    
  SUM(T1.total_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.total_minutes_per_meeting_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.total_minutes_per_meeting_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS mthly_min_per_mtg_growth_180, 
  

  SUM(T1.paid_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.paid_meetings_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_meetings_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_mtgs_growth_30,
  
  SUM(T1.paid_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.paid_meetings_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_meetings_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_mtgs_growth_60,
    
  SUM(T1.paid_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.paid_meetings_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_meetings_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_mtgs_growth_90,
    
  SUM(T1.paid_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.paid_meetings_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_meetings_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_mtgs_growth_180, 
  

  SUM(T1.paid_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_growth_30,
  
  SUM(T1.paid_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_growth_60,
    
  SUM(T1.paid_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_growth_90,
    
  SUM(T1.paid_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_growth_180, 
  

  SUM(T1.paid_meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.paid_meetings_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_meetings_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_mtgs_per_user_growth_30,
  
  SUM(T1.paid_meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.paid_meetings_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_meetings_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_mtgs_per_user_growth_60,
    
  SUM(T1.paid_meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.paid_meetings_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_meetings_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_mtgs_per_user_growth_90,
    
  SUM(T1.paid_meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.paid_meetings_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_meetings_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_mtgs_per_user_growth_180, 
  

  SUM(T1.paid_minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_per_user_growth_30,
  
  SUM(T1.paid_minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_per_user_growth_60,
    
  SUM(T1.paid_minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_per_user_growth_90,
    
  SUM(T1.paid_minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_per_user_growth_180, 
  

  SUM(T1.paid_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_per_meeting_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_per_meeting_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_per_mtg_growth_30,
  
  SUM(T1.paid_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_per_meeting_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_per_meeting_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_per_mtg_growth_60,
    
  SUM(T1.paid_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_per_meeting_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_per_meeting_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_per_mtg_growth_90,
    
  SUM(T1.paid_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.paid_minutes_per_meeting_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.paid_minutes_per_meeting_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS paid_mthly_min_per_mtg_growth_180, 
  

  SUM(T1.free_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.free_meetings_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_meetings_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_mtgs_growth_30,
  
  SUM(T1.free_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.free_meetings_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_meetings_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_mtgs_growth_60,
    
  SUM(T1.free_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.free_meetings_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_meetings_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_mtgs_growth_90,
    
  SUM(T1.free_meetings_last_30)/(CASE WHEN (LAG(SUM(T1.free_meetings_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_meetings_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_mtgs_growth_180, 
  

  SUM(T1.free_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_growth_30,
  
  SUM(T1.free_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_growth_60,
    
  SUM(T1.free_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_growth_90,
    
  SUM(T1.free_minutes_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_growth_180, 
  

  SUM(T1.free_meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.free_meetings_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_meetings_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_mtgs_per_user_growth_30,
  
  SUM(T1.free_meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.free_meetings_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_meetings_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_mtgs_per_user_growth_60,
    
  SUM(T1.free_meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.free_meetings_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_meetings_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_mtgs_per_user_growth_90,
    
  SUM(T1.free_meetings_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.free_meetings_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_meetings_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_mtgs_per_user_growth_180, 
  

  SUM(T1.free_minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_per_user_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_per_user_growth_30,
  
  SUM(T1.free_minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_per_user_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_per_user_growth_60,
    
  SUM(T1.free_minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_per_user_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_per_user_growth_90,
    
  SUM(T1.free_minutes_per_user_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_per_user_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_per_user_growth_180, 
  

  SUM(T1.free_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_per_meeting_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_per_meeting_last_30),30) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_per_mtg_growth_30,
  
  SUM(T1.free_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_per_meeting_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_per_meeting_last_30),60) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_per_mtg_growth_60,
    
  SUM(T1.free_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_per_meeting_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_per_meeting_last_30),90) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_per_mtg_growth_90,
    
  SUM(T1.free_minutes_per_meeting_last_30)/(CASE WHEN (LAG(SUM(T1.free_minutes_per_meeting_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, ahs_date)) = 0 THEN NULL
    ELSE (LAG(SUM(T1.free_minutes_per_meeting_last_30),180) OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date)) END)-1 AS free_mthly_min_per_mtg_growth_180, 

  SUM(T2.opp_amount) AS opp_amount, 

  T3.termenddate, DATEDIFF(days, T1.ahs_date, DATE(T3.termenddate)) AS days_left_in_term,

  CASE WHEN T4.mrr_downsell < 0 THEN 1 ELSE 0 END AS churn_downsell,
  CASE WHEN T4.mrr_cancel < 0 THEN 1 ELSE 0 END AS churn_cancel,
  CASE WHEN (T4.mrr_downsell + T4.mrr_cancel) < 0 THEN 1 ELSE 0 END AS churn_gross,

  MAX(CASE WHEN T4.mrr_downsell < 0 THEN 1 ELSE 0 END) 
    OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date ROWS BETWEEN CURRENT ROW AND 90 FOLLOWING) AS churn_downsell_next_90,

  MAX(CASE WHEN T4.mrr_cancel < 0 THEN 1 ELSE 0 END) 
    OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date ROWS BETWEEN CURRENT ROW AND 90 FOLLOWING) AS churn_cancel_next_90,

  MAX(CASE WHEN (T4.mrr_downsell + T4.mrr_cancel) < 0 THEN 1 ELSE 0 END) 
    OVER (PARTITION BY T1.zoom_account_no ORDER BY T1.zoom_account_no, T1.ahs_date ROWS BETWEEN CURRENT ROW AND 90 FOLLOWING) AS churn_gross_next_90

FROM etl_acct_health.ahs_1_usage_ad T1 
LEFT OUTER JOIN T2 ON T1.zoom_account_no = T2.zoom_account_no AND T2.opp_date = T1.ahs_date
LEFT OUTER JOIN T3 ON T1.zoom_account_no = T3.zoom_account_number__c AND T1.ahs_date = T3.dt
LEFT OUTER JOIN T4 ON T1.zoom_account_no = T4.zoom_account_number__c AND T1.ahs_date = T4.load_date

WHERE T1.zoom_account_no IS NOT NULL
AND T1.ahs_date >= (CURRENT_DATE - 92)

GROUP BY 
T1.ahs_date, 
T1.zoom_account_no, 
T1.account_name, 
T1.account_created_date, 
T1.account_age, 
T1.sales_group, 
T1.employee_count, 
T1.coreproduct, 
T1.currentterm, 
T1.data_com_industry,
T1.billtocountry,
T1.billtostate,
T1.total_zr_deployed, 
T1.zr_meetings_last_30, 
T1.zr_minutes_last_30, 
T1.zr_meetings_per_zr_last_30, 
T1.zr_minutes_per_zr_last_30, 
T1.zr_minutes_per_meeting_last_30, 
T1.daily_zr_meeting_attach_rate, 
T1.daily_zr_minutes_attach_rate, 
T1.webinar_meetings_last_30, 
T1.webinar_minutes_last_30, 
T1.webinar_minutes_per_meeting_last_30, 
T1.unique_host_last_30days, 
T1.unique_logins_last_30days,
T1.billing_hosts,
T1.paid_user_count,
T1.free_user_count,
T1.paid_user_mau,
T1.free_user_mau,
T1.billing_hosts_utilization,
T1.paid_user_utilization,
T1.free_user_utilization,
T1.total_meetings_last_30,
T1.total_minutes_last_30,
T1.meetings_per_user_last_30,
T1.minutes_per_user_last_30,
T1.total_minutes_per_meeting_last_30,
T1.paid_meetings_last_30,
T1.paid_minutes_last_30,
T1.paid_meetings_per_user_last_30,
T1.paid_minutes_per_user_last_30,
T1.paid_minutes_per_meeting_last_30,
T1.free_meetings_last_30,
T1.free_minutes_last_30,
T1.free_meetings_per_user_last_30,
T1.free_minutes_per_user_last_30,
T1.free_minutes_per_meeting_last_30,

T3.termenddate,
T4.mrr_entry,
T4.mrr_new,
T4.mrr_upsell,
T4.mrr_downsell,
T4.mrr_cancel,
T4.mrr_exit


ORDER BY T1.ahs_date ASC
;


------------------------------------------------------
------------------------------------------------------
------------------------------------------------------
---------- ETL Stage 3 ---- Machine Learning ---------
------------------------------------------------------
------------------------------------------------------
------------------------------------------------------

---------------------------------------------------------------------------------------------------
---machine learning dataset 3 -------NO WEEKENDS---------------------------------------------------
---------------------------------------------------------------------------------------------------
---machine learning dataset all data--account health-----------------------------------------------
---ahs_date starts on 10/13/2016-------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

DELETE FROM etl_acct_health.ahs_3_ml_ad WHERE ahs_date >= (CURRENT_DATE - 92);

INSERT INTO etl_acct_health.ahs_3_ml_ad

SELECT ahs_date,
zoom_account_no,
account_name,
account_created_date,
account_age,
CASE WHEN sales_group IS NULL THEN 'UnDefined' WHEN sales_group = '' THEN 'UnDefined' ELSE sales_group END,
CASE WHEN employee_count IS NULL THEN 'UnDefined' WHEN employee_count = '' THEN 'UnDefined' ELSE employee_count END,
CASE WHEN coreproduct = '' THEN 'Zoom Room Only' WHEN coreproduct IS NULL THEN 'Zoom Room Only' ELSE coreproduct END,
currentterm,
CASE WHEN data_com_industry IS NULL THEN 'UnDefined' WHEN data_com_industry = '' THEN 'UnDefined' ELSE data_com_industry END,
CASE WHEN billtostate IS NULL AND billtocountry IS NOT NULL THEN billtocountry WHEN billtostate = '' AND billtocountry IS NOT NULL THEN billtocountry 
     WHEN billtostate IS NULL AND billtocountry IS NULL THEN 'UnDefined' WHEN billtostate = '' AND billtocountry IS NULL THEN 'UnDefined'
     ELSE billtostate END,
CASE WHEN billtocountry IS NULL THEN 'UnDefined' WHEN billtocountry = '' THEN 'UnDefined' ELSE billtocountry END,

mrr_entry,
mrr_exit,

CASE WHEN mrr_growth_30 IS NULL THEN 0.00 ELSE mrr_growth_30 END,
CASE WHEN mrr_growth_60 IS NULL AND mrr_growth_30 IS NULL THEN 0.00 
     WHEN mrr_growth_60 IS NULL AND mrr_growth_30 IS NOT NULL THEN mrr_growth_30 
     ELSE mrr_growth_60 END,
CASE WHEN mrr_growth_30 IS NULL THEN 0.00 
     WHEN mrr_growth_90 IS NULL AND mrr_growth_60 IS NULL AND mrr_growth_30 IS NOT NULL THEN mrr_growth_30
     WHEN mrr_growth_90 IS NULL AND mrr_growth_60 IS NOT NULL THEN mrr_growth_60 
     ELSE mrr_growth_90 END,
CASE WHEN mrr_growth_30 IS NULL THEN 0.00 
     WHEN mrr_growth_180 IS NULL AND mrr_growth_90 IS NULL AND mrr_growth_60 IS NULL AND mrr_growth_30 IS NOT NULL THEN mrr_growth_30
     WHEN mrr_growth_180 IS NULL AND mrr_growth_90 IS NULL AND mrr_growth_60 IS NOT NULL THEN mrr_growth_60
     WHEN mrr_growth_180 IS NULL AND mrr_growth_90 IS NOT NULL THEN mrr_growth_90 
     ELSE mrr_growth_180 END,

CASE WHEN billing_hosts IS NULL THEN 
  MAX(billing_hosts) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
  ELSE billing_hosts END,

CASE WHEN paid_user_count IS NULL THEN 
  MAX(paid_user_count) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
  ELSE paid_user_count END,
  
CASE WHEN billing_hosts_utilization IS NULL AND paid_user_count = 0 THEN 0
   WHEN billing_hosts_utilization IS NULL AND paid_user_count <> 0 THEN 
    MAX(billing_hosts_utilization) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
   ELSE billing_hosts_utilization END, 
   
CASE WHEN paid_user_mau IS NULL THEN 
  MAX(paid_user_mau) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
  ELSE paid_user_mau END,
  
CASE WHEN paid_user_utilization IS NULL AND paid_user_mau = 0 THEN 0 
   WHEN paid_user_utilization IS NULL AND paid_user_mau <> 0 THEN 
    MAX(paid_user_utilization) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
   ELSE paid_user_utilization END,
   
CASE WHEN free_user_count IS NULL THEN 
  MAX(free_user_count) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
  ELSE free_user_count END,
  
CASE WHEN free_user_mau IS NULL THEN 
  MAX(free_user_mau) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
  ELSE free_user_mau END,

CASE WHEN free_user_utilization IS NULL AND free_user_mau = 0 THEN 0 
   WHEN free_user_utilization IS NULL AND free_user_mau <> 0 THEN 
    MAX(free_user_utilization) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
   ELSE free_user_utilization END,

CASE WHEN total_zr_deployed = 0 THEN 
    MAX(total_zr_deployed) OVER (PARTITION BY zoom_account_no ORDER BY zoom_account_no, ahs_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
  ELSE total_zr_deployed END,

CASE WHEN total_meetings_last_30 IS NULL THEN 0 ELSE total_meetings_last_30 END,
CASE WHEN total_minutes_last_30 IS NULL THEN 0 ELSE total_minutes_last_30 END,
CASE WHEN meetings_per_user_last_30 IS NULL THEN 0 ELSE meetings_per_user_last_30 END,
CASE WHEN minutes_per_user_last_30 IS NULL THEN 0 ELSE minutes_per_user_last_30 END,
CASE WHEN total_minutes_per_meeting_last_30 IS NULL THEN 0 ELSE total_minutes_per_meeting_last_30 END,

CASE WHEN paid_meetings_last_30 IS NULL THEN 0 ELSE paid_meetings_last_30 END,
CASE WHEN paid_minutes_last_30 IS NULL THEN 0 ELSE paid_minutes_last_30 END,
CASE WHEN paid_meetings_per_user_last_30 IS NULL THEN 0 ELSE paid_meetings_per_user_last_30 END,
CASE WHEN paid_minutes_per_user_last_30 IS NULL THEN 0 ELSE paid_minutes_per_user_last_30 END,
CASE WHEN paid_minutes_per_meeting_last_30 IS NULL THEN 0 ELSE paid_minutes_per_meeting_last_30 END,

CASE WHEN free_meetings_last_30 IS NULL THEN 0 ELSE free_meetings_last_30 END,
CASE WHEN free_minutes_last_30 IS NULL THEN 0 ELSE free_minutes_last_30 END,
CASE WHEN free_meetings_per_user_last_30 IS NULL THEN 0 ELSE free_meetings_per_user_last_30 END,
CASE WHEN free_minutes_per_user_last_30 IS NULL THEN 0 ELSE free_minutes_per_user_last_30 END,
CASE WHEN free_minutes_per_meeting_last_30 IS NULL THEN 0 ELSE free_minutes_per_meeting_last_30 END,

CASE WHEN zr_meetings_last_30 IS NULL THEN 0 ELSE zr_meetings_last_30 END,
CASE WHEN zr_minutes_last_30 IS NULL THEN 0 ELSE zr_minutes_last_30 END,
CASE WHEN zr_meetings_per_zr_last_30 IS NULL THEN 0 ELSE zr_meetings_per_zr_last_30 END,
CASE WHEN zr_minutes_per_zr_last_30 IS NULL THEN 0 ELSE zr_minutes_per_zr_last_30 END,
CASE WHEN zr_minutes_per_meeting_last_30 IS NULL THEN 0 ELSE zr_minutes_per_meeting_last_30 END,
CASE WHEN daily_zr_meeting_attach_rate IS NULL THEN 0 ELSE daily_zr_meeting_attach_rate END,
CASE WHEN daily_zr_minutes_attach_rate IS NULL THEN 0 ELSE daily_zr_minutes_attach_rate END,

CASE WHEN webinar_meetings_last_30 IS NULL THEN 0 ELSE webinar_meetings_last_30 END,
CASE WHEN webinar_minutes_last_30 IS NULL THEN 0 ELSE webinar_minutes_last_30 END,
CASE WHEN webinar_minutes_per_meeting_last_30 IS NULL THEN 0 ELSE webinar_minutes_per_meeting_last_30 END,

CASE WHEN unique_host_last_30days IS NULL THEN 0 ELSE unique_host_last_30days END,
CASE WHEN unique_logins_last_30days IS NULL THEN 0 ELSE unique_logins_last_30days END,

---usage growth items are treated differently:

CASE WHEN billing_hosts_growth_30 IS NULL THEN 0.00 ELSE billing_hosts_growth_30 END,
CASE WHEN billing_hosts_growth_60 IS NULL AND billing_hosts_growth_30 IS NULL THEN 0.00 
     WHEN billing_hosts_growth_60 IS NULL AND billing_hosts_growth_30 IS NOT NULL THEN billing_hosts_growth_30 
     ELSE billing_hosts_growth_60 END,
CASE WHEN billing_hosts_growth_30 IS NULL THEN 0.00 
     WHEN billing_hosts_growth_90 IS NULL AND billing_hosts_growth_60 IS NULL AND billing_hosts_growth_30 IS NOT NULL THEN billing_hosts_growth_30
     WHEN billing_hosts_growth_90 IS NULL AND billing_hosts_growth_60 IS NOT NULL THEN billing_hosts_growth_60 
     ELSE billing_hosts_growth_90 END,
CASE WHEN billing_hosts_growth_30 IS NULL THEN 0.00 
     WHEN billing_hosts_growth_180 IS NULL AND billing_hosts_growth_90 IS NULL AND billing_hosts_growth_60 IS NULL AND billing_hosts_growth_30 IS NOT NULL THEN billing_hosts_growth_30
     WHEN billing_hosts_growth_180 IS NULL AND billing_hosts_growth_90 IS NULL AND billing_hosts_growth_60 IS NOT NULL THEN billing_hosts_growth_60
     WHEN billing_hosts_growth_180 IS NULL AND billing_hosts_growth_90 IS NOT NULL THEN billing_hosts_growth_90 
     ELSE billing_hosts_growth_180 END,


CASE WHEN billing_hosts_util_growth_30 IS NULL THEN 0.00 ELSE billing_hosts_util_growth_30 END,
CASE WHEN billing_hosts_util_growth_60 IS NULL AND billing_hosts_util_growth_30 IS NULL THEN 0.00 
     WHEN billing_hosts_util_growth_60 IS NULL AND billing_hosts_util_growth_30 IS NOT NULL THEN billing_hosts_util_growth_30 
     ELSE billing_hosts_util_growth_60 END,
CASE WHEN billing_hosts_util_growth_30 IS NULL THEN 0.00 
     WHEN billing_hosts_util_growth_90 IS NULL AND billing_hosts_util_growth_60 IS NULL AND billing_hosts_util_growth_30 IS NOT NULL THEN billing_hosts_util_growth_30
     WHEN billing_hosts_util_growth_90 IS NULL AND billing_hosts_util_growth_60 IS NOT NULL THEN billing_hosts_util_growth_60 
     ELSE billing_hosts_util_growth_90 END,
CASE WHEN billing_hosts_util_growth_30 IS NULL THEN 0.00 
     WHEN billing_hosts_util_growth_180 IS NULL AND billing_hosts_util_growth_90 IS NULL AND billing_hosts_util_growth_60 IS NULL AND billing_hosts_util_growth_30 IS NOT NULL THEN billing_hosts_util_growth_30
     WHEN billing_hosts_util_growth_180 IS NULL AND billing_hosts_util_growth_90 IS NULL AND billing_hosts_util_growth_60 IS NOT NULL THEN billing_hosts_util_growth_60
     WHEN billing_hosts_util_growth_180 IS NULL AND billing_hosts_util_growth_90 IS NOT NULL THEN billing_hosts_util_growth_90 
     ELSE billing_hosts_util_growth_180 END,



CASE WHEN free_mthly_min_growth_30 IS NULL THEN 0.00 ELSE free_mthly_min_growth_30 END,
CASE WHEN free_mthly_min_growth_60 IS NULL AND free_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_growth_60 IS NULL AND free_mthly_min_growth_30 IS NOT NULL THEN free_mthly_min_growth_30 
     ELSE free_mthly_min_growth_60 END,
CASE WHEN free_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_growth_90 IS NULL AND free_mthly_min_growth_60 IS NULL AND free_mthly_min_growth_30 IS NOT NULL THEN free_mthly_min_growth_30
     WHEN free_mthly_min_growth_90 IS NULL AND free_mthly_min_growth_60 IS NOT NULL THEN free_mthly_min_growth_60 
     ELSE free_mthly_min_growth_90 END,
CASE WHEN free_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_growth_180 IS NULL AND free_mthly_min_growth_90 IS NULL AND free_mthly_min_growth_60 IS NULL AND free_mthly_min_growth_30 IS NOT NULL THEN free_mthly_min_growth_30
     WHEN free_mthly_min_growth_180 IS NULL AND free_mthly_min_growth_90 IS NULL AND free_mthly_min_growth_60 IS NOT NULL THEN free_mthly_min_growth_60
     WHEN free_mthly_min_growth_180 IS NULL AND free_mthly_min_growth_90 IS NOT NULL THEN free_mthly_min_growth_90 
     ELSE free_mthly_min_growth_180 END,



CASE WHEN free_mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 ELSE free_mthly_min_per_mtg_growth_30 END,
CASE WHEN free_mthly_min_per_mtg_growth_60 IS NULL AND free_mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_per_mtg_growth_60 IS NULL AND free_mthly_min_per_mtg_growth_30 IS NOT NULL THEN free_mthly_min_per_mtg_growth_30 
     ELSE free_mthly_min_per_mtg_growth_60 END,
CASE WHEN free_mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_per_mtg_growth_90 IS NULL AND free_mthly_min_per_mtg_growth_60 IS NULL AND free_mthly_min_per_mtg_growth_30 IS NOT NULL THEN free_mthly_min_per_mtg_growth_30
     WHEN free_mthly_min_per_mtg_growth_90 IS NULL AND free_mthly_min_per_mtg_growth_60 IS NOT NULL THEN free_mthly_min_per_mtg_growth_60 
     ELSE free_mthly_min_per_mtg_growth_90 END,
CASE WHEN free_mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_per_mtg_growth_180 IS NULL AND free_mthly_min_per_mtg_growth_90 IS NULL AND free_mthly_min_per_mtg_growth_60 IS NULL AND free_mthly_min_per_mtg_growth_30 IS NOT NULL THEN free_mthly_min_per_mtg_growth_30
     WHEN free_mthly_min_per_mtg_growth_180 IS NULL AND free_mthly_min_per_mtg_growth_90 IS NULL AND free_mthly_min_per_mtg_growth_60 IS NOT NULL THEN free_mthly_min_per_mtg_growth_60
     WHEN free_mthly_min_per_mtg_growth_180 IS NULL AND free_mthly_min_per_mtg_growth_90 IS NOT NULL THEN free_mthly_min_per_mtg_growth_90 
     ELSE free_mthly_min_per_mtg_growth_180 END,



CASE WHEN free_mthly_min_per_user_growth_30 IS NULL THEN 0.00 ELSE free_mthly_min_per_user_growth_30 END,
CASE WHEN free_mthly_min_per_user_growth_60 IS NULL AND free_mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_per_user_growth_60 IS NULL AND free_mthly_min_per_user_growth_30 IS NOT NULL THEN free_mthly_min_per_user_growth_30 
     ELSE free_mthly_min_per_user_growth_60 END,
CASE WHEN free_mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_per_user_growth_90 IS NULL AND free_mthly_min_per_user_growth_60 IS NULL AND free_mthly_min_per_user_growth_30 IS NOT NULL THEN free_mthly_min_per_user_growth_30
     WHEN free_mthly_min_per_user_growth_90 IS NULL AND free_mthly_min_per_user_growth_60 IS NOT NULL THEN free_mthly_min_per_user_growth_60 
     ELSE free_mthly_min_per_user_growth_90 END,
CASE WHEN free_mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_min_per_user_growth_180 IS NULL AND free_mthly_min_per_user_growth_90 IS NULL AND free_mthly_min_per_user_growth_60 IS NULL AND free_mthly_min_per_user_growth_30 IS NOT NULL THEN free_mthly_min_per_user_growth_30
     WHEN free_mthly_min_per_user_growth_180 IS NULL AND free_mthly_min_per_user_growth_90 IS NULL AND free_mthly_min_per_user_growth_60 IS NOT NULL THEN free_mthly_min_per_user_growth_60
     WHEN free_mthly_min_per_user_growth_180 IS NULL AND free_mthly_min_per_user_growth_90 IS NOT NULL THEN free_mthly_min_per_user_growth_90 
     ELSE free_mthly_min_per_user_growth_180 END,



CASE WHEN free_mthly_mtgs_growth_30 IS NULL THEN 0.00 ELSE free_mthly_mtgs_growth_30 END,
CASE WHEN free_mthly_mtgs_growth_60 IS NULL AND free_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_mtgs_growth_60 IS NULL AND free_mthly_mtgs_growth_30 IS NOT NULL THEN free_mthly_mtgs_growth_30 
     ELSE free_mthly_mtgs_growth_60 END,
CASE WHEN free_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_mtgs_growth_90 IS NULL AND free_mthly_mtgs_growth_60 IS NULL AND free_mthly_mtgs_growth_30 IS NOT NULL THEN free_mthly_mtgs_growth_30
     WHEN free_mthly_mtgs_growth_90 IS NULL AND free_mthly_mtgs_growth_60 IS NOT NULL THEN free_mthly_mtgs_growth_60 
     ELSE free_mthly_mtgs_growth_90 END,
CASE WHEN free_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_mtgs_growth_180 IS NULL AND free_mthly_mtgs_growth_90 IS NULL AND free_mthly_mtgs_growth_60 IS NULL AND free_mthly_mtgs_growth_30 IS NOT NULL THEN free_mthly_mtgs_growth_30
     WHEN free_mthly_mtgs_growth_180 IS NULL AND free_mthly_mtgs_growth_90 IS NULL AND free_mthly_mtgs_growth_60 IS NOT NULL THEN free_mthly_mtgs_growth_60
     WHEN free_mthly_mtgs_growth_180 IS NULL AND free_mthly_mtgs_growth_90 IS NOT NULL THEN free_mthly_mtgs_growth_90 
     ELSE free_mthly_mtgs_growth_180 END,



CASE WHEN free_mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 ELSE free_mthly_mtgs_per_user_growth_30 END,
CASE WHEN free_mthly_mtgs_per_user_growth_60 IS NULL AND free_mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_mtgs_per_user_growth_60 IS NULL AND free_mthly_mtgs_per_user_growth_30 IS NOT NULL THEN free_mthly_mtgs_per_user_growth_30 
     ELSE free_mthly_mtgs_per_user_growth_60 END,
CASE WHEN free_mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_mtgs_per_user_growth_90 IS NULL AND free_mthly_mtgs_per_user_growth_60 IS NULL AND free_mthly_mtgs_per_user_growth_30 IS NOT NULL THEN free_mthly_mtgs_per_user_growth_30
     WHEN free_mthly_mtgs_per_user_growth_90 IS NULL AND free_mthly_mtgs_per_user_growth_60 IS NOT NULL THEN free_mthly_mtgs_per_user_growth_60 
     ELSE free_mthly_mtgs_per_user_growth_90 END,
CASE WHEN free_mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN free_mthly_mtgs_per_user_growth_180 IS NULL AND free_mthly_mtgs_per_user_growth_90 IS NULL AND free_mthly_mtgs_per_user_growth_60 IS NULL AND free_mthly_mtgs_per_user_growth_30 IS NOT NULL THEN free_mthly_mtgs_per_user_growth_30
     WHEN free_mthly_mtgs_per_user_growth_180 IS NULL AND free_mthly_mtgs_per_user_growth_90 IS NULL AND free_mthly_mtgs_per_user_growth_60 IS NOT NULL THEN free_mthly_mtgs_per_user_growth_60
     WHEN free_mthly_mtgs_per_user_growth_180 IS NULL AND free_mthly_mtgs_per_user_growth_90 IS NOT NULL THEN free_mthly_mtgs_per_user_growth_90 
     ELSE free_mthly_mtgs_per_user_growth_180 END,



CASE WHEN free_user_count_growth_30 IS NULL THEN 0.00 ELSE free_user_count_growth_30 END,
CASE WHEN free_user_count_growth_60 IS NULL AND free_user_count_growth_30 IS NULL THEN 0.00 
     WHEN free_user_count_growth_60 IS NULL AND free_user_count_growth_30 IS NOT NULL THEN free_user_count_growth_30 
     ELSE free_user_count_growth_60 END,
CASE WHEN free_user_count_growth_30 IS NULL THEN 0.00 
     WHEN free_user_count_growth_90 IS NULL AND free_user_count_growth_60 IS NULL AND free_user_count_growth_30 IS NOT NULL THEN free_user_count_growth_30
     WHEN free_user_count_growth_90 IS NULL AND free_user_count_growth_60 IS NOT NULL THEN free_user_count_growth_60 
     ELSE free_user_count_growth_90 END,
CASE WHEN free_user_count_growth_30 IS NULL THEN 0.00 
     WHEN free_user_count_growth_180 IS NULL AND free_user_count_growth_90 IS NULL AND free_user_count_growth_60 IS NULL AND free_user_count_growth_30 IS NOT NULL THEN free_user_count_growth_30
     WHEN free_user_count_growth_180 IS NULL AND free_user_count_growth_90 IS NULL AND free_user_count_growth_60 IS NOT NULL THEN free_user_count_growth_60
     WHEN free_user_count_growth_180 IS NULL AND free_user_count_growth_90 IS NOT NULL THEN free_user_count_growth_90 
     ELSE free_user_count_growth_180 END,



CASE WHEN free_user_mau_growth_30 IS NULL THEN 0.00 ELSE free_user_mau_growth_30 END,
CASE WHEN free_user_mau_growth_60 IS NULL AND free_user_mau_growth_30 IS NULL THEN 0.00 
     WHEN free_user_mau_growth_60 IS NULL AND free_user_mau_growth_30 IS NOT NULL THEN free_user_mau_growth_30 
     ELSE free_user_mau_growth_60 END,
CASE WHEN free_user_mau_growth_30 IS NULL THEN 0.00 
     WHEN free_user_mau_growth_90 IS NULL AND free_user_mau_growth_60 IS NULL AND free_user_mau_growth_30 IS NOT NULL THEN free_user_mau_growth_30
     WHEN free_user_mau_growth_90 IS NULL AND free_user_mau_growth_60 IS NOT NULL THEN free_user_mau_growth_60 
     ELSE free_user_mau_growth_90 END,
CASE WHEN free_user_mau_growth_30 IS NULL THEN 0.00 
     WHEN free_user_mau_growth_180 IS NULL AND free_user_mau_growth_90 IS NULL AND free_user_mau_growth_60 IS NULL AND free_user_mau_growth_30 IS NOT NULL THEN free_user_mau_growth_30
     WHEN free_user_mau_growth_180 IS NULL AND free_user_mau_growth_90 IS NULL AND free_user_mau_growth_60 IS NOT NULL THEN free_user_mau_growth_60
     WHEN free_user_mau_growth_180 IS NULL AND free_user_mau_growth_90 IS NOT NULL THEN free_user_mau_growth_90 
     ELSE free_user_mau_growth_180 END,



CASE WHEN free_user_util_growth_30 IS NULL THEN 0.00 ELSE free_user_util_growth_30 END,
CASE WHEN free_user_util_growth_60 IS NULL AND free_user_util_growth_30 IS NULL THEN 0.00 
     WHEN free_user_util_growth_60 IS NULL AND free_user_util_growth_30 IS NOT NULL THEN free_user_util_growth_30 
     ELSE free_user_util_growth_60 END,
CASE WHEN free_user_util_growth_30 IS NULL THEN 0.00 
     WHEN free_user_util_growth_90 IS NULL AND free_user_util_growth_60 IS NULL AND free_user_util_growth_30 IS NOT NULL THEN free_user_util_growth_30
     WHEN free_user_util_growth_90 IS NULL AND free_user_util_growth_60 IS NOT NULL THEN free_user_util_growth_60 
     ELSE free_user_util_growth_90 END,
CASE WHEN free_user_util_growth_30 IS NULL THEN 0.00 
     WHEN free_user_util_growth_180 IS NULL AND free_user_util_growth_90 IS NULL AND free_user_util_growth_60 IS NULL AND free_user_util_growth_30 IS NOT NULL THEN free_user_util_growth_30
     WHEN free_user_util_growth_180 IS NULL AND free_user_util_growth_90 IS NULL AND free_user_util_growth_60 IS NOT NULL THEN free_user_util_growth_60
     WHEN free_user_util_growth_180 IS NULL AND free_user_util_growth_90 IS NOT NULL THEN free_user_util_growth_90 
     ELSE free_user_util_growth_180 END,



CASE WHEN mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 ELSE mthly_min_per_mtg_growth_30 END,
CASE WHEN mthly_min_per_mtg_growth_60 IS NULL AND mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN mthly_min_per_mtg_growth_60 IS NULL AND mthly_min_per_mtg_growth_30 IS NOT NULL THEN mthly_min_per_mtg_growth_30 
     ELSE mthly_min_per_mtg_growth_60 END,
CASE WHEN mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN mthly_min_per_mtg_growth_90 IS NULL AND mthly_min_per_mtg_growth_60 IS NULL AND mthly_min_per_mtg_growth_30 IS NOT NULL THEN mthly_min_per_mtg_growth_30
     WHEN mthly_min_per_mtg_growth_90 IS NULL AND mthly_min_per_mtg_growth_60 IS NOT NULL THEN mthly_min_per_mtg_growth_60 
     ELSE mthly_min_per_mtg_growth_90 END,
CASE WHEN mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN mthly_min_per_mtg_growth_180 IS NULL AND mthly_min_per_mtg_growth_90 IS NULL AND mthly_min_per_mtg_growth_60 IS NULL AND mthly_min_per_mtg_growth_30 IS NOT NULL THEN mthly_min_per_mtg_growth_30
     WHEN mthly_min_per_mtg_growth_180 IS NULL AND mthly_min_per_mtg_growth_90 IS NULL AND mthly_min_per_mtg_growth_60 IS NOT NULL THEN mthly_min_per_mtg_growth_60
     WHEN mthly_min_per_mtg_growth_180 IS NULL AND mthly_min_per_mtg_growth_90 IS NOT NULL THEN mthly_min_per_mtg_growth_90 
     ELSE mthly_min_per_mtg_growth_180 END,



CASE WHEN mthly_min_per_user_growth_30 IS NULL THEN 0.00 ELSE mthly_min_per_user_growth_30 END,
CASE WHEN mthly_min_per_user_growth_60 IS NULL AND mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN mthly_min_per_user_growth_60 IS NULL AND mthly_min_per_user_growth_30 IS NOT NULL THEN mthly_min_per_user_growth_30 
     ELSE mthly_min_per_user_growth_60 END,
CASE WHEN mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN mthly_min_per_user_growth_90 IS NULL AND mthly_min_per_user_growth_60 IS NULL AND mthly_min_per_user_growth_30 IS NOT NULL THEN mthly_min_per_user_growth_30
     WHEN mthly_min_per_user_growth_90 IS NULL AND mthly_min_per_user_growth_60 IS NOT NULL THEN mthly_min_per_user_growth_60 
     ELSE mthly_min_per_user_growth_90 END,
CASE WHEN mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN mthly_min_per_user_growth_180 IS NULL AND mthly_min_per_user_growth_90 IS NULL AND mthly_min_per_user_growth_60 IS NULL AND mthly_min_per_user_growth_30 IS NOT NULL THEN mthly_min_per_user_growth_30
     WHEN mthly_min_per_user_growth_180 IS NULL AND mthly_min_per_user_growth_90 IS NULL AND mthly_min_per_user_growth_60 IS NOT NULL THEN mthly_min_per_user_growth_60
     WHEN mthly_min_per_user_growth_180 IS NULL AND mthly_min_per_user_growth_90 IS NOT NULL THEN mthly_min_per_user_growth_90 
     ELSE mthly_min_per_user_growth_180 END,



CASE WHEN mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 ELSE mthly_mtgs_per_user_growth_30 END,
CASE WHEN mthly_mtgs_per_user_growth_60 IS NULL AND mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN mthly_mtgs_per_user_growth_60 IS NULL AND mthly_mtgs_per_user_growth_30 IS NOT NULL THEN mthly_mtgs_per_user_growth_30 
     ELSE mthly_mtgs_per_user_growth_60 END,
CASE WHEN mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN mthly_mtgs_per_user_growth_90 IS NULL AND mthly_mtgs_per_user_growth_60 IS NULL AND mthly_mtgs_per_user_growth_30 IS NOT NULL THEN mthly_mtgs_per_user_growth_30
     WHEN mthly_mtgs_per_user_growth_90 IS NULL AND mthly_mtgs_per_user_growth_60 IS NOT NULL THEN mthly_mtgs_per_user_growth_60 
     ELSE mthly_mtgs_per_user_growth_90 END,
CASE WHEN mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN mthly_mtgs_per_user_growth_180 IS NULL AND mthly_mtgs_per_user_growth_90 IS NULL AND mthly_mtgs_per_user_growth_60 IS NULL AND mthly_mtgs_per_user_growth_30 IS NOT NULL THEN mthly_mtgs_per_user_growth_30
     WHEN mthly_mtgs_per_user_growth_180 IS NULL AND mthly_mtgs_per_user_growth_90 IS NULL AND mthly_mtgs_per_user_growth_60 IS NOT NULL THEN mthly_mtgs_per_user_growth_60
     WHEN mthly_mtgs_per_user_growth_180 IS NULL AND mthly_mtgs_per_user_growth_90 IS NOT NULL THEN mthly_mtgs_per_user_growth_90 
     ELSE mthly_mtgs_per_user_growth_180 END,



CASE WHEN paid_mthly_min_growth_30 IS NULL THEN 0.00 ELSE paid_mthly_min_growth_30 END,
CASE WHEN paid_mthly_min_growth_60 IS NULL AND paid_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_growth_60 IS NULL AND paid_mthly_min_growth_30 IS NOT NULL THEN paid_mthly_min_growth_30 
     ELSE paid_mthly_min_growth_60 END,
CASE WHEN paid_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_growth_90 IS NULL AND paid_mthly_min_growth_60 IS NULL AND paid_mthly_min_growth_30 IS NOT NULL THEN paid_mthly_min_growth_30
     WHEN paid_mthly_min_growth_90 IS NULL AND paid_mthly_min_growth_60 IS NOT NULL THEN paid_mthly_min_growth_60 
     ELSE paid_mthly_min_growth_90 END,
CASE WHEN paid_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_growth_180 IS NULL AND paid_mthly_min_growth_90 IS NULL AND paid_mthly_min_growth_60 IS NULL AND paid_mthly_min_growth_30 IS NOT NULL THEN paid_mthly_min_growth_30
     WHEN paid_mthly_min_growth_180 IS NULL AND paid_mthly_min_growth_90 IS NULL AND paid_mthly_min_growth_60 IS NOT NULL THEN paid_mthly_min_growth_60
     WHEN paid_mthly_min_growth_180 IS NULL AND paid_mthly_min_growth_90 IS NOT NULL THEN paid_mthly_min_growth_90 
     ELSE paid_mthly_min_growth_180 END,



CASE WHEN paid_mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 ELSE paid_mthly_min_per_mtg_growth_30 END,
CASE WHEN paid_mthly_min_per_mtg_growth_60 IS NULL AND paid_mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_per_mtg_growth_60 IS NULL AND paid_mthly_min_per_mtg_growth_30 IS NOT NULL THEN paid_mthly_min_per_mtg_growth_30 
     ELSE paid_mthly_min_per_mtg_growth_60 END,
CASE WHEN paid_mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_per_mtg_growth_90 IS NULL AND paid_mthly_min_per_mtg_growth_60 IS NULL AND paid_mthly_min_per_mtg_growth_30 IS NOT NULL THEN paid_mthly_min_per_mtg_growth_30
     WHEN paid_mthly_min_per_mtg_growth_90 IS NULL AND paid_mthly_min_per_mtg_growth_60 IS NOT NULL THEN paid_mthly_min_per_mtg_growth_60 
     ELSE paid_mthly_min_per_mtg_growth_90 END,
CASE WHEN paid_mthly_min_per_mtg_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_per_mtg_growth_180 IS NULL AND paid_mthly_min_per_mtg_growth_90 IS NULL AND paid_mthly_min_per_mtg_growth_60 IS NULL AND paid_mthly_min_per_mtg_growth_30 IS NOT NULL THEN paid_mthly_min_per_mtg_growth_30
     WHEN paid_mthly_min_per_mtg_growth_180 IS NULL AND paid_mthly_min_per_mtg_growth_90 IS NULL AND paid_mthly_min_per_mtg_growth_60 IS NOT NULL THEN paid_mthly_min_per_mtg_growth_60
     WHEN paid_mthly_min_per_mtg_growth_180 IS NULL AND paid_mthly_min_per_mtg_growth_90 IS NOT NULL THEN paid_mthly_min_per_mtg_growth_90 
     ELSE paid_mthly_min_per_mtg_growth_180 END,



CASE WHEN paid_mthly_min_per_user_growth_30 IS NULL THEN 0.00 ELSE paid_mthly_min_per_user_growth_30 END,
CASE WHEN paid_mthly_min_per_user_growth_60 IS NULL AND paid_mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_per_user_growth_60 IS NULL AND paid_mthly_min_per_user_growth_30 IS NOT NULL THEN paid_mthly_min_per_user_growth_30 
     ELSE paid_mthly_min_per_user_growth_60 END,
CASE WHEN paid_mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_per_user_growth_90 IS NULL AND paid_mthly_min_per_user_growth_60 IS NULL AND paid_mthly_min_per_user_growth_30 IS NOT NULL THEN paid_mthly_min_per_user_growth_30
     WHEN paid_mthly_min_per_user_growth_90 IS NULL AND paid_mthly_min_per_user_growth_60 IS NOT NULL THEN paid_mthly_min_per_user_growth_60 
     ELSE paid_mthly_min_per_user_growth_90 END,
CASE WHEN paid_mthly_min_per_user_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_min_per_user_growth_180 IS NULL AND paid_mthly_min_per_user_growth_90 IS NULL AND paid_mthly_min_per_user_growth_60 IS NULL AND paid_mthly_min_per_user_growth_30 IS NOT NULL THEN paid_mthly_min_per_user_growth_30
     WHEN paid_mthly_min_per_user_growth_180 IS NULL AND paid_mthly_min_per_user_growth_90 IS NULL AND paid_mthly_min_per_user_growth_60 IS NOT NULL THEN paid_mthly_min_per_user_growth_60
     WHEN paid_mthly_min_per_user_growth_180 IS NULL AND paid_mthly_min_per_user_growth_90 IS NOT NULL THEN paid_mthly_min_per_user_growth_90 
     ELSE paid_mthly_min_per_user_growth_180 END,



CASE WHEN paid_mthly_mtgs_growth_30 IS NULL THEN 0.00 ELSE paid_mthly_mtgs_growth_30 END,
CASE WHEN paid_mthly_mtgs_growth_60 IS NULL AND paid_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_mtgs_growth_60 IS NULL AND paid_mthly_mtgs_growth_30 IS NOT NULL THEN paid_mthly_mtgs_growth_30 
     ELSE paid_mthly_mtgs_growth_60 END,
CASE WHEN paid_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_mtgs_growth_90 IS NULL AND paid_mthly_mtgs_growth_60 IS NULL AND paid_mthly_mtgs_growth_30 IS NOT NULL THEN paid_mthly_mtgs_growth_30
     WHEN paid_mthly_mtgs_growth_90 IS NULL AND paid_mthly_mtgs_growth_60 IS NOT NULL THEN paid_mthly_mtgs_growth_60 
     ELSE paid_mthly_mtgs_growth_90 END,
CASE WHEN paid_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_mtgs_growth_180 IS NULL AND paid_mthly_mtgs_growth_90 IS NULL AND paid_mthly_mtgs_growth_60 IS NULL AND paid_mthly_mtgs_growth_30 IS NOT NULL THEN paid_mthly_mtgs_growth_30
     WHEN paid_mthly_mtgs_growth_180 IS NULL AND paid_mthly_mtgs_growth_90 IS NULL AND paid_mthly_mtgs_growth_60 IS NOT NULL THEN paid_mthly_mtgs_growth_60
     WHEN paid_mthly_mtgs_growth_180 IS NULL AND paid_mthly_mtgs_growth_90 IS NOT NULL THEN paid_mthly_mtgs_growth_90 
     ELSE paid_mthly_mtgs_growth_180 END,



CASE WHEN paid_mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 ELSE paid_mthly_mtgs_per_user_growth_30 END,
CASE WHEN paid_mthly_mtgs_per_user_growth_60 IS NULL AND paid_mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_mtgs_per_user_growth_60 IS NULL AND paid_mthly_mtgs_per_user_growth_30 IS NOT NULL THEN paid_mthly_mtgs_per_user_growth_30 
     ELSE paid_mthly_mtgs_per_user_growth_60 END,
CASE WHEN paid_mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_mtgs_per_user_growth_90 IS NULL AND paid_mthly_mtgs_per_user_growth_60 IS NULL AND paid_mthly_mtgs_per_user_growth_30 IS NOT NULL THEN paid_mthly_mtgs_per_user_growth_30
     WHEN paid_mthly_mtgs_per_user_growth_90 IS NULL AND paid_mthly_mtgs_per_user_growth_60 IS NOT NULL THEN paid_mthly_mtgs_per_user_growth_60 
     ELSE paid_mthly_mtgs_per_user_growth_90 END,
CASE WHEN paid_mthly_mtgs_per_user_growth_30 IS NULL THEN 0.00 
     WHEN paid_mthly_mtgs_per_user_growth_180 IS NULL AND paid_mthly_mtgs_per_user_growth_90 IS NULL AND paid_mthly_mtgs_per_user_growth_60 IS NULL AND paid_mthly_mtgs_per_user_growth_30 IS NOT NULL THEN paid_mthly_mtgs_per_user_growth_30
     WHEN paid_mthly_mtgs_per_user_growth_180 IS NULL AND paid_mthly_mtgs_per_user_growth_90 IS NULL AND paid_mthly_mtgs_per_user_growth_60 IS NOT NULL THEN paid_mthly_mtgs_per_user_growth_60
     WHEN paid_mthly_mtgs_per_user_growth_180 IS NULL AND paid_mthly_mtgs_per_user_growth_90 IS NOT NULL THEN paid_mthly_mtgs_per_user_growth_90 
     ELSE paid_mthly_mtgs_per_user_growth_180 END,



CASE WHEN paid_user_count_growth_30 IS NULL THEN 0.00 ELSE paid_user_count_growth_30 END,
CASE WHEN paid_user_count_growth_60 IS NULL AND paid_user_count_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_count_growth_60 IS NULL AND paid_user_count_growth_30 IS NOT NULL THEN paid_user_count_growth_30 
     ELSE paid_user_count_growth_60 END,
CASE WHEN paid_user_count_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_count_growth_90 IS NULL AND paid_user_count_growth_60 IS NULL AND paid_user_count_growth_30 IS NOT NULL THEN paid_user_count_growth_30
     WHEN paid_user_count_growth_90 IS NULL AND paid_user_count_growth_60 IS NOT NULL THEN paid_user_count_growth_60 
     ELSE paid_user_count_growth_90 END,
CASE WHEN paid_user_count_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_count_growth_180 IS NULL AND paid_user_count_growth_90 IS NULL AND paid_user_count_growth_60 IS NULL AND paid_user_count_growth_30 IS NOT NULL THEN paid_user_count_growth_30
     WHEN paid_user_count_growth_180 IS NULL AND paid_user_count_growth_90 IS NULL AND paid_user_count_growth_60 IS NOT NULL THEN paid_user_count_growth_60
     WHEN paid_user_count_growth_180 IS NULL AND paid_user_count_growth_90 IS NOT NULL THEN paid_user_count_growth_90 
     ELSE paid_user_count_growth_180 END,



CASE WHEN paid_user_mau_growth_30 IS NULL THEN 0.00 ELSE paid_user_mau_growth_30 END,
CASE WHEN paid_user_mau_growth_60 IS NULL AND paid_user_mau_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_mau_growth_60 IS NULL AND paid_user_mau_growth_30 IS NOT NULL THEN paid_user_mau_growth_30 
     ELSE paid_user_mau_growth_60 END,
CASE WHEN paid_user_mau_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_mau_growth_90 IS NULL AND paid_user_mau_growth_60 IS NULL AND paid_user_mau_growth_30 IS NOT NULL THEN paid_user_mau_growth_30
     WHEN paid_user_mau_growth_90 IS NULL AND paid_user_mau_growth_60 IS NOT NULL THEN paid_user_mau_growth_60 
     ELSE paid_user_mau_growth_90 END,
CASE WHEN paid_user_mau_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_mau_growth_180 IS NULL AND paid_user_mau_growth_90 IS NULL AND paid_user_mau_growth_60 IS NULL AND paid_user_mau_growth_30 IS NOT NULL THEN paid_user_mau_growth_30
     WHEN paid_user_mau_growth_180 IS NULL AND paid_user_mau_growth_90 IS NULL AND paid_user_mau_growth_60 IS NOT NULL THEN paid_user_mau_growth_60
     WHEN paid_user_mau_growth_180 IS NULL AND paid_user_mau_growth_90 IS NOT NULL THEN paid_user_mau_growth_90 
     ELSE paid_user_mau_growth_180 END,



CASE WHEN paid_user_util_growth_30 IS NULL THEN 0.00 ELSE paid_user_util_growth_30 END,
CASE WHEN paid_user_util_growth_60 IS NULL AND paid_user_util_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_util_growth_60 IS NULL AND paid_user_util_growth_30 IS NOT NULL THEN paid_user_util_growth_30 
     ELSE paid_user_util_growth_60 END,
CASE WHEN paid_user_util_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_util_growth_90 IS NULL AND paid_user_util_growth_60 IS NULL AND paid_user_util_growth_30 IS NOT NULL THEN paid_user_util_growth_30
     WHEN paid_user_util_growth_90 IS NULL AND paid_user_util_growth_60 IS NOT NULL THEN paid_user_util_growth_60 
     ELSE paid_user_util_growth_90 END,
CASE WHEN paid_user_util_growth_30 IS NULL THEN 0.00 
     WHEN paid_user_util_growth_180 IS NULL AND paid_user_util_growth_90 IS NULL AND paid_user_util_growth_60 IS NULL AND paid_user_util_growth_30 IS NOT NULL THEN paid_user_util_growth_30
     WHEN paid_user_util_growth_180 IS NULL AND paid_user_util_growth_90 IS NULL AND paid_user_util_growth_60 IS NOT NULL THEN paid_user_util_growth_60
     WHEN paid_user_util_growth_180 IS NULL AND paid_user_util_growth_90 IS NOT NULL THEN paid_user_util_growth_90 
     ELSE paid_user_util_growth_180 END,



CASE WHEN total_mthly_min_growth_30 IS NULL THEN 0.00 ELSE total_mthly_min_growth_30 END,
CASE WHEN total_mthly_min_growth_60 IS NULL AND total_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN total_mthly_min_growth_60 IS NULL AND total_mthly_min_growth_30 IS NOT NULL THEN total_mthly_min_growth_30 
     ELSE total_mthly_min_growth_60 END,
CASE WHEN total_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN total_mthly_min_growth_90 IS NULL AND total_mthly_min_growth_60 IS NULL AND total_mthly_min_growth_30 IS NOT NULL THEN total_mthly_min_growth_30
     WHEN total_mthly_min_growth_90 IS NULL AND total_mthly_min_growth_60 IS NOT NULL THEN total_mthly_min_growth_60 
     ELSE total_mthly_min_growth_90 END,
CASE WHEN total_mthly_min_growth_30 IS NULL THEN 0.00 
     WHEN total_mthly_min_growth_180 IS NULL AND total_mthly_min_growth_90 IS NULL AND total_mthly_min_growth_60 IS NULL AND total_mthly_min_growth_30 IS NOT NULL THEN total_mthly_min_growth_30
     WHEN total_mthly_min_growth_180 IS NULL AND total_mthly_min_growth_90 IS NULL AND total_mthly_min_growth_60 IS NOT NULL THEN total_mthly_min_growth_60
     WHEN total_mthly_min_growth_180 IS NULL AND total_mthly_min_growth_90 IS NOT NULL THEN total_mthly_min_growth_90 
     ELSE total_mthly_min_growth_180 END,



CASE WHEN total_mthly_mtgs_growth_30 IS NULL THEN 0.00 ELSE total_mthly_mtgs_growth_30 END,
CASE WHEN total_mthly_mtgs_growth_60 IS NULL AND total_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN total_mthly_mtgs_growth_60 IS NULL AND total_mthly_mtgs_growth_30 IS NOT NULL THEN total_mthly_mtgs_growth_30 
     ELSE total_mthly_mtgs_growth_60 END,
CASE WHEN total_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN total_mthly_mtgs_growth_90 IS NULL AND total_mthly_mtgs_growth_60 IS NULL AND total_mthly_mtgs_growth_30 IS NOT NULL THEN total_mthly_mtgs_growth_30
     WHEN total_mthly_mtgs_growth_90 IS NULL AND total_mthly_mtgs_growth_60 IS NOT NULL THEN total_mthly_mtgs_growth_60 
     ELSE total_mthly_mtgs_growth_90 END,
CASE WHEN total_mthly_mtgs_growth_30 IS NULL THEN 0.00 
     WHEN total_mthly_mtgs_growth_180 IS NULL AND total_mthly_mtgs_growth_90 IS NULL AND total_mthly_mtgs_growth_60 IS NULL AND total_mthly_mtgs_growth_30 IS NOT NULL THEN total_mthly_mtgs_growth_30
     WHEN total_mthly_mtgs_growth_180 IS NULL AND total_mthly_mtgs_growth_90 IS NULL AND total_mthly_mtgs_growth_60 IS NOT NULL THEN total_mthly_mtgs_growth_60
     WHEN total_mthly_mtgs_growth_180 IS NULL AND total_mthly_mtgs_growth_90 IS NOT NULL THEN total_mthly_mtgs_growth_90 
     ELSE total_mthly_mtgs_growth_180 END,
----------------------------------------------

CASE WHEN opp_amount IS NULL THEN 0 ELSE opp_amount END,
----when a subscription is cancelled in zuora, there will not be can active subscription, 
----so the account_health_score dataset will show a null value for days_left_in_term, therefore, this is really a 0
CASE WHEN days_left_in_term IS NULL THEN 0 ELSE days_left_in_term END,
CASE WHEN churn_downsell IS NULL THEN 0 ELSE churn_downsell END,
CASE WHEN churn_cancel IS NULL THEN 0 ELSE churn_cancel END,
CASE WHEN churn_gross IS NULL THEN 0 ELSE churn_gross END,
CASE WHEN churn_downsell_next_90 IS NULL THEN 0 ELSE churn_downsell_next_90 END,
CASE WHEN churn_cancel_next_90 IS NULL THEN 0 ELSE churn_cancel_next_90 END,
CASE WHEN churn_gross_next_90 IS NULL THEN 0 ELSE churn_gross_next_90 END


FROM etl_acct_health.ahs_2_usage_growth_adBETWEEN 
WHERE ahs_date >= '2016-10-13' AND ahs_date >= (CURRENT_DATE - 92)
----the is null clauses exluded some channel items, and some data issues dates/accounts 
----there is no account utilization data when the mrs first came in for most accounts, since they are not set up
----AND (DATEPART(dw, ahs_date) <> 0 AND DATEPART(dw, ahs_date) <> 6)
AND zoom_account_no IS NOT NULL  
AND account_name IS NOT NULL
AND billing_hosts IS NOT NULL
AND coreproduct IS NOT NULL
AND currentterm IS NOT NULL

----if mrr_exit is null, the account was canceled before 8/1 (the date we started calculating churn by request date see table mrr_4_agg_waterfall_ad), we dropped them
AND mrr_exit IS NOT NULL
ORDER BY zoom_account_no, ahs_date;



