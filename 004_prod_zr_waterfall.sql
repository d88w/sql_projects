

------------------------------------------------------------
------------------------------------------------------------
------------- Zoom Room Waterfall Table --------------------
------------------------------------------------------------
------------------------------------------------------------

delete from rpt.zr_waterfall
--------DATE LIMIT-----
where load_date >= (CURRENT_DATE -30)
;

INSERT INTO rpt.zr_waterfall

WITH TA AS
(
WITH T1 AS
(
SELECT load_date, acc_number, acc_name, pd_name, 
SUM(rpc_quantity_entry) AS quantity_entry,
SUM(rpc_quantity_change) AS quantity_change, 
SUM(rpc_quantity) AS quantity_exit,
SUM(mrr_entry) AS mrr_entry,
SUM(mrr_new) AS mrr_new,
SUM(mrr_upsell) AS mrr_upsell,
SUM(mrr_downsell) AS mrr_downsell,
SUM(mrr_cancel) AS mrr_cancel,
SUM(mrr_change) AS mrr_change,
SUM(mrr_exit) AS mrr_exit
FROM etl_acct_health.mrr_2_rpc_waterfall_apd 
WHERE pd_name = 'Zoom Rooms'
AND rpc_quantity_change < 100000
--------DATE LIMIT-----
and load_date >= (CURRENT_DATE -30)
GROUP BY load_date, acc_number, acc_name, pd_name),

T2 AS 
(SELECT 
	mrs_date,
	account_number,
	fiscal_quarter, 
	yearmonth, 
	zoom_account_no,
	account_owner, 
	salesdivision,
	billtocountry, 
	billtostate, 
	--------------ADDED employee_count
	employee_count,
	sales_group_exit,
	accountcreateddate,
	CASE WHEN (sales_group_exit= 'Int') AND (employee_count= 'Personal') THEN 'SB'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '1-50') THEN 'SB'
    WHEN (sales_group_exit= 'Int') AND (employee_count= 'Just Me') THEN 'SB'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '11-50') THEN 'SB'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '2-10') THEN 'SB'
    WHEN (sales_group_exit= 'Int') AND (employee_count= 'Nov-50') THEN 'SB'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '251-1000') THEN 'MM'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '251-500') THEN 'MM'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '51-250') THEN 'MM'
    WHEN (sales_group_exit= 'Int') AND (employee_count= 'Select One') THEN 'Majors'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '2501+') THEN 'Majors'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '1001-2500') THEN 'Commercial'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '') THEN 'Majors'
    WHEN (sales_group_exit= 'Int') AND (employee_count IS NULL) THEN 'Majors'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '5001-10000') THEN 'Majors'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '10001+') THEN 'INTL'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '501-1000') THEN 'Commercial'
    WHEN (sales_group_exit= 'Int') AND (employee_count= '1001-5000') THEN 'Majors'
    ELSE sales_group_exit END AS sales_group_exit_no_int,
	through_reseller, --- (true/false)
	is_reseller, ---（Yes/No)
	parent_account_name,
	parent_account_number,
	ultimate_parent_account_name,
	ultimate_parent_account_number,
	COUNT(zoom_account_no)
 FROM dm_sales.mrs_by_ad_growth_single
 --------DATE LIMIT-----
 where mrs_date >= (CURRENT_DATE -30)
 GROUP BY
 	mrs_date,
 	account_number,
 	fiscal_quarter, 
	yearmonth, 
	zoom_account_no,
	zoom_account_no,
	account_owner, 
	salesdivision,
	billtocountry, 
	billtostate,
	--------------ADDED employee_count
	employee_count,
	through_reseller, --- (true/false)
	is_reseller, ---（Yes/No)
	parent_account_name,
	parent_account_number,
	ultimate_parent_account_name,
	ultimate_parent_account_number,
	sales_group_exit,
	sales_group_exit_no_int,
	accountcreateddate)

SELECT T1.load_date, 
to_char(T1.load_date, 'yyyymm') AS yearmonth, 
to_char(DATEADD(month, -1, T1.load_date), 'yyyyq') AS fiscal_quarter,
T2.zoom_account_no, 
T1.acc_number, 
T1.acc_name, 
T2.account_owner, 
T2.salesdivision,
T2.billtocountry, 
T2.billtostate, 
--------------ADDED employee_count
T2.employee_count,
T2.accountcreateddate,
T2.sales_group_exit,
T2.sales_group_exit_no_int,
CASE WHEN T2.through_reseller = 'true' THEN 'indirect'
	 WHEN T2.is_reseller = 'Yes' THEN 'indirect'
	 ELSE 'direct' END AS channel,
T2.parent_account_name,
T2.parent_account_number,
T2.ultimate_parent_account_name,
T2.ultimate_parent_account_number,
T1.pd_name, 
T1.quantity_entry,
T1.quantity_change, 
T1.quantity_exit,
T1.mrr_entry,
T1.mrr_new,
T1.mrr_upsell,
T1.mrr_downsell,
T1.mrr_cancel,
T1.mrr_change,
T1.mrr_exit
FROM  T1
LEFT OUTER JOIN T2 ON T1.load_date = DATEADD(day, -1, T2.mrs_date) AND T1.acc_number = T2.account_number
),

TB AS
(SELECT b.accountnumber, 
a.zoom_account_number__c, 0 AS zoom_account_no, 
a.name AS account_name, 
a.industry, 
a.billingcountry, 
a.billingstate,
--------------ADDED employee_count
a.employee_count__c AS employee_count,
CASE WHEN a.ispartner = 'true' THEN 'indirect'
	 WHEN a.channel_account_owner__c IS NOT NULL THEN 'indirect'
	 WHEN a.channel_account_owner__c <> '' THEN 'indirect'
	 WHEN b.isreseller__c = 'Yes' THEN 'indirect'
	 ELSE 'direct' END AS channel,
a.segment__c,
a.ls_company_industry__c,
a.ls_company_sub_industry__c
FROM src_sfdc.account a, src_zuora.account b
WHERE a.zoom_account_number__c = CAST(b.zoom_account_number__c AS VARCHAR)
AND a.type <> 'Free Trial')

SELECT 
TA.load_date, 
to_char(load_date, 'yyyymm') AS yearmonth, 
to_char(DATEADD(month, -1, load_date), 'yyyyq') AS fiscal_quarter,
CASE WHEN TA.zoom_account_no IS NULL THEN CAST(TRUNC(TB.zoom_account_no, 1) AS VARCHAR) ELSE TA.zoom_account_no END AS zoom_account_no,
--TA.zoom_account_no,
TA.acc_number, 
CASE WHEN TA.acc_name IS NULL 
	THEN TB.account_name ELSE TA.acc_name END, 
TA.account_owner, 
TA.salesdivision,
TB.ls_company_industry__c AS ls_industry,
TB.ls_company_sub_industry__c AS ls_sub_industry,	
CASE WHEN TA.billtocountry IS NULL 
	THEN TB.billingcountry ELSE TA.billtocountry END,  
CASE WHEN TA.billtostate IS NULL 
	THEN TB.billingstate ELSE TA.billtostate END,
CASE WHEN TA.sales_group_exit IS NULL THEN TB.segment__c 
	 WHEN TA.sales_group_exit = 'Channel' THEN TB.segment__c  
	 WHEN TA.sales_group_exit = '14.99' AND TA.quantity_change <> 0 THEN TB.segment__c
	 ELSE TA.sales_group_exit END, 
CASE WHEN TA.sales_group_exit_no_int IS NULL THEN TB.segment__c 
	 WHEN TA.sales_group_exit_no_int = 'Channel' THEN TB.segment__c  
	 WHEN TA.sales_group_exit_no_int = '14.99' THEN TB.segment__c 
	 WHEN TA.sales_group_exit_no_int = 'Int' AND TB.segment__c  = 'Int' THEN 'MM'
	 WHEN TA.sales_group_exit_no_int = 'Intl' AND TB.segment__c  = 'Intl' THEN 'MM'
	 WHEN TA.sales_group_exit_no_int = 'INTL' AND TB.segment__c  = 'INTL' THEN 'MM'
	 WHEN TA.sales_group_exit_no_int = 'Int' THEN TB.segment__c   
	 WHEN TA.sales_group_exit_no_int = 'Intl' THEN TB.segment__c  
	 WHEN TA.sales_group_exit_no_int = 'INTL' THEN TB.segment__c   
	 ELSE TA.sales_group_exit_no_int END,
CASE WHEN TA.channel IS NULL 
	THEN TB.channel ELSE TA.channel END,
TA.parent_account_name,
TA.parent_account_number,
TA.ultimate_parent_account_name,
TA.ultimate_parent_account_number,
TA.pd_name, 
TA.quantity_entry,
TA.quantity_change, 
TA.quantity_exit,
TA.mrr_entry,
TA.mrr_new,
TA.mrr_upsell,
TA.mrr_downsell,
TA.mrr_cancel,
TA.mrr_change,
TA.mrr_exit,
--------------ADDED employee_count
CASE WHEN TA.employee_count IS NULL 
	THEN TB.employee_count ELSE TA.employee_count END,
TA.accountcreateddate::DATE
FROM TA
LEFT OUTER JOIN TB ON TB.accountnumber = TA.acc_number;

grant select on all tables in schema rpt to zm_domo_rpt_read_only;
commit
