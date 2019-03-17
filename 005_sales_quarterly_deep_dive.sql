---------------------------------------------------------
---------------------------------------------------------
------------------ MRR growth cuts -------------------
---------------------------------------------------------
---------------------------------------------------------

SELECT *
FROM dm_sales_airflow.v_zr_mrs_by_qtr_growth_combined;

SELECT b.fy_quarter,
       CASE
         WHEN a.acct_channel IN ('Indirect','ISV') THEN 'Channel & ISV'
         ELSE a.acct_channel
       END 
,
       SUM(mrs_new + mrs_upsell + mrs_backlog_net_new + mrs_downsell + mrs_cancel) AS mrr_net,
       SUM(CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_adj
FROM dm_sales_airflow.v_zr_mrs_by_ad_growth_combined a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.dt BETWEEN b.qtr_start
        AND b.qtr_end
GROUP BY 1,
         2
ORDER BY 2,
         1;
         


SELECT b.fy_quarter,
       CASE
         WHEN billtocountry IN ('United States','Canada') THEN 'US & CAN'
         ELSE 'INTL'
       END 
,
       SUM(mrs_new + mrs_upsell + mrs_backlog_net_new + mrs_downsell + mrs_cancel) AS mrr_net,
       SUM(CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_adj
FROM dm_sales_airflow.v_zr_mrs_by_ad_growth_combined a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.dt BETWEEN b.qtr_start
        AND b.qtr_end
GROUP BY 1,
         2
ORDER BY 1,
         2;



SELECT b.fy_quarter,
       CASE
         WHEN billtocountry IN ('United States','Canada') THEN 'US & CAN'
         ELSE 'INTL'
       END 
,
       CASE
         WHEN a.acct_channel IN ('Indirect','ISV') THEN 'Channel & ISV'
         ELSE a.acct_channel
       END 
,
       SUM(mrs_new + mrs_upsell + mrs_backlog_net_new + mrs_downsell + mrs_cancel) AS mrr_net,
       SUM(CASE WHEN adjustment IS NULL THEN 0 ELSE adjustment END) AS mrr_adj
FROM dm_sales_airflow.v_zr_mrs_by_ad_growth_combined a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.dt BETWEEN b.qtr_start
        AND b.qtr_end
GROUP BY 1,
         2,3
ORDER BY 3,
         2,1;



---------------------------------------------------------
---------------------------------------------------------
------------------ Bookings growth cuts -----------------
---------------------------------------------------------
---------------------------------------------------------



---- 14.99 for all Quarters
SELECT fy_quarter,
       CASE
         WHEN lower(Billing_Country__c) NOT IN ('united states','canada') THEN 'Online - 14.99 INTL'
         ELSE 'Online - 14.99 US'
       END AS division,
       SUM(Amount__c) AS bookings
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
WHERE id IS NOT NULL
AND   id <> ''
AND   Order_Type__c IN ('New','New Order')
AND   Amount__c < 17
--- exclusive of Direct, DOM SMB Online & INTL SMB Online & VAST ONLINE & QTD Online
AND   (Coupon__c = '' OR coupon__c IS NULL)
--- Exclusive of Direct-MLM
AND   account__c <> ''
AND   account__c IS NOT NULL
AND   bookingexception__c <> 'Y'
AND   isdeleted = FALSE
GROUP BY 1


UNION ALL

--- QTD ONLINE
SELECT fy_quarter,
       'Online - QTD' AS division,
       SUM(Amount__c) AS QTD_online
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
WHERE id IS NOT NULL
AND   id <> ''
AND   QTD__c = TRUE
--- exclusive from DOM SMB Online & INTL SMB Online
--- Not exclusive from VAST ONLINE, because QTD Online takes priority if both are true
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17)
---- exclusive of 14.99
OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL))
AND   lower(owner_name) LIKE '%integration%'
--- Exclusive of Direct
AND   account__c <> ''
AND   account__c IS NOT NULL
AND   isdeleted = FALSE
GROUP BY 1

UNION ALL

----- VAST Online
SELECT fy_quarter,
       'Online - VAST' AS division,
       SUM(Amount__c) AS VAST_Online
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
WHERE id IS NOT NULL
AND   id <> ''
AND   Is_Online_VAST__c = TRUE
--- exclusive from DOM SMB Online & INTL SMB Online
AND   QTD__c = FALSE
--- exclusive from QTD Online
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17)
---- exclusive of 14.99
OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL))
AND   lower(owner_name) LIKE '%integration%'
--- Exclusive of Direct
AND   account__c <> ''
AND   account__c IS NOT NULL
AND   isdeleted = FALSE
GROUP BY 1

UNION ALL

---- DOM SMB Online - Q3 and after Q3 2018
SELECT fy_quarter,
       'Online - DOM SMB' AS division,
       SUM(amount__c) AS DOM_SMB_Online
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
WHERE id IS NOT NULL
AND   id <> ''
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17)
---- exclusive of 14.99
OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL))
AND   lower(Billing_Country__c) IN ('united states','canada')
--- Exclusive of INTL SMB ONLINE
AND   lower(Employee_Count__c) IN ('just me','2-10','11-50','51-250')
AND   lower(Segment__c) NOT IN ('edu','healthcare')
AND   Is_Online_VAST__c = FALSE
---- Exclusive of VAST Online
AND   QTD__c = FALSE
---- Exclusive of QTD Online
AND   lower(owner_name) LIKE '%integration%'
--- Exclusive of Direct
AND   account__c <> ''
AND   account__c IS NOT NULL
AND   isdeleted = FALSE
GROUP BY 1

UNION ALL

---- INTL SMB Online - Q3 and after Q3 2018
SELECT fy_quarter,
       'Online - INTL SMB' AS division,
       SUM(amount__c) AS DOM_SMB_Online
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
WHERE id IS NOT NULL
AND   id <> ''
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17)
---- exclusive of 14.99
OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL))
AND   lower(Billing_Country__c) NOT IN ('united states','canada')
--- Exclusive of DOM SMB ONLINE
AND   lower(Employee_Count__c) IN ('just me','2-10','11-50','51-250')
AND   lower(Segment__c) NOT IN ('edu','healthcare')
AND   Is_Online_VAST__c = FALSE
---- Exclusive of VAST Online
AND   QTD__c = FALSE
---- Exclusive of QTD Online
AND   lower(owner_name) LIKE '%integration%'
--- Exclusive of Direct
AND   account__c <> ''
AND   account__c IS NOT NULL
AND   isdeleted = FALSE
GROUP BY 1

UNION ALL

--- Other ONLINE- Q3 and after Q3 2018
SELECT fy_quarter,
       'Online - Other' AS division,
       SUM(Amount__c) AS other_online
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
WHERE 1 = 1
AND   (id IS NOT NULL AND id <> '')
AND   QTD__c = FALSE
---- Exclusive of QTD Online
AND   Is_Online_VAST__c = FALSE
---- Exclusive of VAST Online
AND   (lower(Employee_Count__c) NOT IN ('just me','2-10','11-50','51-250') OR lower(Segment__c) IN ('edu','healthcare'))
---- Exclusive of DOM SMB ONLINE & INTL SMB ONLINE
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17)
---- exclusive of 14.99
OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL) OR (bookingexception__c = 'Y')
--- picking up 14.99 slack
)
AND   lower(owner_name) LIKE '%integration%'
--- exclusive of Direct 
AND   account__c <> ''
AND   account__c IS NOT NULL
AND   isdeleted = FALSE
GROUP BY 1

UNION ALL

---- DIRECT Bookings
SELECT fy_quarter,
       CASE
         WHEN Owner_Division__c = 'ISV' THEN 'Direct - ISV'
         WHEN lower(Owner_Division__c) LIKE '%intl%' THEN 'Direct - INTL Teams'
         ELSE 'Direct - US Teams'
       END AS division,

       SUM(Amount__c) AS direct
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end

WHERE 1 = 1
AND   ((Order_Type__c IN ('New','New Order') AND Amount__c >= 17)
---- exclusive of 14.99
OR (Order_Type__c = 'Upsell' AND Amount__c >= 0) OR (Order_Type__c IN ('New','New Order') AND Amount__c < 17 AND Coupon__c <> '' AND coupon__c IS NOT NULL)
--- picking up 14.99 slack
OR (bookingexception__c = 'Y'))
AND   lower(owner_name) NOT LIKE '%integration%'
AND   isdeleted = FALSE
AND   account__c <> ''
AND   account__c IS NOT NULL
GROUP BY 1,
         2
ORDER BY 2,1;


--- TOTAL
SELECT fy_quarter,
       'Total' AS division,
       SUM(Amount__c) AS total
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
WHERE 1 = 1
AND   Order_Type__c IN ('New','New Order','Upsell')
AND   isdeleted = FALSE
AND   account__c <> ''
AND   account__c IS NOT NULL
GROUP BY 1;



---------------------------------------------------------
---------------------------------------------------------
------------------ Lead growth cuts -----------------
---------------------------------------------------------
---------------------------------------------------------

--- inbound
SELECT fy_quarter,
       COUNT(*)
FROM src_sfdc.lead a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.createddate::DATE BETWEEN b.qtr_start
        AND b.qtr_end
WHERE   isdeleted = FALSE
and leadsource in
('AVer 60 Day Pro Free Trial','Appointment Setting Vendor','Contact Sales','Customer Referrals','Employee Referral','Employee Referral - Non-sales','External Referral','Free Corporate Domain','Free w CC','GoogleSign','Inbound Call','Inbound Call via Customer Support','Inbound Email','Jigsaw','Live Demo','Live Demo Absentee','Live Demo Attended','Live Training Attendee','Marketing List','Marketing-purchased Leads','Nurtured Free Signups','Online Purchase','Partner Deal Reg','Partner Portal Access','Partner Referral Registration','Partner Request','Partner Resale Registration','Referral','Referrer Request','Request Demo','Sales Chat','Seminar - Internal','Seminar - Partner','Trade Show','Tradeshow','Web','Webinar','Webinar Absentee','Webinar Attended','Webinar OnDemand','White Paper','Word of mouth','Zoom Room Free Trial')
GROUP BY 1
ORDER BY 1;



SELECT fy_quarter,
       COUNT(*)
FROM src_sfdc.lead a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.createddate::DATE BETWEEN b.qtr_start
        AND b.qtr_end
WHERE   isdeleted = FALSE
and leadsource in
(
'Contact Sales',
'Inbound Call',
'Inbound Call via Customer Support',
'Inbound Email',
'Request Demo',
'Sales Chat',
'Zoom Room Free Trial')
GROUP BY 1
ORDER BY 1;




---- DIRECT 
SELECT b.fy_quarter,
       c.leadsource,
       CASE
         WHEN a.Owner_Division__c = 'ISV' THEN 'Direct - ISV'
         WHEN lower(a.Owner_Division__c) LIKE '%intl%' THEN 'Direct - INTL Teams'
         ELSE 'Direct - US Teams'
       END AS division,
       a.Owner_Division__c,
       CASE
         WHEN lower(a.owner_division__c) LIKE '%vast%' THEN 'VAST'
         WHEN lower(d.sub_division__c) = 'vast' THEN 'VAST'
         WHEN lower(d.division) LIKE '%vast%' THEN 'VAST'
         ELSE 'Aquisition'
       END AS acq_vs_vast,
       SUM(a.Amount__c) AS direct,
       CASE
         WHEN leadsource IN ('AVer 60 Day Pro Free Trial','Appointment Setting Vendor','Contact Sales','Customer Referrals','Employee Referral','Employee Referral - Non-sales','External Referral','Free Corporate Domain','Free w CC','GoogleSign','Inbound Call','Inbound Call via Customer Support','Inbound Email','Jigsaw','Live Demo','Live Demo Absentee','Live Demo Attended','Live Training Attendee','Marketing List','Marketing-purchased Leads','Nurtured Free Signups','Online Purchase','Partner Deal Reg','Partner Portal Access','Partner Referral Registration','Partner Request','Partner Resale Registration','Referral','Referrer Request','Request Demo','Sales Chat','Seminar - Internal','Seminar - Partner','Trade Show','Tradeshow','Web','Webinar','Webinar Absentee','Webinar Attended','Webinar OnDemand','White Paper','Word of mouth','Zoom Room Free Trial') THEN 'inbound'
         WHEN leadsource IN ('Outbound Sales','Overage','Upsell','WorkSign','Zoom Info','ZoomInfo') THEN 'outbound'
         ELSE 'neither'
       END AS inbound_v_outbound,
       CASE
         WHEN e.billingcountry IN ('United States','India','Ireland', 'United Kingdom','Canada','Australia','Brazil','Other','Mexico','China','Germany','France','Japan','Colombia','Saudi Arabia','Spain','Egypt','South Africa','Russian Federation','Italy','New Zealand','Indonesia','Argentina','Israel','Philippines','Pakistan') THEN e.billingcountry
         ELSE 'Rest of World'
       END AS country,
       e.billing_country_region__c,
              a.employee_count__c
FROM src_sfdc.bookings a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.booking_date__c BETWEEN b.qtr_start
        AND b.qtr_end
  LEFT JOIN src_sfdc.opportunity c ON a.opportunity__c = c.id
  LEFT JOIN src_sfdc.user_history d
         ON d.id = a.ownerid
        AND a.booking_date__c = d.dt
  LEFT JOIN src_sfdc.account e ON a.account__c = e.id
WHERE 1 = 1
AND   ((a.Order_Type__c IN ('New','New Order') AND a.Amount__c >= 17)
---- exclusive of 14.99
OR (a.Order_Type__c = 'Upsell' AND a.Amount__c >= 0) OR (a.Order_Type__c IN ('New','New Order') AND a.Amount__c < 17 AND a.Coupon__c <> '' AND a.coupon__c IS NOT NULL)
--- picking up 14.99 slack
OR (a.bookingexception__c = 'Y'))
AND   lower(a.owner_name) NOT LIKE '%integration%'
AND   a.isdeleted = FALSE
AND   a.account__c <> ''
AND   a.account__c IS NOT NULL
GROUP BY 1,
         2,
         3,
         4,
         5,
         7,
         8,
         9,10
ORDER BY 2,
         1







--------------
--------------
--------------
--------------
--------------
-------------- chat conversion
--------------
--------------
--------------

WITH 
t1 as (
SELECT domofiscalyear+1||'-'||domofiscalquarter as FYQuarter, count(*) as sales_chat_count    
FROM src_zendesk.chats as a
LEFT JOIN src_config.zoom_calendar as c
ON a.session_start_date::DATE = c.dt
WHERE lower(department_name) in ('sales')
AND lower(missed) != 'true'
AND session_start_date::TIMESTAMP >= '2017-08-01'
GROUP BY 1
),
t2 as 
(SELECT domofiscalyear+1||'-'||domofiscalquarter as FYQuarter, 
count(*) as lead_count,
sum(case when LENGTH(convertedopportunityid)>10 THEN 1 ELSE 0 END) as opp_count,
sum(case when LENGTH(convertedopportunityid)>10 AND c.iswon = TRUE THEN 1 ELSE 0 END) as deals_count
FROM 
(
SELECT b.new_countries, a.*, d.new_employee_count, e.email as zoom_user_email,
c.*, e.division,
row_number() OVER(PARTITION BY a.email, domofiscalyear||domofiscalquarter ORDER BY a.createddate ASC) as row_num
FROM src_sfdc.lead as a
LEFT JOIN lab.hp_00062v3_map_countries_for_leads as b
ON a.country = b.old_countries
LEFT JOIN src_config.zoom_calendar as c
ON a.createddate::DATE = c.dt
LEFT JOIN lab.hp_00061_map_employee_counts as d
ON a.employee_count__c = d.old_employee_count
LEFT JOIN src_sfdc.user as e
ON a.createdbyid = e.id
WHERE a.isdeleted = 'FALSE'
AND a.createddate::DATE BETWEEN '2017-08-01' AND CURRENT_DATE
AND a.leadsource in (
'Sales Chat')
) as t1
LEFT JOIN src_sfdc.opportunity as c
ON t1.convertedopportunityid = c.id AND c.isdeleted = FALSE
WHERE (row_num=1  OR len(email) <3 OR t1.email is null)
/* AND LENGTH(convertedopportunityid)>10 */
GROUP BY 1
)
SELECT t1.fyquarter as Fiscal_Year_Quarter, sales_chat_count, lead_count ,opp_count, deals_count
FROM t1,t2 
WHERE t1.FYquarter = t2.fyquarter;


--------------
--------------
--------------
--------------
--------------
-------------- inbound leads
--------------
--------------





SELECT nvl(t1.country_region,'Other') AS region,
       CASE
         WHEN new_countries IN ('United States','India', 'Ireland', 'United Kingdom','Canada','Australia','Brazil','Other','Mexico','China','Germany','France','Japan','Colombia','Saudi Arabia','Spain','Egypt','South Africa','Russian Federation','Italy','New Zealand','Indonesia','Argentina','Israel','Philippines','Pakistan') THEN new_countries
         ELSE 'Rest of World'
       END AS country,
       t1.domofiscalyear +1 || '-' ||t1.domofiscalquarter AS FYQuarter,
       'emp_count: ' ||nvl(new_employee_count,'other') AS employee_count,
       CASE
         WHEN lower(T1.owner_division__c) LIKE '%vast%' THEN 'VAST'
         WHEN lower(T1.sub_division__c) = 'vast' THEN 'VAST'
         WHEN lower(T1.division) LIKE '%vast%' THEN 'VAST'
         ELSE 'Aquisition'
       END AS acq_vs_vast,
       CASE
         WHEN t1.segment__c = '' THEN 'n/a'
         WHEN t1.segment__c = 'Online' THEN 'OAE'
         ELSE t1.segment__c
       END AS segment,
       CASE
         WHEN t1.leadsource = 'Inbound Call' THEN 'Inbound Call/Email'
         WHEN t1.leadsource = 'Inbound Call via Customer Support' THEN 'Inbound Call/Email'
         WHEN t1.leadsource = 'Inbound Email' THEN 'Inbound Call/Email'
         WHEN t1.leadsource = 'Trade Show' THEN 'Trade Show'
         WHEN t1.leadsource = 'Tradeshow' THEN 'Trade Show'
         ELSE t1.leadsource
       END AS leadsource,
       CASE
         WHEN NVL (ramp_status.ramp,'n/a') = 'Fully Ramped' THEN 'Ramped'
         ELSE NVL (ramp_status.ramp,'n/a')
       END AS ramp_status,
       COUNT(*) AS leads,
       SUM(CASE WHEN LENGTH(convertedopportunityid) > 10 THEN 1 ELSE 0 END) AS opps_count,
       SUM(CASE WHEN LENGTH(convertedopportunityid) > 10 AND c.iswon = TRUE THEN 1 ELSE 0 END) AS deals_count
FROM (SELECT b.new_countries,
             a.*,
             d.new_employee_count,
             c.*,
             e.country_region__c AS country_region,
             f.sub_division__c, f.division,
             ROW_NUMBER() OVER (PARTITION BY a.email,domofiscalyear||domofiscalquarter ORDER BY a.createddate ASC) AS row_num
      FROM src_sfdc.lead AS a
        LEFT JOIN lab.hp_00062v3_map_countries_for_leads AS b ON a.country = b.old_countries
        LEFT JOIN src_config.zoom_calendar AS c ON a.createddate::DATE = c.dt
        LEFT JOIN lab.hp_00061_map_employee_counts AS d ON a.employee_count__c = d.old_employee_count
        LEFT JOIN (SELECT country,
                          country_region__c
                   FROM src_sfdc.lead
                   WHERE country_region__c IS NOT NULL
                   AND   country_region__c != ''
                   GROUP BY 1,
                            2) AS e ON b.new_countries = e.country
        LEFT JOIN src_sfdc.user_history AS f
               ON f.id = a.ownerid
              AND a.createddate = f.dt
      WHERE a.isdeleted = 'FALSE'
      AND   a.createddate::DATE BETWEEN '2016-08-01' AND CURRENT_DATE
      AND   a.leadsource IN ('AVer 60 Day Pro Free Trial','Appointment Setting Vendor','Contact Sales','Customer Referrals','Employee Referral','Employee Referral - Non-sales','External Referral','Free Corporate Domain','Free w CC','GoogleSign','Inbound Call','Inbound Call via Customer Support','Inbound Email','Jigsaw','Live Demo','Live Demo Absentee','Live Demo Attended','Live Training Attendee','Marketing List','Marketing-purchased Leads','Nurtured Free Signups','Online Purchase','Partner Deal Reg','Partner Portal Access','Partner Referral Registration','Partner Request','Partner Resale Registration','Referral','Referrer Request','Request Demo','Sales Chat','Seminar - Internal','Seminar - Partner','Trade Show','Tradeshow','Web','Webinar','Webinar Absentee','Webinar Attended','Webinar OnDemand','White Paper','Word of mouth','Zoom Room Free Trial')) AS t1
  LEFT JOIN src_sfdc.opportunity AS c
         ON t1.convertedopportunityid = c.id
        AND c.isdeleted = FALSE
  LEFT JOIN src_config.zoom_calendar AS ramp_cal ON t1.createddate::DATE = ramp_cal.dt
  LEFT JOIN rpt.sales_ae_benchmark AS ramp_status ON ('FY' ||RIGHT (ramp_cal.domofiscalyear +1,2) || '-Q' ||ramp_cal.domofiscalquarter||t1.ownername) = (ramp_status.quarter||ramp_status.name)
  LEFT JOIN lab.hp_00073_oae_region_mapping AS f ON t1.ownerid = f.id
WHERE (row_num = 1 OR len(t1.email) < 3 OR t1.email IS NULL)
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8



--------------
--------------
--------------
--------------
-------------- pipeline creation
-------------- 
--------------
--------------

---- pipeline created
SELECT b.fy_quarter,
       a.leadsource,
       a.owner_division__c,
       CASE
         WHEN lower(a.owner_division__c) LIKE '%vast%' THEN 'VAST'
         WHEN lower(d.sub_division__c) = 'vast' THEN 'VAST'
         WHEN lower(d.division) LIKE '%vast%' THEN 'VAST'
         ELSE 'Aquisition'
       END AS acq_vs_vast,
       CASE
         WHEN a.leadsource IN ('AVer 60 Day Pro Free Trial','Appointment Setting Vendor','Contact Sales','Customer Referrals','Employee Referral','Employee Referral - Non-sales','External Referral','Free Corporate Domain','Free w CC','GoogleSign','Inbound Call','Inbound Call via Customer Support','Inbound Email','Jigsaw','Live Demo','Live Demo Absentee','Live Demo Attended','Live Training Attendee','Marketing List','Marketing-purchased Leads','Nurtured Free Signups','Online Purchase','Partner Deal Reg','Partner Portal Access','Partner Referral Registration','Partner Request','Partner Resale Registration','Referral','Referrer Request','Request Demo','Sales Chat','Seminar - Internal','Seminar - Partner','Trade Show','Tradeshow','Web','Webinar','Webinar Absentee','Webinar Attended','Webinar OnDemand','White Paper','Word of mouth','Zoom Room Free Trial') THEN 'inbound'
         WHEN a.leadsource IN ('Outbound Sales','Overage','Upsell','WorkSign','Zoom Info','ZoomInfo') THEN 'outbound'
         ELSE 'neither'
       END AS inbound_v_outbound,
       CASE
         WHEN e.billingcountry IN ('United States','India','Ireland','United Kingdom','Canada','Australia','Brazil','Other','Mexico','China','Germany','France','Japan','Colombia','Saudi Arabia','Spain','Egypt','South Africa','Russian Federation','Italy','New Zealand','Indonesia','Argentina','Israel','Philippines','Pakistan') THEN e.billingcountry
         ELSE 'Rest of World'
       END AS country,
       e.billing_country_region__c,
       a.employee_count__c,
       SUM(a.amount) AS pipe_created,
       COUNT(DISTINCT a.id) AS opp_created_count
FROM src_sfdc.opportunity a
  LEFT JOIN src_config.zoom_quarter_mapping b
         ON a.createddate::DATE BETWEEN b.qtr_start
        AND b.qtr_end
  LEFT JOIN src_sfdc.user_history d
         ON d.id = a.ownerid
        AND a.createddate::DATE = d.dt
  LEFT JOIN src_sfdc.account e ON a.accountid = e.id
WHERE a.isdeleted = FALSE
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8
ORDER BY 2,
         1;


---INbound vs OUTbound ----
------ channel and BDR taken out as well
---- Based on lead sources, sales div based on oppos, sales channel assisted & online classified as not inbound
SELECT domofiscalyear +1 || '-' ||domofiscalquarter AS FYQuarter,
       CASE
         WHEN b.owner_division__c = 'COMM-VAST' THEN 'Up-Market VAST'
         WHEN b.owner_division__c = 'Network Alliance' THEN 'MLM'
         ELSE b.owner_division__c
       END AS owner_division__c,
       CASE
         WHEN a.sales_channel__c IN ('Online','Assisted') THEN 'online or assisted'
         WHEN a.sales_channel__c = 'Indirect Sales' THEN 'channel'
         WHEN b.leadsource IN ('AVer 60 Day Pro Free Trial','Appointment Setting Vendor','Contact Sales','Customer Referrals','Employee Referral','Employee Referral - Non-sales','External Referral','Free Corporate Domain','Free w CC','GoogleSign','Inbound Call','Inbound Call via Customer Support','Inbound Email','Jigsaw','Live Demo','Live Demo Absentee','Live Demo Attended','Live Training Attendee','Marketing List','Marketing-purchased Leads','Nurtured Free Signups','Online Purchase','Partner Deal Reg','Partner Portal Access','Partner Referral Registration','Partner Request','Partner Resale Registration','Referral','Referrer Request','Request Demo','Sales Chat','Seminar - Internal','Seminar - Partner','Trade Show','Tradeshow','Web','Webinar','Webinar Absentee','Webinar Attended','Webinar OnDemand','White Paper','Word of mouth','Zoom Room Free Trial') THEN 'inbound'
         WHEN b.bdr_assigned__c LIKE '%0%' THEN 'bdr'
         WHEN b.leadsource IN ('Outbound Sales','Overage','Upsell','WorkSign','Zoom Info','ZoomInfo') THEN 'outbound'
         ELSE 'neither'
       END AS leadsource,
       a.Order_Type__c AS order_type,
       SUM(a.amount__c) AS total,
       COUNT(a.id) AS booking_count
FROM src_sfdc.bookings AS a
  LEFT JOIN src_config.zoom_calendar AS c ON a.booking_date__c::DATE = c.dt
  LEFT JOIN src_sfdc.opportunity AS b ON a.opportunity__c = b.id
  LEFT JOIN src_sfdc.user_history d
         ON b.ownerid = d.id
        AND d.dt = b.closedate
WHERE 1 = 1
AND   a.booking_date__c::DATE BETWEEN '2017-08-01' AND CURRENT_DATE
AND   a.Order_Type__c IN ('New','New Order','Upsell')
AND   a.isdeleted = FALSE
AND   a.account__c <> ''
AND   a.account__c IS NOT NULL
AND   a.Amount__c > 0
GROUP BY 1,
         2,
         3,
         4


---INbound vs OUTbound ----
------ channel and BDR taken out as well
---- Based on lead sources, sales div based on bookings, sales channel assisted & online classified as not inbound
SELECT domofiscalyear +1 || '-' ||domofiscalquarter AS FYQuarter,
       CASE
         WHEN d.division = 'COMM-VAST' THEN 'Up-Market VAST'
         WHEN d.division = 'Network Alliance' THEN 'MLM'
         ELSE d.division
       END AS owner_division__c,
       CASE
         WHEN a.sales_channel__c IN ('Online','Assisted') THEN 'online or assisted'
         WHEN a.sales_channel__c = 'Indirect Sales' THEN 'channel'
         WHEN b.leadsource IN ('AVer 60 Day Pro Free Trial','Appointment Setting Vendor','Contact Sales','Customer Referrals','Employee Referral','Employee Referral - Non-sales','External Referral','Free Corporate Domain','Free w CC','GoogleSign','Inbound Call','Inbound Call via Customer Support','Inbound Email','Jigsaw','Live Demo','Live Demo Absentee','Live Demo Attended','Live Training Attendee','Marketing List','Marketing-purchased Leads','Nurtured Free Signups','Online Purchase','Partner Deal Reg','Partner Portal Access','Partner Referral Registration','Partner Request','Partner Resale Registration','Referral','Referrer Request','Request Demo','Sales Chat','Seminar - Internal','Seminar - Partner','Trade Show','Tradeshow','Web','Webinar','Webinar Absentee','Webinar Attended','Webinar OnDemand','White Paper','Word of mouth','Zoom Room Free Trial') THEN 'inbound'
         WHEN b.bdr_assigned__c LIKE '%0%' THEN 'bdr'
         WHEN b.leadsource IN ('Outbound Sales','Overage','Upsell','WorkSign','Zoom Info','ZoomInfo') THEN 'outbound'
         ELSE 'neither'
       END AS leadsource,
       a.Order_Type__c AS order_type,
       SUM(a.amount__c) AS total,
       COUNT(a.id) AS booking_count
FROM src_sfdc.bookings AS a
  LEFT JOIN src_config.zoom_calendar AS c ON a.booking_date__c::DATE = c.dt
  LEFT JOIN src_sfdc.opportunity AS b ON a.opportunity__c = b.id
  LEFT JOIN src_sfdc.user_history d
         ON b.ownerid = d.id
        AND d.dt = b.closedate
WHERE 1 = 1
AND   a.booking_date__c::DATE BETWEEN '2017-08-01' AND CURRENT_DATE
AND   a.Order_Type__c IN ('New','New Order','Upsell')
AND   a.isdeleted = FALSE
AND   a.account__c <> ''
AND   a.account__c IS NOT NULL
AND   a.Amount__c > 0
GROUP BY 1,
         2,
         3,
         4

