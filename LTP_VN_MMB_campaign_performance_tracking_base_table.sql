delete tutorial.LTP_VN_MMB_campaign_performance_tracking_base_table;
insert into tutorial.LTP_VN_MMB_campaign_performance_tracking_base_table 

-- drop table if exists tutorial.LTP_VN_MMB_campaign_performance_tracking_base_table;
-- create table tutorial.LTP_VN_MMB_campaign_performance_tracking_base_table as 

WITH reservations AS (
      SELECT rev.campaign_code,
             rev.agent_id                   AS partner,
             rev.store_code                  AS store_id,
             event_id,
             count(distinct type_value)      AS ttl_reservations
    FROM edw.d_dl_wmp_campaign_reservation rev
    WHERE res_status <> '已取消'
      AND source_type = 1
    group by 1,2,3,4
    ),
    
CTE AS (
       SELECT DISTINCT
                lvm.campaign_code,
                lvm.campaign_type,
                rev.campaign_name,
               rev.agent_id,
               ps.distributor,
               ps.region,
               ps.partner,
              store.city_name AS city,
              rev.store_code  AS store_id,
              store.name      AS store_name,
              event_id,
              DATE(verification_time) AS event_dt,
              source_type,
              type_value      AS member_id,
              res_status,
              mbr.join_time,
              CASE WHEN DATE(mbr.join_time) = DATE(verification_time) AND  rev.store_code = mbr.eff_reg_store THEN 1 ELSE 0 END AS new_member_tag,
             CASE WHEN coupon_redeemed.member_detail_id IS NOT NULL THEN 1 ELSE 0 END   AS redeemed_coupon
        FROM edw.d_dl_wmp_campaign_reservation rev
        left join edw.d_dl_phy_store ps
            on ps.lego_store_code=rev.store_code
        LEFT JOIN stg.wmp_store store
                ON rev.store_id = store.store_id
        LEFT JOIN (select DISTINCT
                        member_detail_id,
                        join_time, 
                        eff_reg_store
                        from edw.d_member_detail
                    ) mbr
              ON rev.type_value::integer = mbr.member_detail_id::integer
        left join tutorial.ltp_vn_mbb_campaign_code lvm
            on lvm.campaign_code = rev.campaign_code
        LEFT JOIN (  SELECT DISTINCT member_detail_id,   date(redeemed_time) AS redeemed_dt, redeemed_channel_code
                    FROM edw.d_coupon_benefit
                    WHERE coupon_code = 'Q240065'
                    AND coupon_status_code = 'redeemed'
                  ) coupon_redeemed
               ON DATE(rev.verification_time) = coupon_redeemed.redeemed_dt
              AND rev.store_code = coupon_redeemed.redeemed_channel_code
              AND rev.type_value::integer = coupon_redeemed.member_detail_id::integer
              AND lvm.campaign_type = 'LTP'
        WHERE lvm.campaign_code is not null
          and res_status = '已核销'
        )
    
                  
                  
,CTE_trans_0 AS (
SELECT CTE_CTE.*,
      SUM(sales.order_rrp_amt)                                                                          AS converted_0_days_rrp_amt
  FROM (
    SELECT agent_id,
           distributor,
           region,
           partner,
           city,
           store_id,
           store_name,
           event_id,
           event_dt,
           source_type,
           CTE.member_id,
            trans.crm_member_id,
            trans.initial_purchase,
           campaign_code,
           campaign_type,
           new_member_tag,
           redeemed_coupon,
           campaign_name,
           CASE WHEN trans.crm_member_id IS NULL THEN 0 ELSE 1 END                                            AS converted_0_days,
          CASE WHEN trans.initial_purchase = 1 THEN 1 ELSE 0 END                                             AS converted_is_initial_0_dyas,
           SUM(trans.trans_cnt)                                                                               AS converted_0_days_order_cnt
    FROM CTE
    LEFT JOIN (SELECT DISTINCT crm_member_id, 
                      date_id,
                      lego_store_code,
                      COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE THEN original_order_id ELSE NULL END) AS trans_cnt
                      ,MAX(CASE WHEN initial_vs_repurchase_lifecycle = '会员生命周期首单' THEN 1 ELSE 0 END)  AS initial_purchase
                 FROM edw.f_member_order_detail             
                 WHERE is_rrp_sales_type = 1
                   AND distributor_name <> 'LBR'
                   AND if_eff_order_tag IS TRUE
                   AND crm_member_id IS NOT NULL
                  GROUP BY 1,2,3
             ) trans
           ON CTE.member_id::integer = trans.crm_member_id::integer
          AND DATE(trans.date_id) = DATE(CTE.event_dt) 
          and trans.lego_store_code=CTE.store_id
     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
     ) CTE_CTE
     LEFT JOIN (SELECT DISTINCT crm_member_id, 
                      date_id,
                      lego_store_code,
                      sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS order_rrp_amt
                 FROM edw.f_member_order_detail                                 
                 WHERE is_rrp_sales_type = 1
                  AND distributor_name <> 'LBR'
                  AND crm_member_id IS NOT NULL
             GROUP BY 1,2,3
             ) sales
          ON CTE_CTE.member_id::integer = sales.crm_member_id::integer
          AND DATE(sales.date_id) = DATE(CTE_CTE.event_dt)
          and sales.lego_store_code=CTE_CTE.store_id
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
        )
        
        
        
 ,CTE_trans_7 AS (
 SELECT CTE_CTE.*,
        SUM(sales_7_day.order_rrp_amt)                                                                     AS converted_7_days_rrp_amt
    FROM (
    SELECT agent_id,
          distributor,
          region,
          partner,
          city,
          store_id,
          store_name,
          event_id,
          event_dt,
          source_type,
          CTE.member_id,
            trans_7.crm_member_id as crm_member_id_7,
            trans_7.initial_purchase as initial_purchase_7,
          campaign_code,
          campaign_type,
          new_member_tag,
          CASE WHEN trans_7.crm_member_id IS NULL THEN 0 ELSE 1 END                                            AS converted_7_days,
          CASE WHEN trans_7.initial_purchase = 1 THEN 1 ELSE 0 END                                             AS converted_is_initial_7_dyas,
          SUM(trans_7.trans_cnt)                                                                               AS converted_7_days_order_cnt
           
    FROM CTE
   
      ------- 7 days-----
     LEFT JOIN (SELECT DISTINCT crm_member_id, 
                      date_id,
                      lego_store_code,
                      COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE THEN original_order_id ELSE NULL END) AS trans_cnt
                     ,MAX(CASE WHEN initial_vs_repurchase_lifecycle = '会员生命周期首单' THEN 1 ELSE 0 END)  AS initial_purchase
                 FROM edw.f_member_order_detail             
                 WHERE is_rrp_sales_type = 1
                  AND distributor_name <> 'LBR'
                  AND if_eff_order_tag IS TRUE
                  AND crm_member_id IS NOT NULL
                  GROUP BY 1,2,3
             ) trans_7
          ON CTE.member_id::integer = trans_7.crm_member_id::integer
          AND DATE(trans_7.date_id) - DATE(CTE.event_dt) <= 7
          AND  DATE(trans_7.date_id) - DATE(CTE.event_dt) >= 0
          and trans_7.lego_store_code=CTE.store_id
     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
     ) CTE_CTE
      LEFT JOIN (SELECT DISTINCT crm_member_id, 
                      date_id,
                      lego_store_code,
                      sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS order_rrp_amt
                 FROM edw.f_member_order_detail                                 
                 WHERE is_rrp_sales_type = 1
                  AND distributor_name <> 'LBR'
                  AND crm_member_id IS NOT NULL
             GROUP BY 1,2,3
             ) sales_7_day
          ON CTE_CTE.member_id::integer = sales_7_day.crm_member_id::integer
          AND DATE(sales_7_day.date_id) - DATE(CTE_CTE.event_dt) <= 7
          AND DATE(sales_7_day.date_id) - DATE(CTE_CTE.event_dt) >= 0 
          and sales_7_day.lego_store_code=CTE_CTE.store_id
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
        
        
,CTE_trans_30 AS (
SELECT CTE_CTE.*,
       SUM(sales_30_day.order_rrp_amt)                                                                     AS converted_30_days_rrp_amt
     FROM (
    SELECT agent_id,
          distributor,
          region,
          partner,
          city,
          store_id,
          store_name,
          event_id,
          event_dt,
          source_type,
          CTE.member_id,
            trans_30.crm_member_id as crm_member_id_30, 
            trans_30.initial_purchase as initial_purchase_30,
          campaign_code,
          campaign_type,
          new_member_tag,
          CASE WHEN trans_30.crm_member_id IS NULL THEN 0 ELSE 1 END                                            AS converted_30_days,
          CASE WHEN trans_30.initial_purchase = 1 THEN 1 ELSE 0 END                                             AS converted_is_initial_30_dyas,
          SUM(trans_30.trans_cnt)                                                                               AS converted_30_days_order_cnt
    FROM CTE
    -- -- ------30days--------
    
      LEFT JOIN (SELECT DISTINCT crm_member_id, 
                      date_id,
                      lego_store_code,
                      COUNT(DISTINCT CASE WHEN if_eff_order_tag IS TRUE THEN original_order_id ELSE NULL END) AS trans_cnt
                      ,MAX(CASE WHEN initial_vs_repurchase_lifecycle = '会员生命周期首单' THEN 1 ELSE 0 END)  AS initial_purchase
                 FROM edw.f_member_order_detail             
                 WHERE is_rrp_sales_type = 1
                  AND distributor_name <> 'LBR'
                  AND if_eff_order_tag IS TRUE
                  AND crm_member_id IS NOT NULL
                  GROUP BY 1,2,3
             ) trans_30
          ON CTE.member_id::integer = trans_30.crm_member_id::integer
          AND DATE(trans_30.date_id) - DATE(CTE.event_dt) <= 30
          AND  DATE(trans_30.date_id) - DATE(CTE.event_dt) >= 0
          and trans_30.lego_store_code=CTE.store_id
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
          ) CTE_CTE
          
       LEFT JOIN (SELECT DISTINCT crm_member_id, 
              date_id,
              lego_store_code,
              sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS order_rrp_amt
         FROM edw.f_member_order_detail                                 
         WHERE is_rrp_sales_type = 1
          AND distributor_name <> 'LBR'
          AND crm_member_id IS NOT NULL
     GROUP BY 1,2,3
     ) sales_30_day
  ON CTE_CTE.member_id::integer = sales_30_day.crm_member_id::integer
  AND DATE(sales_30_day.date_id) - DATE(CTE_CTE.event_dt) <= 30
  AND DATE(sales_30_day.date_id) - DATE(CTE_CTE.event_dt) >= 0
  and sales_30_day.lego_store_code=CTE_CTE.store_id
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
        
          
,base_0 as(    
    SELECT cte.campaign_type,
        cte.agent_id,
        cte.distributor,
        cte.region,
        cte.partner,
        cte.city,
        cte.store_id,
        cte.store_name,
        cte.store_id || ' ' ||cte.store_name as store_id_store_name,
        cte.campaign_code,
        cte.campaign_name,
        cte.event_id,
        DATE(cte.event_dt) as date_id,
        ----本活动场次总数-----
        count(distinct case when cte.source_type = 1 then cte.member_id else null end)                                     AS ttl_scheduling_check,--预约且签到
        count(distinct case when cte.source_type = 2 then cte.member_id else null end)                                     AS ttl_check,--现场签到
        COUNT(DISTINCT cte.member_id)                                                                                      AS ttl_participants,--总参与人数
        COUNT(DISTINCT CASE WHEN cte.new_member_tag = 1 THEN cte.member_id ELSE NULL END)                                  AS ttl_new_participants,--总新会员参与数
        COUNT(DISTINCT CASE WHEN cte.new_member_tag = 0 THEN cte.member_id ELSE NULL END)                                  AS ttl_existing_participants,--总老会员参与数
        COUNT(DISTINCT CASE WHEN cte.redeemed_coupon = 1 THEN cte.member_id else null end)                                 AS ttl_redeemed_recruitment_bags,
        ------------- 0days--------------    
        COUNT(DISTINCT CASE WHEN converted_0_days = 1 THEN cte.member_id ELSE NULL END)                                            AS converted_member_0_days,--当日内转化会员数
        COUNT(DISTINCT CASE WHEN converted_0_days = 1 AND cte.new_member_tag = 1 THEN cte.member_id ELSE NULL END)                     AS converted_new_member_0_days,--当日内转化新会员数
        COUNT(DISTINCT CASE WHEN converted_0_days = 1 AND cte.new_member_tag = 0 THEN cte.member_id ELSE NULL END)                     AS converted_existing_member_0_days,--当日内转化老会员数
        
        converted_member_0_days*1.0/ NULLIF(ttl_participants, 0)                                                                                  as converted_member_rate_0_days,--会员转化率
        converted_new_member_0_days*1.0/NULLIF(ttl_new_participants, 0)                                                                          as converted_new_member_rate_0_days, --新会员转化率
        converted_existing_member_0_days*1.0/NULLIF(ttl_existing_participants, 0)                                                               as converted_existing_member_rate_0_days,--老会员转化率
        
        SUM(CASE WHEN converted_0_days = 1 THEN converted_0_days_rrp_amt ELSE 0 END)                                         AS converted_rrp_amt_0_days,--当日内会员消费额
        SUM(CASE WHEN converted_0_days = 1 AND new_member_tag = 1 THEN converted_0_days_rrp_amt ELSE 0 END)                  AS converted_rrp_amt_new_0_days,--当日内新会员消费额
        SUM(CASE WHEN converted_0_days = 1 AND new_member_tag = 0 THEN converted_0_days_rrp_amt ELSE 0 END)                  AS converted_rrp_amt_existing_0_days,--当日内老会员消费额
        
        converted_rrp_amt_0_days*1.0/NULLIF(converted_member_0_days, 0)                                                      as converted_member_aspp_0_days,--会员ASPP
        converted_rrp_amt_new_0_days*1.0/NULLIF(converted_new_member_0_days, 0)                                              as converted_new_member_aspp_0_days, --新会员ASPP
        converted_rrp_amt_existing_0_days*1.0/NULLIF(converted_existing_member_0_days, 0)                                    as converted_existing_member_aspp_0_days, --老会员ASPP
        
        SUM(CASE WHEN converted_0_days = 1 THEN converted_0_days_order_cnt ELSE 0 END)                                         AS converted_order_cnt_0_days,--当日内会员订单数
        SUM(CASE WHEN converted_0_days = 1 AND new_member_tag = 1 THEN converted_0_days_order_cnt ELSE 0 END)                  AS converted_order_cnt_new_0_days,--当日内新会员订单数
        SUM(CASE WHEN converted_0_days = 1 AND new_member_tag = 0 THEN converted_0_days_order_cnt ELSE 0 END)                  AS converted_order_cnt_existing_0_days,--当日内老会员订单数
        
        converted_rrp_amt_0_days*1.0/NULLIF(converted_order_cnt_0_days, 0)                                                                  as converted_member_atv_0_days,--会员ATV
        converted_rrp_amt_new_0_days*1.0/NULLIF(converted_order_cnt_new_0_days, 0)                                                          as converted_new_member_atv_0_days, --新会员ATV
        converted_rrp_amt_existing_0_days*1.0/NULLIF(converted_order_cnt_existing_0_days, 0)                                                as converted_existing_member_atv_0_days,--老会员ATV
        
        COUNT(DISTINCT CASE WHEN converted_0_days = 1 AND converted_is_initial_0_dyas = 1 THEN cte.member_id ELSE NULL END)        AS converted_initial_member_0_days,--当日内转化首购会员数
        COUNT(DISTINCT CASE WHEN converted_0_days = 1 AND converted_is_initial_0_dyas = 0 THEN cte.member_id ELSE NULL END)        AS converted_repurchase_member_0_days--当日内转化复购会员数
     
    FROM CTE_trans_0 cte
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
    -- ,10
)


,base_7 as(
    SELECT campaign_type,
        partner,
        city,
        store_id,
        store_name,
        -- store_id || ' ' || store_name as store_id_store_name,
        campaign_code,
        -- campaign_name,
        event_id,
        DATE(event_dt) as date_id,
        -----------------7days--------------
        COUNT(DISTINCT CASE WHEN converted_7_days = 1 THEN member_id ELSE NULL END)                                            AS converted_member_7_days,--当日内转化会员数
        COUNT(DISTINCT CASE WHEN converted_7_days = 1 AND new_member_tag = 1 THEN member_id ELSE NULL END)                     AS converted_new_member_7_days,--当日内转化新会员数
        COUNT(DISTINCT CASE WHEN converted_7_days = 1 AND new_member_tag = 0 THEN member_id ELSE NULL END)                     AS converted_existing_member_7_days,--当日内转化老会员数
        
        SUM(CASE WHEN converted_7_days = 1 THEN converted_7_days_rrp_amt ELSE 0 END)                                         AS converted_rrp_amt_7_days,--当日内会员消费额
        SUM(CASE WHEN converted_7_days = 1 AND new_member_tag = 1 THEN converted_7_days_rrp_amt ELSE 0 END)                  AS converted_rrp_amt_new_7_days,--当日内新会员消费额
        SUM(CASE WHEN converted_7_days = 1 AND new_member_tag = 0 THEN converted_7_days_rrp_amt ELSE 0 END)                  AS converted_rrp_amt_existing_7_days,--当日内老会员消费额
        
        SUM(CASE WHEN converted_7_days = 1 THEN converted_7_days_order_cnt ELSE 0 END)                                         AS converted_order_cnt_7_days,--当日内会员订单数
        SUM(CASE WHEN converted_7_days = 1 AND new_member_tag = 1 THEN converted_7_days_order_cnt ELSE 0 END)                  AS converted_order_cnt_new_7_days,--当日内新会员订单数
        SUM(CASE WHEN converted_7_days = 1 AND new_member_tag = 0 THEN converted_7_days_order_cnt ELSE 0 END)                  AS converted_order_cnt_existing_7_days,--当日内老会员订单数
        
        converted_rrp_amt_7_days*1.0/NULLIF(converted_order_cnt_7_days, 0)                                                                  as converted_member_atv_7_days,--会员ATV
        converted_rrp_amt_new_7_days*1.0/NULLIF(converted_order_cnt_new_7_days , 0)                                                         as converted_new_member_atv_7_days, --新会员ATV
        converted_rrp_amt_existing_7_days*1.0/NULLIF(converted_order_cnt_existing_7_days, 0)                                                as converted_existing_member_atv_7_days,--老会员ATV
        
        COUNT(DISTINCT CASE WHEN converted_7_days = 1 AND converted_is_initial_7_dyas = 1 THEN member_id ELSE NULL END)        AS converted_initial_member_7_days,--当日内转化首购会员数
        COUNT(DISTINCT CASE WHEN converted_7_days = 1 AND converted_is_initial_7_dyas = 0 THEN member_id ELSE NULL END)        AS converted_repurchase_member_7_days--当日内转化复购会员数
    from CTE_trans_7 
    group by 1,2,3,4,5,6,7,8
    -- ,8
)
      
 ,base_30 as(
    SELECT campaign_type,
        partner,
        city,
        store_id,
        store_name,
        -- store_id || ' ' || store_name as store_id_store_name,
        campaign_code,
        -- campaign_name,
        event_id,
        DATE(event_dt) as date_id,
        -- -----------------30days--------------
        COUNT(DISTINCT CASE WHEN converted_30_days = 1 THEN member_id ELSE NULL END)                                            AS converted_member_30_days,--当日内转化会员数
        COUNT(DISTINCT CASE WHEN converted_30_days = 1 AND new_member_tag = 1 THEN member_id ELSE NULL END)                     AS converted_new_member_30_days,--当日内转化新会员数
        COUNT(DISTINCT CASE WHEN converted_30_days = 1 AND new_member_tag = 0 THEN member_id ELSE NULL END)                     AS converted_existing_member_30_days,--当日内转化老会员数
        
        
        SUM(CASE WHEN converted_30_days = 1 THEN converted_30_days_rrp_amt ELSE 0 END)                                         AS converted_rrp_amt_30_days,--当日内会员消费额
        SUM(CASE WHEN converted_30_days = 1 AND new_member_tag = 1 THEN converted_30_days_rrp_amt ELSE 0 END)                  AS converted_rrp_amt_new_30_days,--当日内新会员消费额
        SUM(CASE WHEN converted_30_days = 1 AND new_member_tag = 0 THEN converted_30_days_rrp_amt ELSE 0 END)                  AS converted_rrp_amt_existing_30_days,--当日内老会员消费额
        
        
        SUM(CASE WHEN converted_30_days = 1 THEN converted_30_days_order_cnt ELSE 0 END)                                         AS converted_order_cnt_30_days,--当日内会员订单数
        SUM(CASE WHEN converted_30_days = 1 AND new_member_tag = 1 THEN converted_30_days_order_cnt ELSE 0 END)                  AS converted_order_cnt_new_30_days,--当日内新会员订单数
        SUM(CASE WHEN converted_30_days = 1 AND new_member_tag = 0 THEN converted_30_days_order_cnt ELSE 0 END)                  AS converted_order_cnt_existing_30_days,--当日内老会员订单数
        
        converted_rrp_amt_30_days*1.0/NULLIF(converted_order_cnt_30_days, 0)                                                                  as converted_member_atv_30_days,--会员ATV
        converted_rrp_amt_new_30_days*1.0/NULLIF(converted_order_cnt_new_30_days, 0)                                                          as converted_new_member_atv_30_days, --新会员ATV
        converted_rrp_amt_existing_30_days*1.0/NULLIF(converted_order_cnt_existing_30_days, 0)                                                as converted_existing_member_atv_30_days,--老会员ATV
        
        COUNT(DISTINCT CASE WHEN converted_30_days = 1 AND converted_is_initial_30_dyas = 1 THEN member_id ELSE NULL END)        AS converted_initial_member_30_days,--当日内转化首购会员数
        COUNT(DISTINCT CASE WHEN converted_30_days = 1 AND converted_is_initial_30_dyas = 0 THEN member_id ELSE NULL END)        AS converted_repurchase_member_30_days--当日内转化复购会员数
    from CTE_trans_30
    group by 1,2,3,4,5,6,7,8
    -- ,8
)


SELECT
    base_0.campaign_type,
    base_0.agent_id,    
    base_0.partner,
    base_0.distributor,
    base_0.city,
    base_0.region,
    base_0.store_id,
    base_0.store_name,
    base_0.store_id || ' ' ||base_0.store_name as store_id_store_name,
    base_0.campaign_code,
    base_0.campaign_name,
    -- cte.event_id,
    base_0.date_id ,
              ----本活动场次总数-----
    0                                                                                                          AS ttl_invitation,
    reservations.ttl_reservations                                                                              AS ttl_reservations,--总预约人数
    ttl_scheduling_check                                                                                       AS ttl_reservation_participants, --预约且签到
    ttl_check                                                                                                  AS ttl_walkin_participants,--预约
    ttl_participants                                                                                           AS ttl_participants,--总参与人数
    ttl_redeemed_recruitment_bags                                                                              as ttl_redeemed_recruitment_bags,--总核销人数
    ttl_new_participants                                                                                       AS new_participants,
    ttl_existing_participants                                                                                  AS existing_participants,
          ------------- 0days--------------    
    converted_member_0_days,
    converted_new_member_0_days,
    converted_existing_member_0_days,
    
    converted_member_rate_0_days                                                                               AS CR_0_day,
    converted_new_member_rate_0_days                                                                           AS new_member_CR_0_day,
    converted_existing_member_rate_0_days                                                                      AS existing_member_CR_0_day,
    
    converted_rrp_amt_0_days,
    converted_rrp_amt_new_0_days,
    converted_rrp_amt_existing_0_days,
    
    converted_member_aspp_0_days,
    converted_new_member_aspp_0_days,
    converted_existing_member_aspp_0_days,
    
    converted_order_cnt_0_days,
    converted_order_cnt_new_0_days,
    converted_order_cnt_existing_0_days,
    
    converted_member_atv_0_days,
    converted_new_member_atv_0_days,
    converted_existing_member_atv_0_days,
    
    converted_initial_member_0_days,
    converted_repurchase_member_0_days,
          ------------- 7days--------------    

    converted_member_7_days,
    converted_new_member_7_days,
    converted_existing_member_7_days,
     
    converted_member_7_days*1.0/NULLIF(ttl_participants, 0)                                                                       as CR_7_days,--会员转化率
    converted_new_member_7_days*1.0/NULLIF(ttl_new_participants, 0)                                                               as new_member_CR_7_days, --新会员转化率
    converted_existing_member_7_days*1.0/NULLIF(ttl_existing_participants, 0)                                                     as existing_member_CR_7_days,--老会员转化率
      
    converted_rrp_amt_7_days,
    converted_rrp_amt_new_7_days,
    converted_rrp_amt_existing_7_days,
    
    converted_rrp_amt_7_days*1.0/NULLIF(converted_member_7_days, 0)                                                                 as converted_member_aspp_7_days,--会员ASPP
    converted_rrp_amt_new_7_days*1.0/NULLIF(converted_new_member_7_days, 0)                                                                as converted_new_member_aspp_7_days, --新会员ASPP
    converted_rrp_amt_existing_7_days*1.0/NULLIF(converted_existing_member_7_days, 0)                                                      as converted_existing_member_aspp_7_days, --老会员ASPP
        
    converted_order_cnt_7_days,
    converted_order_cnt_new_7_days,
    converted_order_cnt_existing_7_days,
    
    converted_member_atv_7_days,
    converted_new_member_atv_7_days,
    converted_existing_member_atv_7_days,
    
    converted_initial_member_7_days,
    converted_repurchase_member_7_days,
    
          ------------- 30days--------------    

    converted_member_30_days,
    converted_new_member_30_days,
    converted_existing_member_30_days,
    
    converted_member_30_days*1.0/NULLIF(ttl_participants , 0)                                                                       as CR_30_days,--会员转化率
    converted_new_member_30_days*1.0/NULLIF(ttl_new_participants, 0)                                                                as new_member_CR_30_days, --新会员转化率
    converted_existing_member_30_days*1.0/NULLIF(ttl_existing_participants, 0)                                                      as existing_member_CR_30_days,--老会员转化率
        
    converted_rrp_amt_30_days,
    converted_rrp_amt_new_30_days,
    converted_rrp_amt_existing_30_days,
    
    converted_rrp_amt_30_days*1.0/NULLIF(converted_member_30_days, 0)                                                                as converted_member_aspp_30_days,--会员ASPP
    converted_rrp_amt_new_30_days*1.0/NULLIF(converted_new_member_30_days, 0)                                                                as converted_new_member_aspp_30_days, --新会员ASPP
    converted_rrp_amt_existing_30_days*1.0/NULLIF(converted_existing_member_30_days, 0)                                                      as converted_existing_member_aspp_30_days, --老会员ASPP
        
    converted_order_cnt_30_days,
    converted_order_cnt_new_30_days,
    converted_order_cnt_existing_30_days,
    
    converted_member_atv_30_days,
    converted_new_member_atv_30_days,
    converted_existing_member_atv_30_days,
    
    converted_initial_member_30_days,
    converted_repurchase_member_30_days,
    
    to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date,
    getdate()                                                   AS dl_load_time

FROM base_0
LEFT JOIN reservations
  on base_0.campaign_code = reservations.campaign_code
    and base_0.store_id = reservations.store_id
    and base_0.event_id = reservations.event_id
LEFT JOIN base_7
    on base_7.store_id = base_0.store_id
    and base_7.campaign_code = base_0.campaign_code
    and base_7.event_id = base_0.event_id
    and base_7.date_id = base_0.date_id
LEFT JOIN base_30
    on base_30.store_id = base_0.store_id
    and base_30.campaign_code = base_0.campaign_code
    and base_30.event_id = base_0.event_id
       and base_30.date_id = base_0.date_id;
;