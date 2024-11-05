delete tutorial.LBR_in_store_activities_performance_tracking_base_table;
insert into tutorial.LBR_in_store_activities_performance_tracking_base_table 


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
    )
    
     ,CTE AS (
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
              CASE WHEN DATE(mbr.join_time) = DATE(verification_time) AND  rev.store_code = mbr.eff_reg_store THEN 1 ELSE 0 END AS new_member_tag         --- need to check LBR store code !!!!!!!!
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
        WHERE lvm.campaign_code is not null
          and res_status = '已核销'
          and ps.distributor = 'LBR'                                        ------ 只看LBR
        )
     
        
,CTE_trans_0 AS (
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
           campaign_code,
           campaign_type,
           new_member_tag,
           campaign_name
    FROM CTE
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
        COUNT(DISTINCT CASE WHEN cte.new_member_tag = 0 THEN cte.member_id ELSE NULL END)                                  AS ttl_existing_participants--总老会员参与数
    FROM CTE_trans_0 cte
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
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
    base_0.date_id ,
              ----本活动场次总数-----
    reservations.ttl_reservations                                                                              AS ttl_reservations,             --总预约人数
    ttl_scheduling_check                                                                                       AS ttl_reservation_participants, --预约且签到
    ttl_check                                                                                                  AS ttl_walkin_participants,--walkin
    ttl_participants                                                                                           AS ttl_participants,--总参与人数
    ttl_new_participants                                                                                       AS new_participants,
    ttl_existing_participants                                                                                  AS existing_participants
FROM base_0
LEFT JOIN reservations
   on base_0.campaign_code = reservations.campaign_code
    and base_0.store_id = reservations.store_id
    and base_0.event_id = reservations.event_id
    ;