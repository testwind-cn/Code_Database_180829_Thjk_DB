select * from ds_mcht A
left join ds_mcht_intention E on A.mcht_intention_tid=E.tid and E.is_delete='0'
LEFT JOIN ds_mcht_intention E1 on E.parent_tid=E1.tid  and E1.is_delete='0' where E1.tid='1';

SELECT
    concat(
        reverse( lpad( cast(pmod(hash(mcht_cd),10000) as STRING),4,'0')),
        lpad( mcht_cd,20,'*' ),
        substr( rpad( ori_dt,12,'0'),1,12 ),
        lpad( cast( pmod( hash(sn),1000) as STRING),3,'0')
    )
FROM `rds_posflow`.bl_total_transflow LIMIT 1000;