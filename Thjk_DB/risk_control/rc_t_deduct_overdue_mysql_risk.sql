SELECT *,count(MERCHANT_NO) from t_reocn_deduct where CURR_DATE in ('20181103','20181104','20181105','20181106') GROUP BY MERCHANT_NO HAVING count(MERCHANT_NO) > 1


SELECT * from t_reocn_deduct where CURR_DATE in ('20181103','20181104','20181105','20181106') and MERCHANT_NO ='990166059436002'


SELECT *  from t_reocn_deduct where CURR_DATE in ('20181103','20181104','20181105','20181106')  ORDER BY CURR_DATE,MERCHANT_NO


SELECT *  from t_reocn_deduct where CURR_DATE in ('20181103','20181104','20181105','20181106')  PARTITION by CURR_DATE


SELECT *  from t_reocn_deduct where CURR_DATE in ('20181103','20181104','20181105','20181106')  ORDER BY MERCHANT_NO,CURR_DATE

select rr.* ,hhh.* from
(select  @rownuma:=1 )rr,( SELECT *  from t_reocn_deduct where CURR_DATE in ('20181103','20181104','20181105','20181106')  ORDER BY MERCHANT_NO,CURR_DATE ) hhh


# ============ 第一个示例
select @thisbill:=hhh.bill_no as ThisBillNo,
	@lastnum:=@rownuma as LastNum,
  if(@last_bill_no= @thisbill, @rownuma:=@rownuma+1,@rownuma:=1) as ThisNum ,
@last_bill_no:=hhh.bill_no as NEXT_Bill_NO,
hhh.*, rr.*  from
(select  @rownuma:=1,@last_bill_no:='NULL' )rr,( SELECT *  from t_reocn_deduct where CURR_DATE in ('20181103','20181104','20181105','20181106')  ORDER BY MERCHANT_NO,CURR_DATE ) hhh
# ============ 第一个示例


# ============ 第二个示例
select @thisbill:=hhh.bill_no as ThisBillNo,
	@lastnum:=@rownuma as LastNum, # 其实不需要这个
  if(@last_bill_no= @thisbill, @rownuma:=@rownuma+1,@rownuma:=1) as ThisNum ,
@last_bill_no:=hhh.bill_no as NEXT_Bill_NO,
hhh.LATE_DEDUCT_FLAG,
hhh.*, rr.*  from
(select  @rownuma:=1,@last_bill_no:='NULL' )rr,( SELECT *  from t_reocn_deduct  ORDER BY MERCHANT_NO,CURR_DATE ) hhh
# ============ 第二个示例


# ============ 第三个示例
select @thisbill:=hhh.bill_no as ThisBillNo,
	@lastnum:=@rownuma as LastNum, # 其实不需要这个
  if(@last_bill_no= @thisbill and hhh.LATE_DEDUCT_FLAG = 1, @rownuma:=@rownuma+1,@rownuma:=0) as ThisNum ,
@last_bill_no:=hhh.bill_no as NEXT_Bill_NO,
hhh.LATE_DEDUCT_FLAG,
hhh.*, rr.*  from
(select  @rownuma:=0,@last_bill_no:='NULL' )rr,( SELECT *  from t_reocn_deduct  ORDER BY MERCHANT_NO,CURR_DATE ) hhh
# ============ 第三个示例





SELECT *  from t_reocn_deduct where LATE_DEDUCT_FLAG = 1



# ============ 第四个示例
SELECT  * FROM
(

select @tnum:=@tnum+1 as ttid,
@thisbill:=hhh.BILL_NO as ThisBillNo,
@last_bill_no  as LastBillNo,
	@lastnum:=@rownuma as LastNum, # 其实不需要这个
  IF( hhh.LATE_DEDUCT_FLAG = 0,@rownuma:=0, if( @last_bill_no != @thisbill, @rownuma:=1, @rownuma:=@rownuma+1 ) ) as ThisNum ,
  IF( hhh.LATE_DEDUCT_FLAG = 0,@firstdate:=NULL, if( @rownuma=1, @firstdate:=DATE_ADD(  STR_TO_DATE( hhh.CURR_DATE,'%Y%m%d'),INTERVAL -1 DAY), @firstdate ) )as FIRST_DAY ,
	hhh.BEGIN_LATE_DATE as BEGIN_LATE_DATE_T,
@last_bill_no:=hhh.BILL_NO as NEXT_Bill_NO,
hhh.LATE_DEDUCT_FLAG as FLAG,
hhh.*, rr.*  from
(select  @rownuma:=0,@tnum:=0,@firstdate:=NULL, @last_bill_no:='NULL' )rr,( SELECT *  from t_reocn_deduct  ORDER BY BILL_NO,CURR_DATE ) hhh


) sss
where
#sss.BILL_NO = '82010140200032999'
# sss.FLAG=1 and
sss.ThisNum >= 1
# ============ 第四个示例

