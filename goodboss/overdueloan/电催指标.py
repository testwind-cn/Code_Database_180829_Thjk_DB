# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 17:02:33 2018

"""

from sqlalchemy import create_engine
import pandas as pd


def c_rank(gp, outcomment='出催'):
    """
    给同一借据标记催案号
    """
    # 初始化
    gp.sort_index(inplace=True)
    result = pd.Series(1, index=gp.index)
    temp = gp.reset_index()
    idx = temp[temp[gp.name]==outcomment].index
    
    # 计算
    for i, v in enumerate(idx):
        if v == idx[-1] and v < len(temp):
            result[(v+1):len(temp)] = i+2
        else:
            result[(v+1):(idx[i+1]+1)] = i+2
    
    return(result)

def indi(gp):
    """
    计算指标
    """
    # 修正统计周期内的迁徙结果
    gp['迁徙结果'] = gp.groupby(['借据编号','催收次数'])['是否迁徙'].transform(lambda x:'有迁徙' if (x=='迁徙').any() else '无迁徙')
    
    temp1 = gp.drop_duplicates(subset=['借据编号', '催收次数']).groupby('逾期阶段').agg({'借据编号':[('累计委托户','count')], '委托金额':[('累计委托额','sum')]})
    
    temp21 = gp[gp.是否出催=='出催'].groupby('逾期阶段').agg({'借据编号':[('累计委托户','count')]})
    temp22 = gp.groupby('逾期阶段').agg({'回款金额':[('累计委托额','sum')]}).rename(columns={'回款金额':'委托金额'})
    temp2 = pd.concat([temp21, temp22], axis=1)
    
    temp31 = gp[gp.是否迁徙=='迁徙'].groupby('逾期阶段').agg({'借据编号':[('累计委托户','count')]})
    temp32 = gp[gp.迁徙结果=='有迁徙'].groupby('逾期阶段').agg({'委托金额':[('累计委托额','sum')]}) - \
             gp[gp.迁徙结果=='有迁徙'].groupby('逾期阶段').agg({'回款金额':[('累计委托额','sum')]}).rename(columns={'回款金额':'委托金额'})
    temp3 = pd.concat([temp31, temp32], axis=1)
    
    pct_out = (temp2/temp1).rename(columns={'累计委托户':'出催率', '累计委托额':'还款率'})
    pct_trans = (temp3/temp1).rename(columns={'累计委托户':'户数迁徙率', '累计委托额':'金额迁徙率'})
    
    # 181009 syzhang 更新输出出催户数/还款金融，迁徙户数/迁徙金额，委托户数/委托金额
    # result = pd.concat([pct_out, pct_trans],axis=1).unstack()
    temp2 = temp2.rename(columns = {'累计委托户':'出催户数', '累计委托额':'还款金额'})
    temp3 = temp3.rename(columns = {'累计委托户':'迁徙户数', '累计委托额':'迁徙金额'})
    result = pd.concat([pct_out, pct_trans, temp2, temp3, temp1],axis=1).unstack()
    
    result.index = result.index.droplevel(0)
    
    return(result)

if __name__=='__main__':
    # 输出目录
    outPath = "\\\\10.2.206.17\\XWRiskshare\\催收\\C-催收相关工作\\2.电催\\统计指标\\"
    
    # 连接数据库
    engine_oracle = create_engine("oracle+cx_oracle://thbl:thblserver@10.2.206.31:1521/POS", 
                                  connect_args={'encoding':'utf8', 'nencoding':'utf8'})
    engine_mysql = create_engine("mysql+pymysql://ttr_dev1:ttr_dev1@10.2.206.28/thbl", 
                                 connect_args={'charset':'utf8'})

    # 读取数据，默认至昨天
    sql = """
        SELECT 
          s.SID 批次号, c.DB 借据编号, c.MCD 商户号, tl.ACTIVATE_DATE 借款日期, c.CORP 催收公司, c.RLEVEL 催收阶段, tff.DEBIT_AMT 回款金额, s.UPDATE_DT 更新日期 
        FROM
        	C_SEPLOG s
        	LEFT JOIN C_COLLECTION c ON s.CID = c.CID
          LEFT JOIN TM_LOAN tl ON c.DB = tl.DUE_BILL_NO
        	LEFT JOIN ( SELECT MCHT_CD, TXN_DATE, SUM( DEBIT_AMT ) DEBIT_AMT FROM TM_FUND_FLOW_HST GROUP BY MCHT_CD, TXN_DATE ) tff ON c.MCD = tff.MCHT_CD 	AND s.UPDATE_DT = tff.TXN_DATE 
        WHERE
        	s.UPDATE_DT < %s
        ORDER BY
        	s.UPDATE_DT
        """
    db_collection = pd.read_sql(sql, engine_mysql, params=((pd.datetime.today()-pd.tseries.offsets.Day(1)).strftime('%Y-%m-%d'),))
    db_collection['更新日期'] = pd.to_datetime(db_collection['更新日期']) # 方便作为时间序列处理

    sql = """
        SELECT
        	DB_NO 借据编号,
          OVERDUE_DAYS 逾期天数,
        	( SP_PRINCIPAL + SP_FEE + SP_FEE1 + SP_FEE2 ) 委托金额,
          DATA_DT 更新日期
        FROM
        	S6700
        """
    db_s6700 = pd.read_sql(sql, engine_oracle)
    
    # 合并数据
    data = db_collection.merge(
            db_s6700.assign(更新日期=db_s6700.更新日期+pd.DateOffset(days=1)), how='left', left_on=['借据编号', '更新日期'], right_on=['借据编号', '更新日期']).rename(columns={'逾期天数':'T日逾期天数'}).merge(
            db_s6700[['借据编号', '更新日期', '逾期天数']], how='left', left_on=['借据编号', '更新日期'], right_on=['借据编号', '更新日期']).rename(columns={'逾期天数':'T+1日逾期天数'}).set_index('更新日期').sort_index()

    # 补充字段
    data['是否出催'] = data['T+1日逾期天数'].map(lambda x:'出催' if x==0 else '在催')
    data['是否迁徙'] = data.apply(lambda x: '迁徙' if ((x['T日逾期天数']<31 and x['T+1日逾期天数']>=31) or 
                                                      (x['T日逾期天数']<61 and x['T+1日逾期天数']>=61) or
                                                      (x['T日逾期天数']<91 and x['T+1日逾期天数']>=91)) else '停留', axis=1)
    #data['新老商户'] = data['借款日期'].map(lambda x: '老商户' if x < pd.datetime(2016, 9, 1).date() else '新商户')
    data['逾期阶段'] = data['T日逾期天数'].map(lambda x: 'M1' if x < 31 else ('M2' if x < 61 else 'M3'))
    data['催收次数'] = data.groupby('借据编号')['是否出催'].transform(c_rank)
    data['催收结果'] = data.groupby(['借据编号','催收次数'])['是否出催'].transform(lambda x:'成功' if x[-1]=='出催' else '失败')
    data['迁徙结果'] = data.groupby(['借据编号','催收次数'])['是否迁徙'].transform(lambda x:'有迁徙' if (x=='迁徙').any() else '无迁徙')
    
    #%% 按催收公司、月份统计（MonthEnd默认的n=1）
    result = data.groupby(['催收公司', data.index+pd.tseries.offsets.MonthEnd(n=0)]).apply(indi).unstack([0,2,-1]).fillna(0)
    result.to_excel(outPath + pd.datetime.today().strftime('%Y%m%d') + '催收统计.xlsx')
    
     #%% 测试
    test=data[(data.催收公司=='深圳分公司') & ((data.index==pd.Timestamp('2018-03-01 00:00:00')) | (data.index==pd.Timestamp('2018-03-02 00:00:00')))]
    
    for idx, gp in test.groupby(['催收公司', test.index+pd.tseries.offsets.MonthEnd(n=0)]):
        # 修正统计周期内的迁徙结果
        gp['迁徙结果'] = gp.groupby(['借据编号','催收次数'])['是否迁徙'].transform(lambda x:'有迁徙' if (x=='迁徙').any() else '无迁徙')
        
        temp1 = gp.drop_duplicates(subset=['借据编号', '催收次数']).groupby('逾期阶段').agg({'借据编号':[('累计委托户','count')], '委托金额':[('累计委托额','sum')]})
        
        temp21 = gp[gp.是否出催=='出催'].groupby('逾期阶段').agg({'借据编号':[('累计委托户','count')]})
        temp22 = gp.groupby('逾期阶段').agg({'回款金额':[('累计委托额','sum')]}).rename(columns={'回款金额':'委托金额'})
        temp2 = pd.concat([temp21, temp22], axis=1)
       
        temp31 = gp[gp.是否迁徙=='迁徙'].groupby('逾期阶段').agg({'借据编号':[('累计委托户','count')]})
        temp32 = gp[gp.迁徙结果=='有迁徙'].groupby('逾期阶段').agg({'委托金额':[('累计委托额','sum')]}) - \
                 gp[gp.迁徙结果=='有迁徙'].groupby('逾期阶段').agg({'回款金额':[('累计委托额','sum')]}).rename(columns={'回款金额':'委托金额'})
        temp3 = pd.concat([temp31, temp32], axis=1)
        
        pct_out = (temp2/temp1).rename(columns={'累计委托户':'出催率', '累计委托额':'还款率'})
        pct_trans = (temp3/temp1).rename(columns={'累计委托户':'户数迁徙率', '累计委托额':'金额迁徙率'})
        result = pd.concat([pct_out, pct_trans],axis=1).unstack()
        result.index = result.index.droplevel(0)
    
    
