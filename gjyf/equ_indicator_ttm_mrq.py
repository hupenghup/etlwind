#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import warnings
import datetime


sys.path.append("../")
from utils.conf import  wd,wd_cur,ai,  ai_cur
from utils.etl import ETL

warnings.filterwarnings('ignore')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

if len(sys.argv) == 1:
    dodate = (datetime.datetime.now()  - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
else:
    dodate = sys.argv[1]

table_name = 'equ_indicator_ttm_mrq'
file_name = 'equ_indicator_ttm_mrq'

sql="""
select 
replace(regexp_substr(s_info_windcode,'.*?\.'),'.')  as ticker,
to_date(ann_dt ,'yyyy-mm-dd')AS anno_dt,
to_date(report_period,'yyyy-mm-dd') AS report_period,
statement_type as report_type,
oper_rev_ttm as oper_rev_ttm,
s_fa_cost_ttm as cost_ttm,
s_fa_expense_ttm as expense_ttm,
s_fa_grossmargin_ttm as gross_margin_ttm,
s_fa_operateincome_ttm as operate_income_ttm,
s_fa_investincome_ttm as invest_income_ttm,
s_fa_op_ttm as op_ttm,
s_fa_ebt_ttm as ebt_ttm,
net_profit_ttm as net_profit_ttm,
net_profit_parent_comp_ttm as net_profit_parent_comp_ttm,
s_fa_gr_ttm as gr_ttm,
s_fa_gc_ttm as gc_ttm,
net_incr_cash_cash_equ_ttm as net_incr_cash_cash_equ_ttm,
net_cash_flows_oper_act_ttm as net_cash_flows_oper_act_ttm,
s_fa_investcashflow_ttm as invest_cashflow_ttm,
s_fa_financecashflow_ttm as finance_cashflow_ttm,
s_fa_asset_mrq as asset_mrq,
s_fa_debt_mrq as debt_mrq,
s_fa_totalequity_mrq as total_equity_mrq,
s_fa_equity_mrq as equity_mrq,
s_fa_netprofitmargin_ttm as net_profit_margin_ttm,
s_fa_grossprofitmargin_ttm as gross_profit_margin_ttm,
s_fa_expensetosales_ttm as expense_to_sales_ttm,
s_fa_profittogr_ttm as profit_to_gr_ttm,
s_fa_operateexpensetogr_ttm as operate_expense_to_gr_ttm,
s_fa_adminexpensetogr_ttm as admin_expense_to_gr_ttm,
s_fa_finaexpensetogr_ttm as fina_expense_to_gr_ttm,
s_fa_impairtogr_ttm as impair_to_gr_ttm,
s_fa_gctogr_ttm as gc_to_gr_ttm,
s_fa_optogr_ttm as op_to_gr_ttm,
s_fa_roa_ttm as roa_ttm,
s_fa_roa2_ttm as roa2_ttm,
s_fa_roe_ttm as roe_ttm,
s_fa_operateincometoebt_ttm as operate_income_to_ebt_ttm,
s_fa_investincometoebt_ttm as invest_income_to_ebt_ttm,
s_fa_nonoperateprofittoebt_ttm as non_operate_profit_to_ebt_ttm,
s_fa_salescashintoor_ttm as sales_cashin_to_or_ttm,
s_fa_ocftoor_ttm as ocf_to_or_ttm,
s_fa_ocftooperateincome_ttm as ocf_to_operate_income_ttm,
s_fa_salescashin_ttm as sales_cashin_ttm,
s_fa_operateexpense_ttm as operate_expense_ttm,
s_fa_adminexpense_ttm as admin_expense_ttm,
s_fa_finaexpense_ttm as fina_expense_ttm,
s_fa_expense as expense,
s_fa_nonoperateprofit_ttm as non_operate_profit_ttm,
s_fa_impairment_ttm as impairment_ttm,
s_fa_ebit_ttm as ebit_ttm,
s_fa_investcapital_mrq as invest_capital_mrq,
fa_roic_ttm as fa_roic_ttm,
s_stm_bsmrq as stm_bsmrq,
s_fa_nonopprofit_ttm as non_opprofit_ttm,
s_fa_prefinexp_op_ttm as pre_fin_exp_op_ttm,
s_fa_optoebt_ttm as op_to_ebt_ttm,
s_fa_noptoebt_ttm as nop_to_ebt_ttm,
s_fa_taxtoebt_ttm as tax_to_ebt_ttm,
s_fa_optoor_ttm as op_to_or_ttm,
s_fa_ebttoor_ttm as ebt_to_or_ttm,
s_fa_prefinexpoptoor_ttm as pre_fin_exp_op_to_or_ttm,
s_fa_netprofittoor_ttm as net_profit_to_or_ttm,
s_fa_prefinexpoptoebt_ttm as pre_finexpop_to_ebt_ttm,
s_fa_ocftoop_ttm as ocf_to_op_ttm,
roa_exclminintinc_ttm as roa_exclminintinc_ttm,
s_fa_debttoassets_mrq as debt_to_assets_mrq,
'wind' as src_channel,
1 as src_table,
object_id as src_object_id,
s_info_windcode as src_code,
opdate as src_opdate
from eterminal.AShareTTMhis
where to_char(opdate,'YYYY-MM-DD') >='%s'
""" % (dodate)

cols = """
ticker
,anno_dt
,report_period
,report_type
,oper_rev_ttm
,cost_ttm
,expense_ttm
,gross_margin_ttm
,operate_income_ttm
,invest_income_ttm
,op_ttm
,ebt_ttm
,net_profit_ttm
,net_profit_parent_comp_ttm
,gr_ttm
,gc_ttm
,net_incr_cash_cash_equ_ttm
,net_cash_flows_oper_act_ttm
,invest_cashflow_ttm
,finance_cashflow_ttm
,asset_mrq
,debt_mrq
,total_equity_mrq
,equity_mrq
,net_profit_margin_ttm
,gross_profit_margin_ttm
,expense_to_sales_ttm
,profit_to_gr_ttm
,operate_expense_to_gr_ttm
,admin_expense_to_gr_ttm
,fina_expense_to_gr_ttm
,impair_to_gr_ttm
,gc_to_gr_ttm
,op_to_gr_ttm
,roa_ttm
,roa2_ttm
,roe_ttm
,operate_income_to_ebt_ttm
,invest_income_to_ebt_ttm
,non_operate_profit_to_ebt_ttm
,sales_cashin_to_or_ttm
,ocf_to_or_ttm
,ocf_to_operate_income_ttm
,sales_cashin_ttm
,operate_expense_ttm
,admin_expense_ttm
,fina_expense_ttm
,expense
,non_operate_profit_ttm
,impairment_ttm
,ebit_ttm
,invest_capital_mrq
,fa_roic_ttm
,stm_bsmrq
,non_opprofit_ttm
,pre_fin_exp_op_ttm
,op_to_ebt_ttm
,nop_to_ebt_ttm
,tax_to_ebt_ttm
,op_to_or_ttm
,ebt_to_or_ttm
,pre_fin_exp_op_to_or_ttm
,net_profit_to_or_ttm
,pre_finexpop_to_ebt_ttm
,ocf_to_op_ttm
,roa_exclminintinc_ttm
,debt_to_assets_mrq
,src_channel
,src_table
,src_object_id
,src_code
,src_opdate
"""

unique_key = "report_period,src_code"

print(sql)
etl = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql,
          table_name=table_name,
          columns=cols,
          unique_key=unique_key)

# 加载数据
etl.dump_data(file_name)

# 写入数据
etl.import_data(file_name)

#etl.update_unique_id()
wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
