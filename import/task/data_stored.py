import pandas as pd
import os
import psycopg2
import json
import logging
import datetime
from psycopg2 import extras as ex
import numpy as np
import sys
import psutil
import os
import datetime
info = psutil.virtual_memory()
# 期末 是 01   期初 是 00
# 线程
from threading import Thread
from multiprocessing import Barrier, Lock, Process
from queue import Queue
from sqlalchemy import create_engine
import uuid


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BMS.settings")
import django
django.setup()
# 初始化参数和链接

# 容器
result_a = []
result_b = []
result_c = []
q = Queue()
# 计数器
num = 0
sum = 0
count = 0
# 初始化数据库链接
engine = psycopg2.connect(database="surexing3_test", user="postgres", password="761212", host="212.129.149.219", port="7006")

conn = create_engine('postgresql+psycopg2://postgres:761212@212.129.149.219:7006/surexing3_test',echo=True,client_encoding='utf8')


# 配置日志
logger = logging.getLogger('Submit_audit')



# 计算函数
def calculate(x,df_company_code):
    global num,sum,result_a,count,infos,result_b,result_c,conn,engine


    try:
        company_code = df_company_code[df_company_code['stock_code'] == x['stock_code']]['company_code'].iloc[0]
    except Exception:
        company_code = ''
        # company_code = str(uuid.uuid1())
        # try:
        #     df = pd.DataFrame([{'S_or_B_code':x['stock_code'],'company_code_id':company_code,'publish_type':'新增企业',
        #                         'S_or_B_name':company_name[company_name['债券代码'] == x[0]]['债券简称'].iloc[0]}])
        # except IndexError:
        #     df = pd.DataFrame([{'S_or_B_code': x['stock_code'], 'company_code_id': company_code, 'publish_type': '企业债公司',
        #           'S_or_B_name': 'ERR'}])
        # pd.io.sql.to_sql(df, name='companySearch_stockorbindinfo', con=conn, if_exists='append',
        #                  index=None)


    # 标准json
    json_strand = [
        {
            "code": "FB",
            "name": "资产负债表",
            "items": {
                "item": {
                    "stock": 0,
                    "lt_loan": 0,
                    "st_loan": 0,
                    "goodwill": 0,
                    "payable_tax": 0,
                    "assets_total": 0,
                    "fixed_assets": 0,
                    "payable_bond": 0,
                    "legal_reserve": 0,
                    "monetary_fund": 0,
                    "payable_notes": 0,
                    "foreign_capita": 0,
                    "payable_others": 0,
                    "payable_salary": 0,
                    "permanent_debt": 0,
                    "treasury_stock": 0,
                    "advance_payment": 0,
                    "capital_reserve": 0,
                    "deferred_income": 0,
                    "liability_total": 0,
                    "paid_in_capital": 0,
                    "preferred_stock": 0,
                    "receivable_bill": 0,
                    "special_payment": 0,
                    "special_reserve": 0,
                    "national_capital": 0,
                    "other_receivable": 0,
                    "payable_accounts": 0,
                    "payable_dividend": 0,
                    "payable_interest": 0,
                    "personal_capital": 0,
                    "biological_assets": 0,
                    "corporate_capital": 0,
                    "fda_raw_materials": 0,
                    "immaterial_assets": 0,
                    "collective_capital": 0,
                    "da_deferred_assets": 0,
                    "nc_assets_one_year": 0,
                    "nc_liability_total": 0,
                    "oil_and_gas_assets": 0,
                    "other_nc_liability": 0,
                    "owner_equity_total": 0,
                    "payment_in_advance": 0,
                    "deferred_tax_assets": 0,
                    "estimated_liability": 0,
                    "lt_payable_accounts": 0,
                    "lt_prepaid_expenses": 0,
                    "nlf_land_use_rights": 0,
                    "public_welfare_fund": 0,
                    "receivable_accounts": 0,
                    "receivable_interest": 0,
                    "current_assets_total": 0,
                    "fda_finished_product": 0,
                    "lt_equity_investment": 0,
                    "other_current_assets": 0,
                    "other_interest_tools": 0,
                    "receivable_dividends": 0,
                    "undistributed_profit": 0,
                    "da_fixed_asset_repair": 0,
                    "engineering_materials": 0,
                    "nc_liability_one_year": 0,
                    "deferred_tax_liability": 0,
                    "investment_real_estate": 0,
                    "lt_receivable_accounts": 0,
                    "construction_in_process": 0,
                    "current_liability_total": 0,
                    "development_expenditure": 0,
                    "other_current_liability": 0,
                    "saleable_finance_assets": 0,
                    "accumulated_depreciation": 0,
                    "fixed_assets_liquidation": 0,
                    "held_maturity_investment": 0,
                    "liabilities_equity_total": 0,
                    "non_current_assets_total": 0,
                    "other_non_current_assets": 0,
                    "trading_financial_assets": 0,
                    "fc_translation_difference": 0,
                    "trans_financial_liability": 0,
                    "equity_belonging_to_parent": 0,
                    "impairment_of_fixed_assets": 0,
                    "other_comprehensive_income": 0,
                    "da_fixed_assets_improve_out": 0,
                    "surplus_public_accumulation": 0,
                    "minority_shareholders_rights": 0,
                    "net_loss_current_assets_todo": 0,
                    "supplementary_working_capital": 0,
                    "nlf_net_loss_fixed_assets_todo": 0,
                    "original_price_of_fixed_assets": 0,
                    "collective_legal_person_capital": 0,
                    "fda_financial_derivative_assets": 0,
                    "da_special_reserves_of_materials": 0,
                    "derivative_financial_liabilities": 0,
                    "state_owned_legal_person_capital": 0
                }
            }
        },
        {
            "code": "FP",
            "name": "利润表",
            "items": {
                "item": {
                    "sales_tax": 0,
                    "income_tax": 0,
                    "net_margin": 0,
                    "profit_total": 0,
                    "gain_on_others": 0,
                    "nc_assets_loss": 0,
                    "cost_in_business": 0,
                    "operating_profit": 0,
                    "selling_expenses": 0,
                    "interest_expenses": 0,
                    "operating_receipt": 0,
                    "earnings_per_share": 0,
                    "financial_expenses": 0,
                    "nonbusiness_income": 0,
                    "gain_on_investments": 0,
                    "management_expenses": 0,
                    "assets_impairment_loss": 0,
                    "nonbusiness_expenditure": 0,
                    "basic_earnings_per_share": 0,
                    "diluted_earnings_per_share": 0,
                    "changes_in_fair_value_gains": 0
                }
            }
        },
        {
            "code": "FC",
            "name": "现金流量表",
            "items": {
                "item": {
                    "loan": 0,
                    "other_income": 0,
                    "debt_expenses": 0,
                    "other_expenses": 0,
                    "assets_expenses": 0,
                    "equity_earnings": 0,
                    "financing_total": 0,
                    "ordinary_income": 0,
                    "emploee_expenses": 0,
                    "financing_income": 0,
                    "tax_reimbursement": 0,
                    "financing_expenses": 0,
                    "subcompany_payment": 0,
                    "cash_goods_increase": 0,
                    "investment_expenses": 0,
                    "purchasing_expenses": 0,
                    "disinvestment_income": 0,
                    "exchange_rate_effect": 0,
                    "financing_net_income": 0,
                    "various_tax_expenses": 0,
                    "allot_shares_expenses": 0,
                    "investment_net_income": 0,
                    "management_net_income": 0,
                    "deal_assets_net_income": 0,
                    "init_cash_goods_balance": 0,
                    "investment_income_total": 0,
                    "last_cash_goods_balance": 0,
                    "management_income_total": 0,
                    "other_investment_income": 0,
                    "absorb_investment_income": 0,
                    "financing_expenses_total": 0,
                    "investment_expenses_total": 0,
                    "management_expenses_total": 0,
                    "deal_subcompany_net_income": 0,
                    "related_investment_expenses": 0
                }
            }
        }
    ]

    report_year = x[1].split('-')[0]

    report_time = x[1].split('-')[1]


    if report_time == '03':
        report_time = '03'
    elif report_time == '06':
        report_time = '02'
    elif report_time == '09':
        report_time = '01'
    elif report_time == '12':
        report_time = '00'
    elif report_time != '03' and report_time != '06' and report_time != '09' and report_time != '12':
        report_time = '000'
    report_type = x[2]
    if report_type == 'A':
        report_type = '00'
    elif report_type == 'B':
        report_type = '01'



    for i in json_strand[0]['items']['item'].keys():
        if i in x.index.tolist():
            json_strand[0]['items']['item'][i] = float(x[i])
    for i in json_strand[1]['items']['item'].keys():
        if i in x.index.tolist():
            json_strand[1]['items']['item'][i] = float(x[i])
    for i in json_strand[2]['items']['item'].keys():
        if i in x.index.tolist():
            json_strand[2]['items']['item'][i] = float(x[i])

    json_financial_statement = json.dumps(json_strand, ensure_ascii=False)
    json_business_data = json.dumps(datetime.datetime.now().strftime('%Y-%m-%d'))

    if len(result_a) >= 5000:
        result_b.append((report_year,report_time,report_type,json_financial_statement,company_code,'01',json_business_data))
        result_b.append(((str(int(report_year) + 1)), report_time, report_type, json_financial_statement, company_code, '00',json_business_data))
    else:
        result_a.append((report_year,report_time,report_type,json_financial_statement,company_code,'01',json_business_data))
        result_a.append(((str(int(report_year) + 1)), report_time, report_type, json_financial_statement, company_code, '00',json_business_data))

    num += 1
    sum += 1

    if num == 10000:
        logger.info(' 已经计算了：%s' % (num))
        # storage()
        num = 0

    if sum % 10000 == 0:
        one = Process(target=storage,args=(result_a,))
        two = Process(target=storage,args=(result_b,))
        one.start()
        two.start()
        one.join()
        two.join()
        result_a = []
        result_b = []

# 存储函数
def storage(x):

    engine = psycopg2.connect(database="surexing3_test", user="postgres", password="761212", host="212.129.149.219", port="7006")

    cursor = engine.cursor()

    sql = ' insert into "companySearch_listedcfinancialdada"(report_year,report_time,report_type,json_financial_statement,company_code_id,report_period,json_business_data) values (%s,%s,%s,%s,%s,%s,%s)'

    cursor.executemany(sql,x)


    engine.commit()

    cursor.close()
    engine.close()

    logger.info('{0} - {1}'.format(datetime.datetime.now(), '存储完成'))



def go(file_1,file_2,file_3):
    logger.info(' 数据处理开始')
    # 资产负债表
    balance = pd.read_excel(file_1)
    columns_list = balance.columns.tolist()
    lists = map(lambda x:x[:-1], balance.iloc[0].tolist())
    result = dict(zip(columns_list, lists))
    balance.rename(columns=result, inplace=True)
    balance.drop([0, 1], axis=0, inplace=True)
    balance.reset_index(drop=True, inplace=True)
    print(len(balance))
    # 现金流量表
    cash_flow = pd.read_excel(file_2)
    columns_list = cash_flow.columns.tolist()
    lists = map(lambda x: x[:-1], cash_flow.iloc[0].tolist())
    result = dict(zip(columns_list, lists))
    cash_flow.rename(columns=result, inplace=True)
    cash_flow.drop([0, 1], axis=0, inplace=True)
    cash_flow.reset_index(drop=True, inplace=True)
    print(len(cash_flow))
    # 利润表
    profit = pd.read_excel(file_3)
    columns_list = profit.columns.tolist()
    lists = map(lambda x: x[:-1], profit.iloc[0].tolist())
    result = dict(zip(columns_list, lists))
    profit.rename(columns=result, inplace=True)
    profit.drop([0, 1], axis=0, inplace=True)
    profit.reset_index(drop=True, inplace=True)
    print(len(profit))
    # print(balance, profit, cash_flow)
    # 中英文对照表
    chinese_to_eng_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'task\chinese_to_eng.csv')
    print(chinese_to_eng_path)
    chines_english = pd.read_csv(chinese_to_eng_path,header = None,sep=',',encoding='gbk')
    chines_english.columns = ['英文','中文']


    logger.info('报表读取完毕')


    # 合并三张报表
    df_result = pd.merge(profit,balance,on=['证券代码','会计期间'],how='left',copy=False)
    df_result = pd.merge(df_result,cash_flow,on=['证券代码','会计期间'],how='left',copy=False)
    # df_result = pd.merge(df_result, company_name, on=['证券代码'], how='left', copy=False)
    # print(df_result.iloc[0])
    logger.info('报表合并完毕')

    # 数据整理
    df_columns = pd.DataFrame(df_result.columns)

    df_columns.rename(columns = {0:'中文'},inplace=True)

    df_columns = pd.merge(df_columns,chines_english,how='left',validate="1:1",copy=False)
    wa = df_columns['英文'].tolist()
    wa[0] = 'stock_code'
    wa[1] = 'year'
    df_result.columns = wa

    # df_result[['stock_code']] = df_result[['stock_code']].astype(str)
    # df_result = pd.merge(df_result,company_code,how = 'left',copy=False)

    logger.info(' 数据整理完毕')



    # 开始循环计算
    del balance
    del profit
    del cash_flow
    del chines_english
    del df_columns

    df_result.fillna(0, inplace=True)
    print(u'内存使用：', psutil.Process(os.getpid()).memory_info().rss)

    # stock_list = list(set(df_result['stock_code'].tolist()))

    # sql = 'select * from "companySearch_stockorbindinfo" '
    # df_company_code = pd.read_sql_query(sql, engine)
    # df_company_code.drop(['id', 'publish_type'], axis=1, inplace=True)
    # df_company_code.rename(
    #     columns={'S_or_B_code': 'stock_code', 'S_or_B_name': 'company_name', 'company_code_id': 'company_code'},
    #     inplace=True)
    # old_stock_code_list =  df_company_code['stock_code'].tolist()
    # new_stock_code_list = []
    # for i in stock_list:
    #     if i not in old_stock_code_list:
    #         new_stock_code_list.append({'S_or_B_code':i,'S_or_B_name':'err','company_code_id':str(uuid.uuid1()),'publish_type':'new'})


    # df_new_stock_code_list = pd.DataFrame(new_stock_code_list)
    # print(len(df_new_stock_code_list))

    # pd.io.sql.to_sql(df_new_stock_code_list, name='companySearch_stockorbindinfo', con=conn, if_exists='append',
    #                  index=None)

    sql = 'select * from "companySearch_stockorbindinfo" '
    df_company_code = pd.read_sql_query(sql, engine)
    df_company_code.drop(['id', 'publish_type'], axis=1, inplace=True)
    df_company_code.rename(
        columns={'S_or_B_code': 'stock_code', 'S_or_B_name': 'company_name', 'company_code_id': 'company_code'},
        inplace=True)

    logger.info(' 开始计算，共需计算：%s' % (len(df_result)))
    df_result.apply(calculate, axis=1,args = (df_company_code,))
    # print(len(df_result))
    # 开始存储
    if result_a != []:
        storage(result_a)
    if result_b != []:
        storage(result_b)
    if result_c != []:
        storage(result_c)



    logger.info(' 计算完毕')


