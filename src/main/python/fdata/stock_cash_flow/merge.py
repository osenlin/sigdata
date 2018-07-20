# coding=utf-8
from fdata.sigmajob import SigmaSparkJob

class CashFlowMerge(SigmaSparkJob):

    def __init__(self, taskName):
        self.sparkSession = SigmaSparkJob.setupSpark(None, "yarn", taskName)

    def process(self, startdate=None,enddate=None):
        self.sparkSession.sql(
            " alter table fdata.stock_cash_flow add if not exists  partition(busi_date= '{}') ".format(enddate))
        self.sparkSession.sql("""
            insert overwrite table fdata.stock_cash_flow partition(busi_date= '{0}')
            select  
                trade_id,secu_acc_id,prd_no,
                concat_ws('-',collect_list(busi_flag_code)) busi_flag_code,
                concat_ws('-',collect_list(busi_flag_name)) busi_flag_name,
                sum(trd_qty) trd_qty,
                sum(case when trd_cash_flow >=0 then trd_qty else 0 end) pos_trd_qty,
                sum(case when trd_cash_flow <0 then trd_qty else 0 end) neg_trd_qty,
                sum(trd_cash_flow) trd_cash_flow,
                sum(case when trd_cash_flow >=0 then trd_cash_flow else 0 end) pos_cash_flow,
                sum(case when trd_cash_flow <0 then trd_cash_flow else 0 end) neg_cash_flow,
                sum(trd_cash_flow-trd_amt) trd_fee,
                max(cash_flow_modi_label) cash_flow_modi_label,
                trd_type
            from odata.stock_cash_flow_detail
            where busi_date = '{0}'
            group by trade_id,secu_acc_id,prd_no,trd_type
            """.format(enddate)
        )
