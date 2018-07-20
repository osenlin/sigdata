# coding=utf-8
from fdata.sigmajob import SigmaSparkJob
from datetime import datetime, timedelta


class CheckExceptionDetail(SigmaSparkJob):

    def __init__(self, taskName):
        self.sparkSession = SigmaSparkJob.setupSpark(None, "yarn", taskName)

    def process(self,startdate=None,enddate=None):

        self.sparkSession.sql(
            " alter table fdata.stock_daily_check_exception add if not exists  partition(busi_date= '{0}') ".format(enddate))

        sqlTmp = """
            insert overwrite table fdata.stock_daily_check_exception partition(busi_date='{0}')
            select  trade_id,secu_acc_id,prd_no,pre_qty,trd_qty,now_qty,pre_mkt_val,
                    now_mkt_val,trd_cash_flow,pos_cash_flow,busi_flag_code,
                    case when qty_exception =1 and return_rate_exception=0 then 'qty' 
                        when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                        else 'both' end exception_type,
                    trd_type
            from fdata.stock_daily_check_data
            where (qty_exception<>0 or return_rate_exception<>0) and busi_date='{0}'
        """.format(enddate)

        self.sparkSession.sql(sqlTmp);


if __name__ == '__main__':
    start= 20170214
    end = 20170225
    for x in xrange(start,end):
        print "--------------strat %s-------------" % x
        caclday=datetime.strptime(str(x),"%Y%m%d").strftime("%Y-%m-%d")
        CheckExceptionDetail(caclday).process()
