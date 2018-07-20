# coding=utf-8
from fdata.sigmajob import SigmaSparkJob
from datetime import datetime



class CheckExceptionStat(SigmaSparkJob):
    """

    """
    def __init__(self, taskName):
        self.sparkSession = SigmaSparkJob.setupSpark(None, "yarn", taskName)

    def process(self,startdate=None,enddate=None):

        sqlPrePartition=" alter table fdata.stock_daily_check_report add if not exists  partition(busi_date= '{0}') "\
            .format(enddate)
        self.sparkSession.sql(sqlPrePartition)

        sqlTmp="""
            insert overwrite table fdata.stock_daily_check_report partition(busi_date= '{0}')
            select 
                exception_pv,exception_uv,
                exception_pv/b.all_num exception_pv_rate,
                exception_uv/b.all_user exception_uv_rate,
                max_return,min_return,
                max_return_rate,min_return_rate,
                exception_type
            from (
                select 
                    count(1) exception_pv,
                    count(distinct trade_id) exception_uv,
                    max(return) max_return,
                    min(return) min_return,
                    max(return_rate) max_return_rate,
                    min(return_rate) min_return_rate,
                    case when qty_exception =1 and return_rate_exception=0 then 'qty' 
                         when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                         else 'both' end exception_type
                from fdata.stock_daily_check_data 
                where  busi_date= '{0}' and (qty_exception<>0 or return_rate_exception<>0)
                group by case when qty_exception =1 and return_rate_exception=0 then 'qty' 
                              when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                              else 'both' end 
            ) a cross join (
                select count(1) all_num,count(distinct trade_id) all_user
                from fdata.stock_daily_check_data 
                where busi_date= '{0}'
            ) b
        """.format(enddate)

        self.sparkSession.sql(sqlTmp)

if __name__ == '__main__':

    start= 20170214
    end = 20170225
    for x in xrange(start,end):
        print "--------------strat %s-------------" % x
        caclday=datetime.strptime(str(x),"%Y%m%d").strftime("%Y-%m-%d")
        CheckExceptionStat(caclday).process()