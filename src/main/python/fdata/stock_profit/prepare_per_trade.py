# coding=utf-8
from fdata.sigmajob import sigmaSparkJob


class PreparePerTrade(sigmaSparkJob):
    sparkSession = sigmaSparkJob.setupSpark(None, "yarn", "app_account_check")

    def __init__(self, caclDate):
        self.caclData = caclDate

    def process(self):
        # self.sparkSession.sql(" alter table fdata.stock_daily_check_data add if not exists  partition(busi_date= '{
        # 0}') ".format(endDate)) debug 需要将配置项注释掉
        self.sparkSession.conf.set("hive.exec.dynamic.partition", "true")
        self.sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        sqlTmp = """
                insert into table fdata.stock_unclose_trade_cal partition(busi_date)
                select
                    trade_id,secu_acc_id,prd_no,busi_date
                    array(map('open_date',busi_date,
                              'orig_trd_qty',qty,
                              'orig_trd_amt',mkt_val,
                              'trd_qty',qty,
                              'trd_amt',mkt_val,
                              'close_amt',0
                              'unclose_qty',0
                              'unclose_amt',0
                              'weighted_term',0
                              'exception_label',0
                              'return',0)
                    ) open_detail
                from odata.stock_asset_holding
                where busi_date='{0}'
        """.format(self.caclData)
        df = self.sparkSession.sql(sqlTmp);


if __name__ == '__main__':
    PreparePerTrade("2017-02-14").process()
