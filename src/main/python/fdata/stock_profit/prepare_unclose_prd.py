# coding=utf-8

from fdata.sigmajob import sigmaSparkJob

class Prepare(sigmaSparkJob):

    sparkSession = sigmaSparkJob.setupSpark(None, "yarn", "prepare stock_profit")

    def __init__(self, caclDate):
        self.caclData = caclDate

    def process(self):

        sqlTmp = """
        select  trade_id,secu_acc_id,prd_no,busi_date open_date,
               cast(holding_term as bigint ) holding_term,
               cast(return as double) return,
               cast(return_rate as double) return_rate,
               cast(total_in as double) total_in,
               cast(total_out as double) total_out,
               cast(remain_qty as bigint) remain_qty,
               cast(remain_val as double) remain_val,
               trd_detail,
               cast(exception_label as bigint) exception_label,
               busi_date
        from (
            select 
                trade_id,secu_acc_id,prd_no,busi_date,0 holding_term,0.0 return,0.0 return_rate, mkt_val total_in,0.0 total_out,qty remain_qty,mkt_val remain_val,
                array(map('trd_date','{0}'   ,'trd_qty',qty,    'trd_amt',mkt_val,   'return',0)) trd_detail,
                 0 exception_label
            from odata.stock_asset_holding
            where busi_date='{0}'
            ) a
        """.format(self.caclData)
        self.sparkSession.sql(sqlTmp).write.format("orc").saveAsTable("fdata.stock_unclose_prd_long_data",
                                                                      mode="overwrite",partitionBy="busi_date")


if __name__ == '__main__':
    Prepare("2017-03-15").process()
