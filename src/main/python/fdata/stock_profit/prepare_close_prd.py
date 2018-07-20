# coding=utf-8

from fdata.sigmajob import sigmaSparkJob

class Prepare(sigmaSparkJob):

    sparkSession = sigmaSparkJob.setupSpark(None, "yarn", "prepare stock_profit")

    def __init__(self, caclDate):
        self.caclData = caclDate

    def process(self):
        # 初始化表结构
        sqlTmp = """
            select  trade_id,secu_acc_id,prd_no,open_date,close_date,
                   cast(holding_term as bigint ) holding_term,
                   cast(return as double) return,
                   cast(return_rate as double) return_rate,
                   cast(total_in as double) total_in,
                   cast(total_out as double) total_out,
                   trd_detail,
                   cast(exception_label as bigint) exception_label,
                   busi_date
            from (
                select 
                    trade_id,secu_acc_id,prd_no,busi_date,open_date, '' close_date,
                    holding_term,return,return_rate,total_in,total_out,trd_detail,exception_label
                from fdata.stock_unclose_prd_long_data
                where 1=0
            ) a
        """.format(self.caclData)
        df=self.sparkSession.sql(sqlTmp)
        df.write.format("ORC").saveAsTable("fdata.stock_close_prd_long_data",mode="overwrite",partitionBy="busi_date")


if __name__ == '__main__':
    Prepare("2017-03-15").process()
