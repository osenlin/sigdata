# coding=utf-8
from fdata.sigmajob import SigmaSparkJob
from datetime import datetime, timedelta


class AccountCheck(SigmaSparkJob):
    """
    账目核对计算模块
    """


    def __init__(self, taskName):
        self.sparkSession = SigmaSparkJob.setupSpark(None, "yarn", taskName)

    def process(self,startdate=None,enddate=None):
        endDate = enddate
        beginDate =startdate

        self.sparkSession.sql(
            " alter table fdata.stock_daily_check_data add if not exists  partition(busi_date= '{0}') ".format(endDate))

        checkSqlTmp = """
            insert {5} table fdata.stock_daily_check_data partition(busi_date='{3}')
            select 
                ft.trade_id,ft.secu_acc_id,ft.prd_no,
                ft.pre_qty,
                ft.trd_qty,
                ft.now_qty,
                ft.pre_mkt_val,
                ft.now_mkt_val,
                ft.trd_cash_flow,
                ft.pos_cash_flow,
                ft.neg_cash_flow,
                ft.busi_flag_code,
                ft.return,
                ft.return_rate,
                ft.qty_exception,
                case when ft.return_rate>{7} and ft.return_rate<{8} then 0 else 1 end return_rate_exception,
                ft.trd_type
            from (
                select nvl(oa.trade_id,fb.trade_id) trade_id,nvl(oa.secu_acc_id,fb.secu_acc_id) secu_acc_id,nvl(oa.prd_no,fb.prd_no) prd_no,
                nvl(pre_qty,0) pre_qty,
                nvl(trd_qty,0) trd_qty,
                nvl(now_qty,0) now_qty,
                nvl(pre_mkt_val,0) pre_mkt_val,
                nvl(now_mkt_val,0) now_mkt_val,
                nvl(trd_cash_flow,0) trd_cash_flow,
                nvl(pos_cash_flow,0) pos_cash_flow,
                nvl(neg_cash_flow,0) neg_cash_flow,
                busi_flag_code,
                nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0) return,
                {0} return_rate,
                (case when nvl(pre_qty,0)+cast(nvl(trd_qty,0) as bigint)>= nvl(now_qty,0) and  nvl(pre_qty,0)+cast(nvl(trd_qty,0)  as bigint)<nvl(now_qty,0)+cast(1 as bigint) then 0 else 1 end)  qty_exception,
                0 return_rate_exception,
                '{6}' trd_type
                from (
                    select trade_id,secu_acc_id,prd_no,
                    sum(case when busi_date='{2}' then {1} else cast(0 as bigint) end) pre_qty,
                    sum(case when busi_date='{3}' then {1} else cast(0 as bigint) end) now_qty,
                    sum(case when busi_date='{2}' then mkt_val else cast(0 as bigint) end) pre_mkt_val,
                    sum(case when busi_date='{3}' then mkt_val else cast(0 as bigint) end) now_mkt_val
                    from {4}
                    -- pro='0.0'现金流 
                    where busi_date>='{2}' and busi_date<='{3}' and prd_no<>'0.0' 
                    group by trade_id,secu_acc_id,prd_no
                ) oa full join (select * from 
                    fdata.stock_cash_flow  
                    where busi_date='{3}' and trd_type='{6}') fb
                    on oa.trade_id=fb.trade_id
                    and oa.secu_acc_id=fb.secu_acc_id
                    and oa.prd_no=fb.prd_no
            ) ft 
            inner join fdata.stock_return_rate_range rg 
            on ft.prd_no=rg.prd_no and rg.busi_date= '{3}'
        """

        longCheckSql = checkSqlTmp.format("(nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0))/(nvl(pos_cash_flow,0)+nvl(pre_mkt_val,0))",
                                          "qty",
                                          beginDate,
                                          endDate,
                                          "odata.stock_asset_holding",
                                          "overwrite",
                                          "long_related",
                                          "long_lower_limit",
                                          "long_upper_limit")

        shortCheckSql = checkSqlTmp.format("(nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0))/(nvl(pos_cash_flow,0)-nvl(now_mkt_val,0))",
                                           "liab_qty",
                                           beginDate,
                                           endDate,
                                           "odata.stock_debt_holding",
                                           "into",
                                           "short_related",
                                           "short_lower_limit",
                                           "short_upper_limit")

        self.sparkSession.sql(longCheckSql);
        self.sparkSession.sql(shortCheckSql);


if __name__ == '__main__':

    end= '2017-03-28'
    start='2017-03-27'
    AccountCheck(start).process(start,end)
