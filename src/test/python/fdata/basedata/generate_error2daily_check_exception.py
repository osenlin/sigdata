from testbase import testBase

class GenerateError2DailyCheckException(testBase):

    def test_stat_generate_error(self):
        sqltmp="""
            select trd_type,busi_date,count(1) error_count
            from odata.stock_generate_error
            where busi_date>='2017-03-16' and busi_date<='2017-04-14'
            group by trd_type,busi_date
        """

        df=self.sparkSession.sql(sqltmp)
        if testBase.logLevel!='debug':
            df.write.format("orc").mode("overwrite").saveAsTable("test.stock_generate_error_stat_1")



    def test_checkExceptionTypeByTrdType(self):

        sqlTmp="""
            select * from (
                select a.trd_type,a.busi_date,a.error_count,b.data_count,a.error_count-b.data_count diff_count from (
                    select trd_type,busi_date,count(1) error_count
                    from odata.stock_generate_error
                    where busi_date='{0}'
                    group by trd_type,busi_date) a
                full outer join (
                    select trd_type,busi_date,count(1) data_count
                    from fdata.stock_daily_check_exception
                    where busi_date='{0}' and exception_type='qty'
                    group by trd_type,busi_date
                ) b on a.busi_date=b.busi_date and a.trd_type=b.trd_type
            ) a where diff_count!=0
            """.format(self.caclData)

        df=self.sparkSession.sql(sqlTmp)
        if testBase.logLevel!='debug':
            for row in df.collect():
                self.assertEqual(
                    int(row['diff_count']),
                    0,
                    msg="trd_type[{0}],stock_generate_error[{1}],stock_daily_check_exception[{2}]".format(
                        row['trd_type'], row['error_count'], row['data_count']
                    )
                )

    def test_checkQty(self):
        sqlTmp="""
            select a.trade_id,a.secu_acc_id,a.prd_no,a.trd_type,a.busi_date,error_qty,pre_qty,trd_qty,now_qty,
                   pre_qty+trd_qty+error_qty-now_qty diff_count 
            from (
                select trade_id,secu_acc_id,prd_no,trd_type,busi_date,sum(nvl(error_qty,0)) error_qty
                from odata.stock_generate_error
                where busi_date='{0}'
                group by trade_id,secu_acc_id,prd_no,trd_type,busi_date) a
            inner join (
                select trade_id,secu_acc_id,prd_no,trd_type,busi_date,
                    sum(nvl(pre_qty,0)) pre_qty,
                    sum(nvl(trd_qty,0)) trd_qty,
                    sum(nvl(now_qty,0)) now_qty
                from fdata.stock_daily_check_exception
                where busi_date='{0}' and exception_type='qty'
                group by trade_id,secu_acc_id,prd_no,trd_type,busi_date
            ) b 
            on a.busi_date=b.busi_date 
            and  a.trade_id=b.trade_id  
            and a.secu_acc_id=b.secu_acc_id  
            and a.prd_no=b.prd_no 
            and a.trd_type=b.trd_type
        """.format(self.caclData)
        df=self.sparkSession.sql(sqlTmp)
        if testBase.logLevel != 'debug':
            for row in df.collect():
                self.assertEqual(
                        int(row['diff_count']),
                        0,
                        "trade_id[{0}],secu_acc_id[{1}],prd_no[{2}],diff_count[{3}]".format(
                            row['trade_id'], row['secu_acc_id'], row['prd_no'],row['diff_count'])
                )
