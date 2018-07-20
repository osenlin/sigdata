from testbase import testBase
from pyspark.sql import Row
from fdata.stock_profit.close_prd import ClosePrd,_traversal_unclose_prd


class ClosePrdTest(testBase):


    def setUp(self):

        self.busi_date='2017-03-16'
        self.funCloseTable="fdata.stock_close_prd_long_data"
        self.funUncloseTable="fdata.stock_unclose_prd_long_data"

    def test_printSaveCmd(self):
        print 'helo'
        pass
        #self.sparkSession.createDataFrame([{'trd_detail': [
        #    {'trd_date': '2017-03-16', 'trd_qty': 45800, 'trd_amt': 876154.0, 'timestamp': '1489662003'}],
        # 'total_out': 0.0, 'return': 0.0, 'prd_no': '1.600036', 'exception_label': 1, 'return_rate': 0.0,
        # 'trade_id': '10076', 'total_in': 876154.0, 'holding_term': 0, 'remain_qty': 45800, 'remain_val': 0.0,
        # 'secu_acc_id': '', 'busi_date': '2017-03-16', 'open_date': '2017-03-16'}],unCloseTableSchema).\
        #    write.format("ORC").saveAsTable("fdata.stock_unclose_prd_long_data", mode="append",partitionBy="busi_date")


    def test_traversal_unclose_prd(self):

        row1=Row(trade_id='10076', secu_acc_id='', prd_no='1.600036', open_date=None, trd_detail=None,
            crash_flow_trd_detail=[{'trd_date': '2017-03-16', 'trd_qty': '45800', 'trd_amt': '876154.0',
                                    'timestamp': '1489662003'}], remain_qty=None, remain_val=None)
        row1Return=_traversal_unclose_prd(row1,self.busi_date,self.funCloseTable,self.funUncloseTable)

        row2=Row(trade_id='10682', secu_acc_id='', prd_no='0.0', open_date='2017-03-15',
                 trd_detail=[{'return': '0', 'trd_date': '2017-03-15', 'trd_qty': '0', 'trd_amt': '6287600.0'}], crash_flow_trd_detail=None, remain_qty=0, remain_val=6287600.0)
        #row2Return =_traversal_unclose_prd(row2,self.busi_date,self.funCloseTable,self.funUncloseTable)

        row3=Row(trade_id='92811', secu_acc_id='', prd_no='0.0', open_date='2017-03-15',
                 trd_detail=[{'return': '0', 'trd_date': '2017-03-15', 'trd_qty': '0', 'trd_amt': '1042200.0'}],
                 crash_flow_trd_detail=[{'trd_date': '2017-03-16', 'trd_qty': '0', 'trd_amt': '20535.39', 'timestamp': '1489654800'}], remain_qty=0, remain_val=1062735.39)

        row3Return=_traversal_unclose_prd(row3,self.busi_date)
        #todo cash_flow_trd_qty<0

    def test_check_return(self):

        rf=self.sparkSession.sql("""
            select * from fdata.stock_close_prd_long_data
            where busi_date='2017-03-16'
        """)

        def func(x):
            sumReturn=0.0
            trd_detail=x.trd_detail if x.trd_detail is not None else []
            for item in trd_detail:
                if item.get('return') is None:
                    continue
                sumReturn+=float(item.get('return'))
            tmp=x.asDict()
            tmp['detail_return_sum']=sumReturn
            return tmp
        self.sparkSession.createDataFrame(rf.rdd.filter(lambda k:k.exception_label==0).map(lambda x:func(x))).\
            write.saveAsTable("checkdata.stock_close_prd_long_data_test",format="orc",mode="overwrite")

    def test_search_base_data(self):
        str=ClosePrd()._search_base_data("2017-03-16","2017-03-15","long_related","fdata.stock_unclose_prd_long_data")
        print("------------------command start-------------------")
        print("{0}{1}{2}".format("sparkSession.sql(\"\"\"", str, "\"\"\")"))
        print("------------------command end---------------------")

