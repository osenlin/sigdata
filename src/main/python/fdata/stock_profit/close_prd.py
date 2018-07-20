# coding=utf-8
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from fdata.sigmajob import sigmaSparkJob
from datetime import datetime, timedelta

logLevel = 'info'


def _traversal_unclose_prd(row, busiDate):
    xAllQty = 0
    yAllAmt = 0.0
    totalIn = 0.0
    totalOut = 0.0
    aTrdDetail = row.trd_detail if row.trd_detail is not None else []
    newTrdDetail = aTrdDetail
    for trdDetailItem in aTrdDetail:
        trdQty = long(trdDetailItem['trd_qty'])
        trdAmt = float(trdDetailItem['trd_amt'])
        if trdQty >= 0:
            xAllQty += trdQty
            yAllAmt += trdAmt
            totalIn += trdAmt
        else:
            xAllQty += trdQty
            yAllAmt += ((xAllQty + trdQty) / xAllQty) * yAllAmt if xAllQty != 0 else trdAmt
            totalOut += trdAmt
    bTrdDetail = row.crash_flow_trd_detail if row.crash_flow_trd_detail is not None else []

    for crashFlowTrdDetailItem in bTrdDetail:
        CashDetailtrdQty = long(crashFlowTrdDetailItem['trd_qty'])
        CashDetailtrdAmt = float(crashFlowTrdDetailItem['trd_amt'])
        if CashDetailtrdQty >= 0:
            xAllQty += CashDetailtrdQty
            yAllAmt += CashDetailtrdAmt
            totalIn += CashDetailtrdAmt
        else:
            avgAmt = yAllAmt / xAllQty if xAllQty != 0 else 0
            xAllQty += CashDetailtrdQty
            yAllAmt += ((xAllQty + CashDetailtrdQty) / xAllQty) * yAllAmt if xAllQty != 0 else CashDetailtrdAmt
            totalOut += CashDetailtrdAmt
            crashFlowTrdDetailItem['return']=avgAmt * CashDetailtrdQty
    newTrdDetail += bTrdDetail

    strOpenDate = row.open_date
    openDate = datetime.strptime(strOpenDate if strOpenDate is not None and len(strOpenDate) == 10 else busiDate,'%Y-%m-%d')
    holdingTerm = (datetime.strptime(busiDate, '%Y-%m-%d') - openDate).days
    # filed1= return
    returnMoney = totalOut + yAllAmt - totalIn
    # filed1 =return_rate
    returnMoneyRate = returnMoney / totalIn if totalIn != 0 else 0
    dataModel = {
        'trade_id': str(row.trade_id),
        'secu_acc_id': str(row.secu_acc_id),
        'prd_no': str(row.prd_no),
        'open_date': openDate.strftime('%Y-%m-%d'),
        'holding_term': holdingTerm,
        'return': returnMoney,
        'return_rate': returnMoneyRate,
        'total_in': totalIn,
        'total_out': abs(totalOut),
        'trd_detail': newTrdDetail,
        #todo 还有其他判断条件
        'exception_label': 1 if row.open_date is None or row.remain_qty != xAllQty else 0,
        'busi_date': busiDate
    }
    TrueOrFalseClose = True
    if xAllQty != 0:
        dataModel.update({
            u'remain_qty': xAllQty,
            u'remain_val': float(row.remain_val if row.remain_val is not None else 0.0),
        })
        TrueOrFalseClose = False
    else:
        dataModel.update({
            u'close_date': busiDate
        })
    return (TrueOrFalseClose, dataModel)

def save_data(spark, database, table, busi_date, data):
    """
    将计算结果保存到HIVE
    """
    spark.sql("create database if not exists %s" % database)
    spark.sql("use %s" % database)
    tables = [i.name for i in spark.catalog.listTables()]
    if table in tables:
        _df = spark.sql("select * from %s.%s" % (database, table))
        # 保证数据的schema和已有HIVE表的schema一致
        data = spark.createDataFrame(data.select(_df.columns).rdd, _df.schema)
        data = data.repartition(spark._sc.defaultParallelism)
        # 如果分区上已有数据，则覆写
        spark.sql("alter table %s.%s drop if exists partition(busi_date='%s')"
                  % (database, table, busi_date))
        data.write.saveAsTable(
            "%s.%s" % (database, table), format="orc",
            partitionBy="busi_date", mode="append")
    else:
        raise Exception("please create table before save,create table must be use spark api")

class ClosePrd(sigmaSparkJob):
    """
    记录每支清仓股票的收益情况和交易明细
    """
    sparkSession = sigmaSparkJob.setupSpark(None, "yarn", "close_prd_calculate")

    def __init__(self, caclDate=None):
        self.caclData = caclDate
        pass

    def _search_base_data(self, T1Date, T2Date, funTrdType, funUncloseTable,database):
        sqlTmp = """
                select nvl(a.trade_id,b.trade_id) trade_id, nvl(a.secu_acc_id,b.secu_acc_id) secu_acc_id,nvl(a.prd_no,b.prd_no) prd_no,
                       open_date,a.trd_detail,b.trd_detail crash_flow_trd_detail,
                       c.qty remain_qty,c.mkt_val remain_val
                from (
                  select * from {4}.{3} where busi_date='{1}' 
                ) a 
                    full outer join (
                        select trade_id,secu_acc_id,prd_no,collect_list(trd_detail_item) trd_detail from(
                            select  trade_id,secu_acc_id,prd_no,
                                    (str_to_map(concat(
                                        'trd_qty:',trd_qty,
                                        ',trd_amt:',trd_amt,
                                        ',trd_date:',busi_date,'',
                                        ',timestamp:',timestamp
                                    ),",",":")) trd_detail_item,
                                    row_number() over(partition by trade_id,secu_acc_id,prd_no order by timestamp asc) rank
                            from odata.stock_cash_flow_detail
                            where trd_type='{2}' and busi_date='{0}'
                        ) a
                        group by trade_id,secu_acc_id,prd_no
                ) b 
                    on  a.trade_id=b.trade_id  and a.secu_acc_id=b.secu_acc_id  and a.prd_no=b.prd_no 
                    full outer join (
                    select * from odata.stock_asset_holding where  busi_date='{0}'
                 ) c 
                    on a.trade_id=c.trade_id  and a.secu_acc_id=c.secu_acc_id  and a.prd_no=c.prd_no
                where a.trade_id is not  NULL or b.trade_id is not NULL 
        """.format(T1Date, T2Date, funTrdType, funUncloseTable,database)
        return sqlTmp

    def process(self):
        self._sub_process('long_related','fdata', 'stock_unclose_prd_long_data', 'stock_close_prd_long_data')
        #self._sub_process('short_related', 'fdata.stock_unclose_prd_short_data', 'fdata.stock_close_prd_short_data')

    def _sub_process(self, funTrdType,database, TableUnClose, TableClose):

        T1Date = self.caclData
        T2Date = (datetime.strptime(self.caclData, '%Y-%m-%d') + timedelta(days=-1)).strftime("%Y-%m-%d")
        rf = self.sparkSession.sql(self._search_base_data(T1Date, T2Date, funTrdType, TableUnClose,database))
        res = rf.rdd.map(lambda row: _traversal_unclose_prd(row, T1Date))
        CloseTableRDD = res.filter(lambda m: m[0] is True).map(lambda m: m[1]).toDF()
        UnCloseTableRDD = res.filter(lambda m: m[0] is False).map(lambda m: m[1]).toDF()
        save_data(self.sparkSession,database,TableClose,T1Date,CloseTableRDD)
        save_data(self.sparkSession, database, TableUnClose, T1Date, UnCloseTableRDD)



if __name__ == '__main__':

    start = 20170317
    end = 20170318
    for x in xrange(start, end):
        print "--------------strat %s-------------" % x
        caclday = datetime.strptime(str(x), "%Y%m%d").strftime("%Y-%m-%d")
        ClosePrd(caclday).process()
