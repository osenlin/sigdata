# coding=utf-8
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext

from datetime import datetime, timedelta

logLevel = 'info'


def setupSpark(clusterConf, master, appName):
    """
    Setup spark cluster

    Parameters
    ----------
    clusterConf: dict
        The additional configuration of PyEsConf
    """
    if logLevel == 'debug':
        class Log:
            def sql(self, str):
                print("------------------command start-------------------")
                print("{0}{1}{2}".format("sparkSession.sql(\"\"\"", str, "\"\"\")"))
                print("------------------command end---------------------")

        sparkSession = Log()
    else:
        conf = SparkConf().setMaster(master).setAppName(appName)
        if not clusterConf:
            # todo iterate clusterConf
            pass
        else:
            # todo default config
            conf.set("spark.executor.memory", "1g")

        sparkSession = SparkSession.builder.config(conf=SparkConf()).enableHiveSupport().getOrCreate()
    return sparkSession


def _traversal_unclose_trade(row,busiDate):
    openDetail = row.open_detail if row.open_detail is not None else []
    trdDetail=row.trd_detail if row.trd_detail is not None else []

    trdInd=0;openInd=0
    while trdInd<len(trdDetail) and openInd<len(openDetail):
        yOpenDetailItem=openDetail[openInd]
        xTrdDetailItem=trdDetail[trdInd]
        xTrdQty = int(xTrdDetailItem['trd_qty'])
        xTrdCashFlow = float(xTrdDetailItem['trd_cash_flow'])
        xAmortizeLabel = float(xTrdDetailItem['amortize_label'])
        if xTrdQty >= 0 and xAmortizeLabel == 0:
            if  openDetail[-1]['open_date'] == busiDate:
                openDetail[-1]['trd_qty'] =int(openDetail[-1]['trd_qty'])+ trdQty
                openDetail[-1]['unclose_qty'] = int(openDetail[-1]['unclose_qty']) + trdQty
                openDetail[-1]['orig_trd_qty'] = int(openDetail[-1]['orig_trd_qty'])+ trdQty
                openDetail[-1]['trd_amt'] = float(openDetail[-1]['trd_amt'])+ trdCashFlow
                openDetail[-1]['orig_trd_amt'] = float(openDetail[-1]['orig_trd_amt'])+ trdCashFlow
            else:
                openDetail.append({
                    u'open_date': busiDate,
                    u'orig_trd_qty': trdQty,
                    u'orig_trd_amt': trdCashFlow,
                    u'trd_qty': trdQty,
                    u'trd_amt': trdCashFlow,
                    u'close_amt': 0,
                    u'unclose_qty': trdQty,
                    u'unclose_amt': 0,
                    u'weighted_term': 0,
                    u'exception_lab': 0
                })
            # 更新X等于0
        elif xTrdQty >= 0 and amortizeLabel == 1:
            # 需要将opendetail的交易量，对trd_detail中的trd_qty和trd_cash_flow进行平摊
            singleAllTrdQty = reduce(lambda x, y: int(x['trd_qty']) + int(y['trd_qty']), openDetail[openInd:])

            for partOpenDetail in openDetail[openInd:]:
                ratio = int(partOpenDetail['trd_qty']) / singleAllTrdQty
                partOpenDetail['trd_qty'] += xTrdQty * ratio
                partOpenDetail['unclose_qty'] += xTrdQty * ratio
                if trdCashFlow >= 0:
                    partOpenDetail['trd_amt']=float(partOpenDetail['trd_amt'])+xTrdCashFlow * ratio
                else:
                    partOpenDetail['close_amt']= float(partOpenDetail['close_amt'])+(-1 * xTrdCashFlow * ratio)
            # 更新X等于0
        elif xTrdQty < 0 and amortizeLabel == 0:

            yUnCloseQty=int(yOpenDetailItem['unclose_qty'])
            if  yUnCloseQty +xTrdQty >=0 :
                yOpenDetailItem['close_amt'] = float(yOpenDetailItem['close_amt'])+xTrdCashFlow
                yOpenDetailItem['unclose_qty'] =yUnCloseQty+trdQty
                yOpenDetailItem['unclose_amt'] =(yUnCloseQty/(yUnCloseQty-xTrdQty))*float(yOpenDetailItem['unclose_amt'])
                trdInd+=1
                if int(yOpenDetailItem['unclose_qty'])==0: openInd+=1
                # 更新X等于0
            else:
                yOpenDetailItem['close_amt'] =float(yOpenDetailItem['close_amt'])+xTrdCashFlow * (yUnCloseQty / xTrdQty)
                # todo 确认按比例更新x的trd_cash_flow和trd_qty
                xTrdCashFlow = xTrdCashFlow * [1 - (yUnCloseQty / -xTrdQty)]
                xTrdDetailItem['trd_qty'] =xTrdQty+ yUnCloseQty
                yOpenDetailItem['unclose_amt'] = 0
                yOpenDetailItem['unclose_qty'] = 0
                openInd+=1
    #处理当日未完成交易
    else:
        for trdDetailItem in row.trd_detail:
            trdQty = int(trdDetailItem['trd_qty'])
            trdCashFlow = float(trdDetailItem['trd_cash_flow'])
            amortizeLabel = float(trdDetailItem['amortize_label'])
            if trdQty >= 0 and amortizeLabel == 0:
                openDetail.append({
                    u'open_date': busiDate,
                    u'orig_trd_qty': trdQty,
                    u'orig_trd_amt': trdCashFlow,
                    u'trd_qty': trdQty,
                    u'trd_amt': trdCashFlow,
                    u'close_amt': 0,
                    u'unclose_qty': trdQty,
                    u'unclose_amt': 0,
                    u'weighted_term': 0,
                    u'exception_lab': 0
                })

            # todo 遇到其他的交易用其他方式进行处理，后面会统一处理
    return openDetail

def _traversal_holding_date(row):
    pass

class PerTrade:
    """
    this class will use to calculate profit of the each trade

    """


    def __init__(self, taskname):
        self.sparkSession = setupSpark(None, "yarn", taskname)
        pass

    def process(self,startdate):
        self.caclData = startdate
        self._sub_process('long_related', 'fdata.stock_unclose_trade_long_data', 'fdata.stock_close_trade_long_data')
        self._sub_process('short_related', 'fdata.stock_unclose_trade_short_data', 'fdata.stock_close_trade_short_data')

    def _sub_process(self, funTrdType, funUncloseTable, funCloseTable, funUncloseCalTable):
        busiDate = datetime.strptime(self.caclData, '%Y-%m-%d')
        T1Date = busiDate.strftime("%Y-%m-%d")
        T2Date = (busiDate + timedelta(days=-1)).strftime("%Y-%m-%d")
        dfTradeDataRows = self.sparkSession.sql(self._search_base_data(T1Date, T2Date, funTrdType, funUncloseCalTable))
        dfTradeDataRows.rdd.map(lambda row :_traversal_unclose_trade(row,T1Date))

        # begin 处理持仓数据
        for row in dfTradeDataRows:
            openDetail = row['open_detail']
            if openDetail is None: continue
            for openDetailItem in openDetail:
                xUncloseQty = openDetailItem['unclose_qty']
                if xUncloseQty != 0 and xUncloseQty <= row['holding_trd_qty']:
                    openDetailItem['unclose_amt'] = xUncloseQty / row['holding_trd_qty'] * row['holding_mkt_val']
                    # todo 没明白是什么意思
                    row['holding_mkt_val'] -= (openDetailItem['unclose_qty'] / row['holding_trd_qty']) * row[
                        'holding_mkt_val']
                    row['holding_trd_qty'] -= openDetailItem['unclose_qty']
                else:
                    openDetailItem['exception_label'] = 1
                    openDetailItem['close_amt'] += (openDetailItem['unclose_qty'] - row['holding_trd_qty']) / \
                                                   openDetailItem['trd_qty'] * openDetailItem['trd_amt']
                    openDetailItem['unclose_qty'] = row['holding_trd_qty']
                    openDetailItem['unclose_amt'] = row['holding_mkt_val']
                    # todo 没明白是什么意思
                    row['holding_mkt_val'] = 0
                    row['holding_trd_qty'] = 0

        newCloseTradeTable = []
        newUnCloseTradeTable = []
        newUnCloseTradeCalTable = []

        for row in dfTradeDataRows:
            openDetail = row['open_detail']
            if openDetail is None: continue
            for openDetailItem in openDetail:
                xUncloseQty = openDetailItem['unclose_qty']
                if xUncloseQty != 0:
                    newUnCloseTradeCalTable.append({
                        "trade_id": row["trade_id"],
                        "secu_acc_id": row['secu_acc_id'],
                        "prd_no": row['prd_no'],
                        "open_detail": row['open_detail'],
                        busiDate: T1Date
                    })
                    newUnCloseTradeTable.append({
                        "trade_id": row["trade_id"],
                        "secu_acc_id": row['secu_acc_id'],
                        "prd_no": row['prd_no'],
                        "busi_date": T1Date,
                        "open_date": openDetailItem['open_date'],
                        "orig_trd_qty": openDetailItem['orig_trd_qty'],
                        "orig_trd_amt": openDetailItem["orig_trd_amt"],
                        "trd_qty": openDetailItem['trd_qty'],
                        "trd_amt": openDetailItem["trd_amt"],
                        "close_qty": openDetailItem["close_qty"],
                        "close_amt": openDetailItem["close_amt"],
                        "unclose_qty": openDetailItem["unclose_qty"],
                        "unclose_amt": openDetailItem["unclose_amt"],
                        # todo 这个收益率
                        "return": row["close_amt"] - row["trd_amt"],
                        "return_rate": (row["close_amt"] - row["trd_amt"]) / row["trd_amt"],
                        # todo weighted_term
                        "weighted_term": row["weighted_term"],
                        "exception_label": row["exception_label"]
                    })
                else:
                    newCloseTradeTable.append({
                        "trade_id": row["trade_id"],
                        "secu_acc_id": row['secu_acc_id'],
                        "prd_no": row['prd_no'],
                        "busi_date": T1Date,
                        "open_date": openDetailItem['open_date'],
                        "orig_trd_qty": openDetailItem['orig_trd_qty'],
                        "orig_trd_amt": openDetailItem["orig_trd_amt"],
                        "trd_qty": openDetailItem['trd_qty'],
                        "trd_amt": openDetailItem["trd_amt"],
                        "close_qty": openDetailItem["close_qty"],
                        "close_amt": openDetailItem["close_amt"],
                        # todo 这个收益率
                        "return": row["close_amt"] - row["trd_amt"],
                        "return_rate": (row["close_amt"] - row["trd_amt"]) / row["trd_amt"],
                        # todo weighted_term
                        "weighted_term": row["weighted_term"],
                        "exception_label": row["exception_label"]
                    })

        if len(newUnCloseTradeCalTable) > 0:
            self._save(newUnCloseTradeCalTable, "fdata", funUncloseCalTable, "overwrite")

        if len(newUnCloseTradeTable) > 0:
            self._save(newUnCloseTradeTable, "fdata", funUncloseTable, "overwrite")

        if len(newCloseTradeTable)>0:
            self._save(newUnCloseTradeTable, "fdata", funUncloseTable, "overwrite")

    def _search_base_data(self, T1Date, T2Date, funTrdType, funUncloseCalTable):
        sqlTmp = """
            select a.trade_id,a.secu_acc_id,a.prd_no,a.open_detail,b.trd_detail
                c.qty holding_trd_qty,c.mkt_val holding_mkt_val
            from ( select * from {3} where busi_date={1} )a
            full outer join (
                  select trade_id,secu_acc_id,prd_no,collect_list(trd_detail_item) trd_detail from(
                    select  trade_id,secu_acc_id,prd_no,
                        (str_to_map(concat(
                        'trd_qty:',trd_qty,
                        ',trd_cash_flow:',trd_cash_flow,
                        ',amortize_label:',amortize_label,
                        ',timestamp:',timestamp),",",":")) trd_detail_item,
                        row_number() over(partition by trade_id,secu_acc_id,prd_no order by timestamp asc) rank
                    from odata.stock_cash_flow_detail
                    where trd_type='{2}' and busi_date={0}
                  ) a
                  group by trade_id,secu_acc_id,prd_no
                ) b
                on  a.trade_id=b.trade_id  and a.secu_acc_id=b.secu_acc_id  and a.prd_no=b.prd_no
            full outer join 
                   (select * from  odata.stock_asset_holding where busi_date={0} ) c 
                on a.trade_id=c.trade_id  and a.secu_acc_id=c.secu_acc_id  and a.prd_no=c.prd_no
            """.format(T1Date, T2Date, funTrdType, funUncloseCalTable)
        return sqlTmp

    def _save(self, data, db, table, mode):
        tablels = SQLContext.tableNames(dbName=db)
        if table not in tablels:
            self.sparkSession.createDataFrame(data). \
                write. \
                partitionBy('busi_date'). \
                format("orc"). \
                mode(mode). \
                saveAsTable(table)
        else:
            self.sparkSession.createDataFrame(data).write.partitionBy("busi_date").format("orc").insertInto(table, True)


if __name__ == '__main__':
    PerTrade("2017-01-03").process()
