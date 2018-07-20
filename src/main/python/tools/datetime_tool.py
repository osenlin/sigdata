import calendar
from datetime import datetime, timedelta

def get_trading_day(spark, database, table):
    """
    """
    def fold_dict(a, b):
        a.update(b)
        return a
    command = "select * from %s.%s" % (database, table)
    df = spark.sql(command)
    date_order = df.rdd.map(lambda x: {x.busi_date: x.rlt_rank}) \
        .reduce(fold_dict)
    order_date = dict([date_order[i], i] for i in date_order)
    date_order = spark._sc.broadcast(date_order)
    order_date = spark._sc.broadcast(order_date)
    return date_order, order_date


def get_date(date_order, order_date, busi_date, diff_num):
    """
    """
    date_rank = date_order.value.get(busi_date) + diff_num
    return order_date.value.get(date_rank)

def is_trading_day(busi_date,date_order):
    """
    """
    return busi_date in date_order.value

