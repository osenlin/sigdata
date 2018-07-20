# coding=utf-8
import optparse
from fdata.sigmajob import SigmaSparkJob
from tools.datetime_tool import *
from datetime import datetime,timedelta
from fdata.stock_cash_flow.merge import CashFlowMerge
from fdata.stock_account.check import AccountCheck
from fdata.stock_account.check_exception_detail import CheckExceptionDetail
from fdata.stock_account.check_exception_stat import CheckExceptionStat

cfg = {
    "merge": "CashFlowMerge",
    "check": "AccountCheck",
    "check_exception_detail":"CheckExceptionDetail",
    "check_exception_stat":"CheckExceptionStat"
}

if __name__ == "__main__":
    parse = optparse.OptionParser(usage='"simga job start cmd template:%prog [options] arg1,arg2,..."',
                                  version="%prog 1.0")
    # parse.add_option("-m",'--module-path',dest='modulepath',type=str,
    #                  help=u"启动任务所在的模块，默认加载src/main/python下的路径的module，eg：/fdata/stock_acount")
    parse.add_option("-f", "--filename", dest='filename', type=str, help=u"启动任务所在YTHON文件名")
    # parse.add_option("-c","--class-name",dest="classname",type=str,help=u'启动任务所在PYTHON类名')
    parse.add_option('-s', '--startdate', dest='startdate', type=str, help=u'任务开始时间,格式%Y-%m-%d')
    parse.add_option('-e', '--enddate', dest='enddate',default=None, type=str, help=u'批量任务开启该参数，任务结束时间，默认等于statdate,格式%Y-%m-%d')
    parse.add_option('-t', '--yarn-task-name', dest='taskname', default=None, type=str, help=u'yarn的任务名，不填默认等于类型')
    #
    options, args = parse.parse_args()
    # print 'OPTIONS:', options
    # print 'ARGS:', args
    # startdate=options.startdate
    # enddate=startdate if options.enddate is None else options.enddate
    # if SigmaSparkJob.logLevel=='debug':
    #     dir = os.path.dirname(os.path.abspath(".")) + options.modulepath
    # else:
    #     dir='.'+options.modulepath
    #
    # file, path_name, description = imp.find_module(options.filename, [dir])
    # try:
    #     ModuleName = imp.load_module(options.filename, file, path_name, description)
    #     eval('%s.%s' % (ModuleName,options.classname))(taskName).process(startdate,enddate)
    # except Exception as e:
    #     raise Exception(u"启动模块[{0}]，文件名[{1}],类名[2]的任务失败".format(options.modulepath,options.filename,options.classname))
    # finally:
    #     if file:
    #         file.close()

    if not cfg.get(options.filename): raise Exception("doesn't exist this task")
    taskName = cfg.get(options.filename) if options.taskname is None else options.taskname
    sparkSession = SigmaSparkJob.setupSpark(None, "yarn", "get_trading_day")
    date_order, order_date = get_trading_day(sparkSession, "odata", "trading_day")
    startdateStr = options.startdate
    enddateStr=options.enddate if options.enddate is not None else startdateStr
    try:
        startdate=datetime.strptime(startdateStr,"%Y-%m-%d")
        enddate=datetime.strptime(enddateStr,"%Y-%m-%d")
    except Exception:
        raise Exception("startdate or endate format should be %Y-%m-%d")
    if(enddate-startdate).days<0: raise Exception("enddate < startdate")
    for tdl in xrange((enddate-startdate).days+1):
        realStartdateStr=(startdate+timedelta(days=tdl)).strftime("%Y-%m-%d")
        if not is_trading_day(realStartdateStr,date_order):
            print("day[%s] is not trading day" % realStartdateStr)
            continue
        t1Date = get_date(date_order, order_date, realStartdateStr, -1)
        t2Date = get_date(date_order, order_date, realStartdateStr, -2)
        print '-'*50
        print 'start task:',taskName
        print 'T1Date:', t1Date,'T2Date:', t2Date
        print '-' * 50
        eval(cfg.get(options.filename))(taskName).process(t2Date,t1Date)
