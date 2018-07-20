# coding=utf-8
import pyspark,imp,os,optparse
from pyspark.sql import SparkSession

class SigmaSparkJob:
    """
    python spark的作业基类
    """
    logLevel = 'info'

    @staticmethod
    def setupSpark(clusterConf, master, appName):
        """
        Setup spark cluster

        Parameters
        ----------
        clusterConf: dict
            The additional configuration of PyEsConf
        """

        if SigmaSparkJob.logLevel == 'debug':
            class Log:
                def sql(self, str):
                    """

                    :rtype: object
                    """
                    print("------------------command line start-------------------")
                    print("{0}{1}{2}".format("sparkSession.sql(\"\"\"", str, "\"\"\")"))
                    print("------------------command line end---------------------")

            sparkSession = Log()
        else:
            conf = pyspark.SparkConf().setMaster(master).setAppName(appName)
            if not clusterConf:
                # todo iterate clusterConf
                pass
            else:
                # todo default config
                conf.set("spark.executor.memory", "1g")
            sparkSession = SparkSession.builder.config(conf=pyspark.SparkConf()).enableHiveSupport().getOrCreate()
        return sparkSession

# 需要定位到依赖文件所在的目录
if __name__=="__main__":

    parse = optparse.OptionParser(usage='"simga job start cmd template:%prog [options] arg1,arg2,..."', version="%prog 1.0")
    parse.add_option("-m",'--module-path',dest='modulepath',type=str,
                     help=u"启动任务所在的模块，默认加载src/main/python下的路径的module，eg：/fdata/stock_acount")
    parse.add_option("-f","--filename",dest='filename',type=str,help=u"启动任务所在YTHON文件名")
    parse.add_option("-c","--class-name",dest="classname",type=str,help=u'启动任务所在PYTHON类名')
    parse.add_option('-s', '--startdate', dest='startdate', type=str, help=u'任务开始时间,格式%Y-%m-%d')
    parse.add_option('-e', '--enddate', dest='enddate',default=None, type=str, help=u'任务结束时间，默认等于statdate,格式%Y-%m-%d')
    parse.add_option('-t','--yarn-task-name',dest='taskname',default=None,type=str,help=u'yarn的任务名，不填默认等于类型')

    options, args = parse.parse_args()
    print 'OPTIONS:', options
    print 'ARGS:', args
    startdate=options.startdate
    enddate=startdate if options.enddate is None else options.enddate
    taskName=options.classname if options.taskname is None else options.taskname
    if SigmaSparkJob.logLevel=='debug':
        dir = os.path.dirname(os.path.abspath(".")) + options.modulepath
    else:
        dir='.'+options.modulepath

    file, path_name, description = imp.find_module(options.filename, [dir])
    try:
        ModuleName = imp.load_module(options.filename, file, path_name, description)
        eval('%s.%s' % (ModuleName,options.classname))(taskName).process(startdate,enddate)
    except Exception as e:
        raise Exception(u"启动模块[{0}]，文件名[{1}],类名[2]的任务失败".format(options.modulepath,options.filename,options.classname))
    finally:
        if file:
            file.close()

