

from datetime import datetime,timedelta
startdateStr='2017-03-15'
enddateStr='2017-03-17'
startdate = datetime.strptime(startdateStr, "%Y-%m-%d")
enddate = datetime.strptime(enddateStr, "%Y-%m-%d")
for tdl in xrange((enddate - startdate).days + 1):
    realStartdateStr = (startdate+timedelta(days=tdl)).strftime("%Y-%m-%d")
    print realStartdateStr

