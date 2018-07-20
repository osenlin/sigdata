import optparse

parse = optparse.OptionParser(usage='"usage:%prog [options] arg1,arg2"', version="%prog 1.2")
parse.add_option('-u', '--user', dest='user', action='store', type=str, metavar='user', help='Enter User Name!!')
parse.add_option('-p', '--port', dest='port',default=1, type=int, metavar='xxxxx', help='Enter Mysql Port!!')

options, args = parse.parse_args()
print 'OPTIONS:', options
print 'ARGS:', args

print '~' * 20
print 'user:', options.user
print 'port:', options.port

