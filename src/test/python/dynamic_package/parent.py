

class Parent:

    def __init__(self):
        print 'helloword'

    #def process(self):

    #    print "parent process"

if __name__=="__main__":

    import imp
    import os

    dir = os.path.dirname(os.path.abspath(".")) + '/dynamic_package/t2'
    print dir
    file, path_name, description = imp.find_module('child_parent', [dir])
    print path_name, description, file
    try:
        B = imp.load_module('child_parent', file, path_name, description)
        eval('B.ChildParent')().process()
        #eval('ChildParent')().process()
    finally:
        if file:
            file.close()