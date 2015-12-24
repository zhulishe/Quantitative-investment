# coding=utf-8
__author__ = 'linchao'

import memcache
from ipyparallel import Client
c=Client('ipcontroller-client.json')
mc=memcache.Client(['192.168.0.103:11211'])
with open("my_test.py",'r')as f:
    f1=f.read()
with open("my_performance.py",'r')as f:
    f2=f.read()
with open("my_strategy.py",'r')as f:
    f3=f.read()
mc.set('my_test',f1)
mc.set('my_performance',f2)
mc.set('my_strategy',f3)
print "to writting files"
def updateFile(n):
    """
    更新远程节点的依赖文件。注意：远程机器必须安装python-memcached
    """
    import memcache
    mc=memcache.Client(['192.168.0.103:11211'])
    with open('my_test.py','w')as f:
        f.write(mc.get('my_test'))
    with open('my_performance.py','w')as f:
        f.write(mc.get('my_performance'))
    with open('my_strategy.py','w')as f:
        f.write(mc.get('my_strategy'))
    return n#mc.get('my_test')
# for n,i in enumerate(c):
#     print n
print c[:].apply_sync(updateFile,"ok")
