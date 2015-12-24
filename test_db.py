__author__ = 'lisa'
import MySQLdb
i = 1
conn1=MySQLdb.connect(host="192.168.0.103", user="root", passwd="root", db="new_stock", charset="utf8")
sql_mv = "select content from `my_stock_index` where date_time='2007-0%d-01' and group_code='000300.SH'"%i
cur = conn1.cursor()
cur.execute(sql_mv)
hangyedata = cur.fetchall()

print hangyedata
