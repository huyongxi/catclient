import MySQLdb

sqlstr = "insert into app_msg (ip,json_str) values ('%s', '%s');"
db = MySQLdb.connect(passwd="abc123",host="127.0.0.1",user="root",db="django_test")
db.autocommit(1)

c=db.cursor()

c.execute(sqlstr%("32323","werererr"))
print(c.fetchall())

c.execute("select * from app_msg;")
print(c.fetchall())


c.close()
db.close()
