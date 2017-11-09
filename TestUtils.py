import MySQLdb as mdb
import Utils as ut
db_host = 'localhost'
db_user = 'Cam'
db_pass = 'password'
db_name = 'securities_master'
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

col = ut.getColumn("utils_symbol", "ticker", con)
print(col)
