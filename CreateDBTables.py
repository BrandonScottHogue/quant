import MySQLdb as mdb

def createTable(tableName, fullCommand):
    db_host = 'localhost'
    db_user = 'Cam'
    db_pass = 'password'
    db_name = 'securities_master'
    con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

    cur = con.cursor()
    try:
        cur.execute("SHOW TABLES LIKE '" + tableName + "'")
        if cur.fetchone():
            print("Symbol table already exists")
            print("")
        else:
            cur.execute(fullCommand)
    except mdb.DatabaseError:
        con.rollback()
        raise
    else:
        con.commit()
    finally:
        cur.close()


tableName = "symbol"
tableCols = ["id","exchange", "ticker", "name", "instrument", "sector",
            "industry","ipo_year", "created_date", "last_update_date"]


createTableStr = "CREATE TABLE `" + tableName + "`(\n"
idStr = "`%s` int NOT NULL AUTO_INCREMENT,\n" % tableCols[0]
exchangeStr = "`%s` varchar(6) NOT NULL,\n"% tableCols[1]
tickerStr = "`%s` varchar(6) NOT NULL,\n"% tableCols[2]
nameStr = "`%s` varchar(210) NOT NULL,\n"% tableCols[3]
instrumentStr = "`%s` varchar(5) NULL,\n"% tableCols[4]
sectorStr = "`%s` varchar(100) NULL,\n" % tableCols[5]
industryStr = "`%s` varchar(200) NULL,\n" %tableCols[6]
ipoStr = "`%s` varchar(4) NULL,\n" %tableCols[7]
createDateStr = "`%s` datetime NOT NULL,\n"% tableCols[8]
updateDateStr = "`%s` datetime NOT NULL,\n"% tableCols[9]
endStr1 = "PRIMARY KEY (`%s`)\n" % tableCols[0]
endStr2 = ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"

fullCommand = (createTableStr+idStr+exchangeStr+tickerStr+nameStr+instrumentStr+
               sectorStr+industryStr+ipoStr+createDateStr+updateDateStr+endStr1
               +endStr2)

print(fullCommand)
print("")

createTable(tableName,fullCommand)
# with con:
#         cur = con.cursor()
#         cur.execute("SHOW TABLES LIKE 'symbol'")
#         if cur.fetchone():
#             print("Symbol table already exists")
#         else:
#             cur.execute(fullCommand)
tableName = "stock_data"
tableCols = ["id","exchange","ticker","close","open","low","high","ask",
            "bid","volume","shares_outstanding","dividend_per_share",
            "ex_dividend_date","dividend_pay_date","eps","eps_est_next_quarter",
            "eps_est_current_year","eps_est_next_year","ebitda","book_value",
            "price_per_sales","price_per_book","peg","short_ratio","revenue",
            "date","adjusted_close"]

createTableStr = "CREATE TABLE `" + tableName +"`(\n"
idStr = "`%s` int NOT NULL AUTO_INCREMENT,\n" % tableCols[0]
exchangeStr = "`%s` varchar(6) NOT NULL,\n"% tableCols[1]
tickerStr = "`%s` varchar(6) NOT NULL,\n"% tableCols[2]
dateStr = "`%s` datetime NOT NULL,\n"% tableCols[25]
closeStr = "`%s` decimal(19,4) NULL,\n"% tableCols[3]
openStr = "`%s` decimal(19,4) NULL,\n"% tableCols[4]
lowStr = "`%s` decimal(19,4) NULL,\n"% tableCols[5]
highStr = "`%s` decimal(19,4) NULL,\n"% tableCols[6]
askStr = "`%s` decimal(19,4) NULL,\n"% tableCols[7]
bidStr = "`%s` decimal(19,4) NULL,\n" % tableCols[8]
volumeStr = "`%s` decimal(19,4) NULL,\n" % tableCols[9]
shareOutstandingStr = "`%s` bigint NULL,\n" % tableCols[10]
divPerShareStr = "`%s` decimal(19,4) NULL,\n" % tableCols[11]
exDivDateStr = "`%s` datetime NULL,\n" % tableCols[12]
divPayDateStr = "`%s` datetime NULL,\n" % tableCols[13]
epsStr = "`%s` decimal(19,4) NULL,\n" % tableCols[14]
epsNextQtrStr = "`%s` decimal(19,4) NULL,\n" % tableCols[15]
epsCurYrStr = "`%s` decimal(19,4) NULL,\n" % tableCols[16]
epsNextYrStr = "`%s` decimal(19,4) NULL,\n" % tableCols[17]
ebitdaStr = "`%s` bigint NULL,\n" % tableCols[18]
bookValueStr = "`%s` bigint NULL,\n" % tableCols[19]
pricePerSalesStr = "`%s` decimal(19,4) NULL,\n" % tableCols[20]
pricePerBookStr = "`%s` decimal(19,4) NULL,\n" % tableCols[21]
pegStr = "`%s` decimal(19,4) NULL,\n" % tableCols[22]
shortRatioStr = "`%s` decimal(19,4) NULL,\n" % tableCols[23]
revenueStr = "`%s` bigint NULL,\n" % tableCols[24]
adjCloseStr = "`%s` decimal(19,4) NULL,\n" % tableCols[26]
endStr1 = "PRIMARY KEY (`%s`),\n" % tableCols[0]
endStr2 = "KEY `symbol_ticker` (`%s`)\n"% tableCols[2]
endStr3 = ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"

fullCommand = (createTableStr+idStr+exchangeStr+tickerStr+dateStr+
                openStr+highStr+lowStr+closeStr+adjCloseStr+volumeStr+askStr+
                bidStr+shareOutstandingStr+divPerShareStr+exDivDateStr+
                divPayDateStr+epsStr+epsNextQtrStr+epsCurYrStr+epsNextYrStr+
                ebitdaStr+bookValueStr+pricePerSalesStr+pricePerBookStr+pegStr+
                shortRatioStr+revenueStr+endStr1+endStr2+endStr3)

print(fullCommand)
print("")

createTable(tableName,fullCommand)
