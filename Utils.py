import pandas as pd
def getColumn(tableName, colName, con):
    col = pd.read_sql("SELECT " + str(colName) + " FROM " + str(tableName),
                        con=con).values
    colStr = ["" for x in range(len(col))]
    idx = 0
    for val in col:
        colStr[idx] = str(val)[1:-1]
        idx += 1
    return  colStr
