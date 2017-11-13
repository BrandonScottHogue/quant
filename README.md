## QuantDB

# Change Log

11/13
- Installed TA-Lib
- Installed TA-Lib python wrapper - See: https://github.com/mrjbq7/ta-lib
- Added TACalculations.py, which is a collection of technical analysis functions that rely on TA-Lib and it's wrapper
  The goal is to make a framework where we can easily perform TA on data in the DB easily. Currently, the functions return a dataframe,     with select few columns. Using Pandas, I think it's easy to combine dataframes on indexes (like a SQL join), but I think it would be       better to create a parent function that will return the final dataframe based on what optional columns are wanted.

--Brandon
