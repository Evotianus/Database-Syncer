import mysql.connector
import csv
from datetime import datetime

# 👉 Database operations
def getTables(cursor, database):
    cursor.execute(f"USE {database}")
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    return tables

def getColumns(cursor, table):
    cursor.execute("SHOW COLUMNS FROM " + table)
    columns = cursor.fetchall()
    return columns

def getRows(cursor, table):
    cursor.execute("SELECT * FROM " + table)
    rows = cursor.fetchall()
    return rows

# 👉 File handling
def writeTxt(fileName, data):
    with open(f"D:/Internship/PBI/Balancer/{fileName}.txt", "w+") as file:
        file.write(data)

def readTxt(fileName):
    with open(f"D:/Internship/PBI/Balancer/{fileName}.txt", "r") as file:
        data = file.read()
        return data

def writeCsv(fileName, delimiter, column, data):
    with open(f"D:/Internship/PBI/Balancer/{fileName}.csv", "w+", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        if column:
            writer.writerow(column)
        writer.writerows(data)

def readCsv(fileName):
    rows = []

    with open(f"D:/Internship/PBI/Balancer{fileName}.csv", 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        rows.append(fields)
        for row in csvreader:
            rows.append(row)

    return rows

# 👉 Process logic
def getIndexOfCreatedAt(columns, createdAtColumnName):
    for i in range(len(columns)):
        if createdAtColumnName in columns[i].lower():
            return i

    return -1

def fetchLatestData(cursor, table, createdAtColumnName):
    try:
        latestDate = readTxt(f"{table}/{table}_last_sync_date")
    except:
        latestDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(f"SELECT * FROM {table} WHERE {createdAtColumnName} > '{latestDate}'")

    data = cursor.fetchall()
    return data

def getFirstDate(cursor, table, createdAtColumnName):
    cursor.execute(f"SELECT {createdAtColumnName} FROM {table} ORDER BY {createdAtColumnName} ASC LIMIT 1")
    firstDate = cursor.fetchall()

    return firstDate

def getRowsPerMonth(cursor, table, columns, createdAtColumnName, startYear, startMonth, endYear, endMonth):
    cursor.execute(f"SELECT * FROM {table} WHERE {createdAtColumnName} >= '{startYear}-{startMonth}-01 00:00:00' AND {createdAtColumnName} < '{endYear}-{endMonth}-01 00:00:00'")
    rows = cursor.fetchall()

    return rows