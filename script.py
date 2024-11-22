from function import getTables, getColumns, writeTxt, writeCsv, getFirstDate, getRowsPerMonth
from datetime import datetime
from connect import cursor, database
import os

class CodeGeneratorBackend:
    def __init__(self):
        self.code = []
        self.level = 0

    def begin(self, tab="    "):
        self.tab = tab

    def write(self, line):
        self.code.append(self.tab * self.level + line)

    def indent(self):
        self.level += 1

    def dedent(self):
        if self.level == 0:
            raise SyntaxError("internal error in code generator")
        self.level -= 1

    def end(self):
        return "".join(self.code)

class CodeGenerator:
    def __init__(self, table, cursor, database, columns, dateColumn):
        self.table = table
        self.cursor = cursor
        self.database = database
        self.columns = columns
        self.dateColumn = dateColumn

    def dump_self(self, filename):
        self.c = CodeGeneratorBackend()
        self.c.begin(tab="    ")
        self.c.write("from function import writeTxt, writeCsv, fetchLatestData\n")
        self.c.write("from datetime import datetime\n")
        self.c.write("from connect import cursor\n\n")
        self.c.write(f"today = datetime.today()\n\n")
        self.c.write(f"writeTxt(f'{self.table}/{self.table}_last_sync_date', today.strftime('%Y-%m-%d %H:%M:%S'))\n")
        self.c.write(f"latestData = fetchLatestData(cursor, '{self.table}', '{self.dateColumn}')\n")
        self.c.write(f'writeCsv(f"{self.table}/{{today.strftime("%Y_%m_%d_%H_%M_%S")}}_{self.table}_", \',\', {self.columns}, latestData)\n')
        f = open(filename, 'w')
        f.write(self.c.end())
        f.close()

cursor = cursor

createdAtColumnNameJson = {
    "capacity_planning_solarwinds_hsm": "datetime_inserted",
    "abuse_webapps_activity": "updated_at",
}

tables = [
    "capacity_planning_solarwinds_hsm",
    "abuse_webapps_activity",
]

# 👉 Generate script per table
for table in tables:
    columns = []
    for column in getColumns(cursor, table):
        columns.append(column[0])

    c = CodeGenerator(table, cursor, database, columns, createdAtColumnNameJson[table])
    c.dump_self(f"{table}.py")

# 👉 Sync all existing tables & data to csv
for table in tables:
    os.mkdir(table)

    columns = []
    for column in getColumns(cursor, table):
        columns.append(column[0])

    firstDate = getFirstDate(cursor, table, createdAtColumnNameJson[table])

    startYear = firstDate[0][0].year
    startMonth = firstDate[0][0].month
    today = datetime.today()

    while (startYear < today.year) or (startYear == today.year and startMonth <= today.month):
        endMonth = startMonth + 1
        endYear = startYear
        if startMonth == 12:
            endMonth = 1
            endYear = startYear + 1

        rows = getRowsPerMonth(cursor, table, columns, createdAtColumnNameJson[table], startYear, startMonth, endYear, endMonth)

        writeCsv(f"{table}/{startYear}_{startMonth}_{table}_data", ',', columns, rows)
        
        startMonth += 1
        if startMonth > 12:
            startMonth = 1
            startYear += 1
        
    # writeCsv(f"{table}/{today.strftime("%Y_%m_%d_%H_%M_%S")}_{table}", ',', columns, [])
    writeTxt(f"{table}/{table}_last_sync_date", today.strftime("%Y-%m-%d %H:%M:%S"))







# # 👉 Generate script per table
# for (table,) in getTables(cursor, database):
#     columns = []
#     for column in getColumns(cursor, table):
#         columns.append(column[0])

#     c = CodeGenerator(table, cursor, database, columns, createdAtColumnNameJson[table])
#     c.dump_self(f"{table}.py")

# # 👉 Sync all existing tables & data to csv
# for (table,) in getTables(cursor, database):
#     os.mkdir(table)

#     columns = []
#     for column in getColumns(cursor, table):
#         columns.append(column[0])

#     firstDate = getFirstDate(cursor, table, createdAtColumnNameJson[table])

#     startYear = firstDate[0][0].year
#     startMonth = firstDate[0][0].month
#     today = datetime.today()

#     while (startYear < today.year) or (startYear == today.year and startMonth <= today.month):
#         endMonth = startMonth + 1
#         endYear = startYear
#         if startMonth == 12:
#             endMonth = 1
#             endYear = startYear + 1

#         rows = getRowsPerMonth(cursor, table, columns, createdAtColumnNameJson[table], startYear, startMonth, endYear, endMonth)

#         writeCsv(f"{table}/{startYear}_{startMonth}_{table}_data", ',', columns, rows)
        
#         startMonth += 1
#         if startMonth > 12:
#             startMonth = 1
#             startYear += 1
        
#     # writeCsv(f"{table}/{today.strftime("%Y_%m_%d_%H_%M_%S")}_{table}", ',', columns, [])
#     writeTxt(f"{table}/{table}_last_sync_date", today.strftime("%Y-%m-%d %H:%M:%S"))

# # 👉 Sync latest data to csv (Implemented for each table by automated generated)
# for (table,) in getTables(cursor, database):
#     writeTxt(f"{table}/{table}_last_sync_date", datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
    
#     columns = []
#     for column in getColumns(cursor, table):
#         columns.append(column[0])

#     print(f"{table}: {fetchLatestData(cursor, table, createdAtColumnNameJson[table])}")
#     latestData = fetchLatestData(cursor, table, createdAtColumnNameJson[table])

#     writeCsv(f"{table}/{table}_sync_data", ',', [], latestData)