import mysql.connector

# 👉 Database connection
def connect(host, user, password, database):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

# 👉 Database Information
database = "pbi_balancer"
connection = connect("localhost", "user", "helloworld12345", database)
cursor = connection.cursor()
