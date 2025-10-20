import mysql.connector

# Instantiate connector.
connector = mysql.connector.connect(
    host="localhost",
    # port="3306", # uncomment and set the port if necessary.
    user="root",  # change if necessary, but it shouldn't be.
    password="Spec3RootFour1",  # set password.
)


# Open a cursor.
try:
    cursor = connector.cursor(dictionary=True)
    cursor.execute("CREATE USER 'curseist'@'localhost' IDENTIFIED BY 'curseword'")
    cursor.execute("GRANT ALL PRIVILEGES ON *.* TO 'curseist'@'%'")
    cursor.execute("CREATE DATABASE ProductDB")
    connector.commit()
except Exception as e:
    print(f"commit failed with: {e}")
    exit
# Close cursor
cursor.close()

print("fino dino!")
