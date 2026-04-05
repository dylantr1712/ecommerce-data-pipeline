import redshift_connector

conn = redshift_connector.connect(
    host="ecommerce-workgroup.313828097071.ap-southeast-2.redshift-serverless.amazonaws.com",
    database="dev",
    user="admin",
    password="CDYVGtonjn372)$",
    port=5439
)

cursor = conn.cursor()

cursor.execute("SELECT current_date")

result = cursor.fetchone()

print("Connected successfully!")
print("Result:", result)

conn.close()