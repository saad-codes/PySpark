
def landing():

    print("Inside landing")

    import connection
    country_operations = connection.mysql_connection_read(table="country",database="classicmodels")
    # writing data into landing
    connection.mysql_write_table(country_operations,"landing",table="country_operations_landing",mode_for_table="overwrite")

    customer_order_operations = connection.mysql_connection_read(table="customer_order",database="classicmodels")

    connection.mysql_write_table(customer_order_operations,"landing",table="customer_order_operations_landing",mode_for_table="overwrite")

    customer_operations = connection.mysql_connection_read(table="customer",database="classicmodels")

    connection.mysql_write_table(customer_operations,"landing",table="customer_operations_landing",mode_for_table="overwrite")


