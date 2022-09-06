def curated():
    print("Inside landing")

    import connection
    customer_order_left_join_customer_lef_country_staging = connection.mysql_connection_read(table="customer_order_left_join_customer_lef_country_staging",database="staging")
    # writing data into landing
    connection.mysql_write_table(customer_order_left_join_customer_lef_country_staging,"curated",table="customer_wide_fact",mode_for_table="overwrite")
