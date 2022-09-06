
def staging():
    from pyspark.sql.functions import col
    from pyspark.sql.functions import when
    import connection
    import spark
    spark = spark.Spark()
    
    
    customer = connection.mysql_connection_read(table="customer_operations_landing" , database = "landing" )
    customer_order = connection.mysql_connection_read(table="customer_order_operations_landing" , database = "landing" )
    country = connection.mysql_connection_read(table="country_operations_landing" , database = "landing" )
    


    # Join left
    customer_order_left_join_customer_lef_country = customer_order.select(col("customer_id"), col("country_id"),col("order_status"), col("order_creation_timestamp"),col("modification_timestamp"))\
                                        .join(customer.select(col("customer_id"),col("customer_first_names"),col("customer_last_name"),col("customer_gender")), ["customer_id"], 'left')\
                                        .join(country.select(col("country_id"),col("country_name")), ["country_id"], 'left')\
                                        .na.fill({"customer_gender" : "not provided" , "customer_first_names" : "not mentioned","customer_last_name" : "not mentioned", "customer_id": -1})\
                                        .withColumn("gender",when(col("customer_gender").startswith("M"),"Male")
                                                            .when(col("customer_gender").startswith("m"),"Male")
                                                            .when(col("customer_gender").startswith("F"),"Female")
                                                            .when(col("customer_gender").startswith("f"),"Female")
                                                            .otherwise(col("customer_gender")))\
                                        .drop(col("customer_gender"))\
                                        .filter(col("order_status") != 'dispatched')\
                                                                    
                                            
    print(customer_order_left_join_customer_lef_country.printSchema())
    print(f"total Data {customer_order_left_join_customer_lef_country.count()}")
    print("group by with repect to order_status")                                       
    print(customer_order_left_join_customer_lef_country.groupBy(col("order_status")).count().show())   
    print("group by with repect to country_name")                                       
    print(customer_order_left_join_customer_lef_country.groupBy("country_name").count().show())   
    print("masking example")
    print(customer_order_left_join_customer_lef_country.filter(customer_order_left_join_customer_lef_country["country_name"] == "united arab emirates").show())    
    
    print("making view and directly running quries")
    print("making View")
    customer_order_left_join_customer_lef_country.createOrReplaceTempView("Joined_Data_view")
    print("finding customers wih -1 customer_id (i.e not given) ")
    sqlDF = spark.sql("SELECT * FROM Joined_Data_view WHERE customer_id == -1 ")
    print(sqlDF.show())
                                  
                                            
    connection.mysql_write_table(customer_order_left_join_customer_lef_country , "staging", "customer_order_left_join_customer_lef_country_staging",mode_for_table="overwrite" )
 