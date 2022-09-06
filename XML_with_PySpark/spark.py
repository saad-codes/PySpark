def Spark():
    from pyspark.sql import SparkSession

    spark = SparkSession \
            .builder\
            .appName("Read MySQL Table Demo")\
            .master("local[3]")\
            .config("spark.jars","C:\Program Files (x86)\MySQL\Connector J 8.0\mysql-connector-java-8.0.28.jar")\
            .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0")\
            .config("spark.executor.xtraclassPath","C:\Program Files (x86)\MySQL\Connector J 8.0\mysql-connector-java-8.0.28.jar")\
            .config("spark.executor.extraLibrary","C:\Program Files (x86)\MySQL\Connector J 8.0\mysql-connector-java-8.0.28.jar")\
            .config("spark.driver.extraclassPath","C:\Program Files (x86)\MySQL\Connector J 8.0\mysql-connector-java-8.0.28.jar")\
            .getOrCreate()
    spark.sparkContext.setLogLevel('OFF')
    return spark