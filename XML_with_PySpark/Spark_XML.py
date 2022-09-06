from pyspark.sql.functions import explode
import connection
from pyspark.sql.types import *


if __name__ == "__main__":
    import spark 
    spark = spark.Spark()

    spark.sparkContext.setLogLevel("OFF")
    print('Ok')
    
    feeds_schema = StructType(
        [
            StructField("code",LongType() , True),
            StructField("commentCount",LongType() , True),
            StructField("createdAt",StringType() , True),
            StructField("description",StringType() , True),
            StructField("feedsComment",StringType() , True),
            StructField("id",LongType() , True),
            StructField("imagePaths",StringType() , True),
            StructField("images",StringType() , True),
            StructField("isdeleted",BooleanType() , True),
            StructField("lat",LongType() , True),
            StructField("likeDislike"
                       ,StructType(
                           [
                               StructField("dislikes" , LongType() , True),
                               StructField("likes" , LongType() , True),
                               StructField("userAction" , LongType() , True),
                           ]
                        )
                       , False),
            StructField("lng",LongType() , False),
            StructField("location",StringType() , False),
            StructField("mediatype",LongType() , False),
            StructField("msg",StringType() , False),
            StructField("multiMedia"
                        ,ArrayType(#"element",
                            
                               StructType(
                                   [
                                       StructField("createAt",StringType() , True),
                                       StructField("description",StringType() , True),
                                       StructField("id",LongType() , True),
                                       StructField("likeCount",LongType() , True),
                                       StructField("mediatype",LongType() , True),
                                       StructField("name",StringType() , True),
                                       StructField("place",StringType() , True),
                                       StructField("url",StringType() , True),
                                   ]
                              
                               ) 
                        )
                            # ,False)
                        
                        ,True),
            StructField("name",StringType() , True),
            StructField("profilePicture",StringType() , True),
            StructField("title",StringType() , True),
            StructField("userId",LongType() , True),
            StructField("videoUrl",StringType() , True),
        ]
    )
    
    
    
    df = spark.read.format('xml').options(rowTag='feeds').schema(feeds_schema).load(r'complexData.xml')
    print("schema of original xml")
    df.printSchema()
    
    
    
    
    likes_dislikes_table = df.select(df.id , df.likeDislike.dislikes.alias("dislikes"), df.likeDislike.likes.alias("likes"),df.likeDislike.userAction.alias("userAction"))
    print(likes_dislikes_table.show())
    print("schema of likes_dislikes extracted from original xml")
    
    connection.mysql_write_table(likes_dislikes_table , "landing" , "likes_dislikes_table_landing", "overwrite")
    
    
    
    multiMedia_table_temp = (df.select(df.id ,explode(df.multiMedia).alias("exploded")))
    multiMedia_table = multiMedia_table_temp.select(multiMedia_table_temp.id
                                                    ,multiMedia_table_temp.exploded.description.alias("description")
                                                    ,multiMedia_table_temp.exploded.id.alias("multiMedia_id")
                                                    ,multiMedia_table_temp.exploded.likeCount.alias("likeCount")
                                                    ,multiMedia_table_temp.exploded.mediatype.alias("mediatype")
                                                    ,multiMedia_table_temp.exploded.place.alias("place")
                                                    ,multiMedia_table_temp.exploded.url.alias("url")
                                                    )
    
    
    connection.mysql_write_table(multiMedia_table , "landing" , "multiMedia_table_landing", "overwrite")
    
    print("schema of multiMedia extracted from original xml")
    multiMedia_table.printSchema()
    
    feed_table = df.drop("likeDislike","multiMedia")
    print("schema of feed extracted from original xml")
    feed_table.printSchema()
    connection.mysql_write_table(feed_table, "landing" , "feed_table_landing", "overwrite")

