from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc
import pandas as pd
import sys
from graphframes import *


conf = SparkConf().setAppName("project").setMaster("local[*]")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

INPUT_DATA_PATH = sys.argv[1]


####SUBTASK 1
#Create a graph of posts and comments. Nodes are users, and there is an edge from
#node 𝑖 to node 𝑗 if 𝑖 wrote a comment for 𝑗’s post. Each edge has a weight 𝑤𝑖𝑗 that is
#the number of times 𝑖 has commented a post by j

comments = sc.textFile(INPUT_DATA_PATH + "/comments.csv.gz")
posts = sc.textFile(INPUT_DATA_PATH + "/posts.csv.gz")
headerComments = comments.first()
headerPosts = posts.first()
#x[0]: postid of which the comment is written for, x[4]: userid of the one commenting
comment = comments.filter(lambda x : x != headerComments) \
            .map(lambda lines : lines.split("\t")) \
            .map(lambda x : (x[0],x[4]))

#x[0]: postID, x[6]: ownerUserid
post = posts.filter(lambda x : x != headerPosts) \
            .map(lambda lines : lines.split("\t")) \
            .map(lambda x : (x[0], x[6]))

join = comment.join(post)
newRDD = join.map(lambda x : (x[1],1)) \
            .reduceByKey(lambda a,b : a+b)
edges = newRDD.map(lambda x : (x[0][0], x[0][1], x[1]))
vertices = comments.filter(lambda x : x != headerComments) \
                .map(lambda x : x[1])

####SUBTASK 2
#Convert the result of the previous step into a Spark DataFrame (DF)
#and answer the following subtasks using DataFrame API, namely using Spark SQL



columns = ["src", "dst", "w"]
df = edges.toDF(columns)
df.printSchema()
df.show(truncate=False)

df.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('/test.csv')


####SUBTASK 3
#Find the user ids of top 10 users who wrote the most comments

df.sort(col("w").desc()).show(truncate=False)
