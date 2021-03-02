from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import sys
from graphframes import *
#from functools import reduce

conf = SparkConf().setAppName("project").setMaster("local[*]")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

INPUT_DATA_PATH = sys.argv[1]

# SUBTASK 1
#Create a graph of posts and comments. Nodes are users, and there is an edge from
#node 𝑖 to node 𝑗 if 𝑖 wrote a comment for 𝑗’s post. Each edge has a weight 𝑤𝑖𝑗 that is
#the number of times 𝑖 has commented a post by j
#df_comments = pd.read_csv("projectData/comments.csv.gz", sep='\t')
#df_posts = pd.read_csv("projectData/posts.csv.gz", sep='\t')
#to dataframes:
#1: nodes(userID, postID)
#2: edges(sourceID(userID), destinationID(userID), postID, weight(number of comments))

comments = sc.textFile(INPUT_DATA_PATH + "/comments.csv.gz")
posts = sc.textFile(INPUT_DATA_PATH + "/posts.csv.gz")
headerComments = comments.first()
headerPosts = posts.first()
#x[0]: postid of which the comment is written for, x[4]: userid of the one commenting
comment = comments.filter(lambda x : x != headerComments) \
            .map(lambda lines : lines.split("\t")) \
            .map(lambda x : (x[0],x[4])) \
            .persist()

#x[0]: postID, x[6]: ownerUserid
post = posts.filter(lambda x : x != headerPosts) \
            .map(lambda lines : lines.split("\t")) \
            .map(lambda x : (x[0], x[6])) \
            .persist()


join = comment.join(post)
newRDD = join.map(lambda x : ((x[1]), 1)) \
            .reduceByKey(lambda a,b : a+b) \
            .take(10)
print(newRDD)

    # columns = ["src,dst", "w"]
    # df = newRDD.toDF(columns)
    # df.printSchema()
    # df.show(truncate=False)



# SUBTASK 2
#Convert the result of the previous step into a Spark DataFrame (DF) and answer the
#following subtasks using DataFrame API, namely using Spark SQL
