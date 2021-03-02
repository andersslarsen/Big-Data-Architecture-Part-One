from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys


conf = SparkConf().setAppName("project").setMaster("local[*]")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

#Directory path from argument
INPUT_DATA_PATH = sys.argv[1]


#Load datasets to RDDs

badgesRDD = sc.textFile(INPUT_DATA_PATH + "/badges.csv.gz")
commentsRDD = sc.textFile(INPUT_DATA_PATH + "/comments.csv.gz")
postsRDD = sc.textFile(INPUT_DATA_PATH + "/posts.csv.gz")
usersRDD = sc.textFile(INPUT_DATA_PATH + "/users.csv.gz")

#Calculate number of rows
badgesRows = badgesRDD.count() - 1
commentsRows = commentsRDD.count() - 1
postsRows = postsRDD.count() - 1
usersRows = usersRDD.count() - 1


print("The number of rows in the badges RDD: " + str(badgesRows))
print("The number of rows in the comments RDD: " + str(commentsRows))
print("The number of rows in the posts RDD: " + str(postsRows))
print("The number of rows in the users RDD: " + str(usersRows))
