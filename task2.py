from pyspark import SparkConf, SparkContext
import sys
import base64
import numpy as np

# def decode(someText):
#     a = someText
#     decoded = base64.b64decode(a)
#     return decoded

conf = SparkConf().setAppName("project").setMaster("local[*]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
INPUT_DATA_PATH = sys.argv[1]


    ###############################################################
    #Task1 - Find the average length of the questions(posts),
    #        answers(posts), and comments(comments) in character
    ###############################################################
comments = sc.textFile(INPUT_DATA_PATH + '/comments.csv.gz')
posts = sc.textFile(INPUT_DATA_PATH + '/posts.csv.gz')
numComments = comments.count()-1
commentsHeader = comments.first()
postsHeader = posts.first()

commentsLength = comments.filter(lambda x : x != commentsHeader) \
                .map(lambda lines: lines.split("\t")) \
                .map(lambda x: base64.b64decode(x[2])) \
                .map(lambda word: len(word)) \
                .reduce(lambda a, b: a+b)

avgLength = commentsLength/numComments


################################################################
postLength = posts.filter(lambda x : x!=postsHeader) \
                .map(lambda lines: lines.split("\t")) \
                .map(lambda x : (x[1], base64.b64decode(x[5]))) \
                .persist()

questions = postLength.filter(lambda x: (x[0] == "1", x[1])) \
                .persist()

numQ = questions.count()

lengthQ = questions.map(lambda word: len(word[1])) \
                .reduce(lambda a,b : a+b)

avgLengthQ = lengthQ/numQ
################################################################
answers= postLength.filter(lambda x: (x[0]=="2", x[1])) \
                .persist()

numA = answers.count()

lengthA = answers.map(lambda word: len(word[1])) \
                .reduce(lambda a,b : a+b)
avgLengthA = lengthA/numA

print("Average character per comment: " + str(avgLength))
print("Average character per question: " + str(avgLengthQ))
print("Average character per answer: " + str(avgLengthA))




###############################################################
#TASK 2 - Find the dates when the first and the last questions were asked.
#         Also, find the display name of users who posted those questions
###############################################################
posts = sc.textFile(INPUT_DATA_PATH + "/posts.csv.gz")
users = sc.textFile(INPUT_DATA_PATH + "/users.csv.gz")
dates = posts.map(lambda lines:lines.split("\t")) \
            .map(lambda x: (x[1],x[2], x[6])) \
            .filter(lambda type: type[0]=="1") \
            .sortBy(lambda date: date[1])

lastQ = dates.max(key=lambda x:x[1])
firstQ = dates.first()

userFirst = firstQ[2]
userLast = lastQ[2]
userName = users.map(lambda lines:lines.split("\t")) \
            .map(lambda x: (x[0],x[3]))

firstUser = userName.filter(lambda id: id[0]==userFirst) \
            .map(lambda x:x[1]).collect()
lastUser = userName.filter(lambda id: id[0]==userLast) \
            .map(lambda x:x[1]).collect()

print("The first question was asked: " + firstQ[1] + " by the user '" + firstUser[0] + "'")
print("The last question was asked: " + lastQ[1] + " by the user '" + lastUser[0] + "'")


###############################################################
#TASK 3 - Find the ids of users who wrote the greatest number of answers and questions.
#         Ignore the user with OwnerUserId equal to -1
###############################################################
posts = sc.textFile(INPUT_DATA_PATH + "/posts.csv.gz")
ids = posts.map(lambda lines:lines.split("\t")) \
        .map(lambda x: (x[6],x[1])) \



A = ids.filter(lambda x: (x[0]!="-1",x[1]==2)) \
        .map(lambda user: (user[0],1)) \
        .reduceByKey(lambda a,b: a+b) \
        .sortBy(lambda x: -x[1]) \
        .take(5)

Q = ids.filter(lambda x: (x[0]!="-1",x[1]==1)) \
        .map(lambda user: (user[0],1)) \
        .reduceByKey(lambda a,b: a+b) \
        .sortBy(lambda x: -x[1]) \
        .take(5)
 #top ten greatest number of Q&As
print("This user wrote the greatest number of answers: " + str(A))
print("This user wrote the greatest number of questions: " + str(Q))

###############################################################
#Task4 - Calculate the number of users who
#        received less than three badges
###############################################################
badges = sc.textFile(INPUT_DATA_PATH + "/badges.csv.gz")
users = sc.textFile(INPUT_DATA_PATH + "/users.csv.gz")
numUsers = users.count()-1
numUsersBadge = badges.map(lambda line: line.split("\t")) \
                .map(lambda x: x[0]) \
                .persist()

lessThanThree = numUsersBadge.map(lambda word: (word, 1)) \
                .reduceByKey(lambda a,b: a+b) \
                .filter(lambda badges: badges[1] < 3).count()

usersWithBadge = numUsersBadge.distinct() \
                    .count()

totUserCount = (numUsers - usersWithBadge) + lessThanThree
print("Number of users who received less than three badges: " + str(totUserCount))

#################### Functions for TASK 5 ########################
# Rewrite!!
def mean(List):
    tot = 0
    for i in List:
        tot += float(i)
    mean = total/len(List)
    return mean

def sDev(List):
    listMean = mean(List)
    dev = 0.0
    for i in range(len(List)):
        dev += (List[i]-listMean)**2
    dev = dev**(1/2.0)
    return dev

def pearsonCC(List1, List2):
# First establish the means and standard deviations for both lists.
    xMean = mean(List1)
    yMean = mean(List2)
    xSDev = sDev(List1)
    ySDev = sDev(List2)
    # r numerator
    rNum = 0.0
    for i in range(len(List1)):
        rNum += (List1[i]-xMean)*(List2[i]-yMean)

    # r denominator
    rDen = xSDev * ySDev

    r =  rNum/rDen
    return r


###############################################################
#Calculate the Pearson correlation coefficient (or Pearson’s r)
#between the number of upvotes and downvotes cast by a user.
###############################################################
users = sc.textFile(INPUT_DATA_PATH + "/users.csv.gz")
junk = users.first()
user = users.filter(lambda x : x != junk) \
        .map(lambda lines:lines.split("\t")) \
        .map(lambda x: (x[0],x[7],x[8])) \
        .filter(lambda x : x[0] != "-1")

upVotes = user.map(lambda x: int(x[1])).collect()
downVotes = user.map(lambda x: int(x[2])).collect()

corr = pearsonCC(upVotes, downVotes)

print("The pearson correlation coefficient between the number of upvotes and downvotes cast by a user: " + str(corr))




###############################################################
#Calculate the entropy of id of users (that is UserId column from
#comments data) who wrote one or more comments.
###############################################################

#r=P(x) is the number of occurences of x(as the id of a user)
#divided by the total number of records(or rows) in userID column

comments = sc.textFile(INPUT_DATA_PATH + "/comments.csv.gz")
junk = comments.first()
numRecords = comments.count()-1 #eliminate first column
comment = comments.filter(lambda x : x != junk) \
            .map(lambda lines:lines.split("\t")) \
            .map(lambda x : (x[4],1)) \
            .reduceByKey(lambda a,b: a+b) \
            .filter(lambda x : x[1] > 0) \
            .collect()


def entropy(someList):
    s=0
    for p in someList:
        r = p/numRecords
        s += -r*(np.log(r))
    return s/np.log(2)

newList = [element[1] for element in comment]
ent = entropy(newList)
print("The entropy of the userIDs who wrote one or more comments: " + str(ent))
