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


# ##############################################################
# Task1 - Find the average length of the questions(posts),
#        answers(posts), and comments(comments) in character
# ##############################################################
comments = sc.textFile(INPUT_DATA_PATH + '/comments.csv.gz')
posts = sc.textFile(INPUT_DATA_PATH + '/posts.csv.gz')
numComments = comments.count()-1
numPosts = posts.count()-1
commentsHeader = comments.first()
postsHeader = posts.first()

commentsLength = comments.filter(lambda x : x != commentsHeader) \
                .map(lambda lines: lines.split("\t")) \
                .map(lambda x: base64.b64decode(x[2])) \
                .map(lambda word: len(word)) \
                .reduce(lambda a, b: a+b)

avgLength = commentsLength/numComments


################################################################
post = posts.filter(lambda x : x!=postsHeader) \
                .map(lambda lines: lines.split("\t")) \
                .map(lambda x : (x[1], base64.b64decode(x[5])))

questions = post.filter(lambda x: x[0] == "1")

numQ = questions.count()

lengthQ = questions.map(lambda word: len(word[1])) \
                .reduce(lambda a,b : a+b)

avgLengthQ = lengthQ/numQ
################################################################
numA= (numPosts-numQ)

lengthPost = post.map(lambda word: len(word[1])) \
                .reduce(lambda a,b : a+b)

lengthA = lengthPost-lengthQ

avgLengthA = lengthA/numA

print("\nTask 2.1\n")
print("Average character per comment: " + str(avgLength))
print("Average character per question: " + str(avgLengthQ))
print("Average character per answer: " + str(avgLengthA)+ "\n")




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

print("Task 2.2\n")
print("The first question was asked: " + firstQ[1] + " by the user '" + firstUser[0] + "'")
print("The last question was asked: " + lastQ[1] + " by the user '" + lastUser[0] + "'\n")


###############################################################
#TASK 3 - Find the ids of users who wrote the greatest number of answers and questions.
#         Ignore the user with OwnerUserId equal to -1
###############################################################
posts = sc.textFile(INPUT_DATA_PATH + "/posts.csv.gz")
ids = posts.map(lambda lines:lines.split("\t")) \
        .map(lambda x: (x[6],x[1])) \
        .filter(lambda x: x[0]!="NULL")



A = ids.filter(lambda x: x[1]=="2") \
        .map(lambda user: (user[0],1)) \
        .reduceByKey(lambda a,b: a+b) \
        .sortBy(lambda x: -x[1])

answr= A.first()
usrID1 = answr[0]
nA = answr[1]


Q = ids.filter(lambda x: x[1]=="1") \
        .map(lambda user: (user[0],1)) \
        .reduceByKey(lambda a,b: a+b) \
        .sortBy(lambda x: -x[1])

qstn = Q.first()
usrID2 = qstn[0]
nQ = qstn[1]

print("Task 2.3\n")
print("This user ID wrote the greatest number of answers(" + str(nA) + "): " + str(usrID1))
print("This user ID wrote the greatest number of questions(" + str(nQ) + "): " + str(usrID2) + "\n")

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
print("Task 2.4\n")
print("Number of users who received less than three badges: " + str(totUserCount) + "\n")

#################### Functions for TASK 5 ########################

def mean(List):
    tot = 0
    for i in List:
        tot += float(i)
    mean = tot/len(List)
    return mean

def sDev(List):
    listMean = mean(List)
    dev = 0.0
    for i in range(len(List)):
        dev += (List[i]-listMean)**2
    dev = dev**(1/2.0)
    return dev

def pearsonCC(List1, List2):

    Mean1 = mean(List1)
    Mean2 = mean(List2)
    SDev1 = sDev(List1)
    SDev2 = sDev(List2)

    r1 = 0.0
    for i in range(len(List1)):
        r1 += (List1[i]-Mean1)*(List2[i]-Mean1)

    r2 = SDev1 * SDev2

    r =  r1/r2
    return r


###############################################################
#Calculate the Pearson correlation coefficient (or Pearson???s r)
#between the number of upvotes and downvotes cast by a user.
###############################################################
users = sc.textFile(INPUT_DATA_PATH + "/users.csv.gz")
userHeader = users.first()
user = users.filter(lambda x : x != userHeader) \
        .map(lambda lines:lines.split("\t")) \
        .map(lambda x: (x[0],x[7],x[8])) \
        .filter(lambda x : x[0] != "-1")

upVotes = user.map(lambda x: int(x[1])).collect()
downVotes = user.map(lambda x: int(x[2])).collect()

corr = pearsonCC(upVotes, downVotes)

print("Task 2.5\n")
print("The pearson correlation coefficient between the number of upvotes and downvotes cast by a user: " + str(corr) + "\n")




###############################################################
#Calculate the entropy of id of users (that is UserId column from
#comments data) who wrote one or more comments.
###############################################################

#r=P(x) is the number of occurences of x(as the id of a user)
#divided by the total number of records(or rows) in userID column

comments = sc.textFile(INPUT_DATA_PATH + "/comments.csv.gz")
commentHeader = comments.first()
numRecords = comments.count()-1 #eliminate first column
comment = comments.filter(lambda x : x != commentHeader) \
            .map(lambda lines:lines.split("\t")) \
            .map(lambda x : (x[4],1)) \
            .reduceByKey(lambda a,b: a+b) \
            .filter(lambda x : x[1] > 0) \
            .collect()


def entropy(someList):
    s=0
    for i in someList:
        r = i/numRecords
        s += -r*(np.log(r))
    return s/np.log(2)

newList = [element[1] for element in comment]
ent = entropy(newList)
print("Task 2.6\n")
print("The entropy of the userIDs who wrote one or more comments: " + str(ent) + "\n")
