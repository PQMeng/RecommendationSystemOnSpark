import sys
from pyspark import SparkContext

sc = SparkContext()
def movieID_userID(recordLine):
	[movieID, userID, rating] = recordLine.split(',')
	return (movieID, userID)

def userID_movieID(recordLine):
	[movieID, userID, rating] = recordLine.split(',')
	return (userID, movieID)

def userID(recordLine):
	[movieID, userID, rating] = recordLine.split(',')
	return userID

def movieID(recordLine):
	[movieID, userID, rating] = recordLine.split(',')
	return movieID

trainingFile = "file:/home/cloudera/Desktop/cse427s/final_project/TrainingRatings.txt"
testingFile = "file:/home/cloudera/Desktop/cse427s/final_project/TestingRatings.txt"


sampleNum = 20

trainingUMPair = sc.textFile(trainingFile).map(userID_movieID).groupByKey().mapValues(list)
# train: [user,[list of movies]] 
trainingMUPair = sc.textFile(trainingFile).map(movieID_userID).groupByKey().mapValues(list)
# train: [movie,[list of users]]
testSampleUsers = sc.textFile(testingFile).map(userID).distinct().takeSample(True, sampleNum)
# test: sample [list of user]
testSampleMovies = sc.textFile(testingFile).map(movieID).distinct().takeSample(True, sampleNum)

# user similarity
oneAvePerUserList = [] #store the ave per user
for testUser in testSampleUsers:
	#print "user:" + testUser
	movies = trainingUMPair.lookup(testUser)
	#print "movies in training set is : "
	#print movies
	sumOverlapUser = 0
	for movie in movies[0]:
		userList = trainingMUPair.lookup(movie)
		sumOverlapUser += len(userList[0])
		if testUser in userList[0]:
			sumOverlapUser -= 1 #except self
	print "sumOverlapUser is: " + str(sumOverlapUser)
	print "lengh of movies is: " + str(len(movies[0])) 
	oneAve = sumOverlapUser / len(movies[0])
	oneAvePerUserList.append(oneAve)
print "oneAvePerUserList is: "
print oneAvePerUserList

allUserOverlap = sum(oneAvePerUserList) / len(oneAvePerUserList)		
print "users overlap is:" + str(allUserOverlap)

# item similarity

oneAvePerMovieList = []
for testMovie in testSampleMovies:
	users = trainingMUPair.lookup(testMovie)
	sumOverlapMovie = 0
	for user in users[0]:
		movieList = trainingUMPair.lookup(user)
		sumOverlapMovie += len(movieList[0])
		if testMovie in movieList[0]:
			sumOverlapMovie -= 1 #except selfi
	oneAve = sumOverlapMovie / len(users[0])
	oneAvePerMovieList.append(oneAve)
print "oneAvePerMovieList is: "
print oneAvePerMovieList

allMovieOverlap = sum(oneAvePerMovieList) / len(oneAvePerMovieList)
print "movies overlap is:" + str(allMovieOverlap)

