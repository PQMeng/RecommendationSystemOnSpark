import sys
#import decimal
from pyspark import SparkContext

sc = SparkContext()

testingFile = "file:/home/cloudera/Desktop/cse427s/final_project/TestingRatings.txt"
trainingFile = "file:/home/cloudera/Desktop/cse427s/final_project/TrainingRatings.txt"


def movieRatingPair(record_line):
	MovieID, UserID, Rating = record_line.split(',')
	return (MovieID, Rating)

#return [user ratings per item...]
testDataset  = sc.textFile(testingFile).map(movieRatingPair).groupByKey().mapValues(list).map(lambda x: x[1])#for test#.takeSample(True, 2)
trainDataset = sc.textFile(trainingFile).map(movieRatingPair).groupByKey().mapValues(list).map(lambda x: x[1])#.takeSample(True, 2)

# Get all normRating list [[..][..][..]..]
normTestRatings = [] 
for testRatings in  testDataset:
	sumRating = 0.0
	numRating = 0
	#Get normRating list per movie [...]
	normRating_perMovie = []
	for rating in testRatings :
		rate = float(rating)
		numRating += 1
		sumRating += rate
	for rating in testRatings :	
		rate = float(rating)
		rate -= sumRating / numRating #get normolized rating by - average
		normRating_perMovie.append(rate)
	normTestRatings.append(normRating_perMovie)

normTrainRatings = []
for trainRatings in  trainDataset:
        sumRating = 0.0
        numRating = 0
        #Get normRating list per movie [...]
        normRating_perMovie = []
        for rating in trainRatings :
                rate = float(rating)
                numRating += 1
                sumRating += rate
        for rating in trainRatings :
                rate = float(rating)
                rate -= sumRating / numRating #get normolized rating by - average
                normRating_perMovie.append(rate)
        normTrainRatings.append(normRating_perMovie)

#for test only
'''
for nR in normTestRatings:
	print  nR
for nR in normTrainRatings:
	print nR
'''
