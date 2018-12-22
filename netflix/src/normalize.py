import sys
#import decimal
from pyspark import SparkContext

sc = SparkContext()

trainingFile = "file:/home/cloudera/Desktop/cse427s/final_project/TrainingRatings.txt"


def movieRatingPair(record_line):
	MovieID, UserID, Rating = record_line.split(',')
	return (MovieID, Rating)

#return [user ratings per item...]
trainDataset = sc.textFile(trainingFile).map(movieRatingPair).groupByKey().mapValues(list).collect()
#print trainDataset
# Get all normRating list [[..][..][..]..]

normTrainRatings = {}   #dictionary {"movie": averating}
for trainMovie,trainRatings in  trainDataset:
        sumRating = 0.0
        numRating = 0
        for rating in trainRatings:
		rate = float(rating)
		numRating += 1
		sumRating += rate
	
	ave = sumRating / numRating  #calculate average
	normTrainRatings[trainMovie] = ave
print normTrainRatings
	
      
