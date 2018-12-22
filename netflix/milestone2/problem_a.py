import argparse
from pyspark import SparkContext

sc = SparkContext()
# Parse arguments from command line. 
# Usage: python problem_a.py -input_path file:/home/cloudera/Desktop/netflix/netflix_subset/TestingRatings.txt

parser = argparse.ArgumentParser()
parser.add_argument('-input_path', type = str)
args = parser.parse_args()

def user_ID(record_line):
    MovieID, UserID, Rating = record_line.split(',')
    return UserID

def movie_ID(record_line):
    MovieID, UserID, Rating = record_line.split(',')
    return MovieID

# Calculate distinct users in input_path
dis_users = sc.textFile(args.input_path).map(user_ID).distinct()
# Calculate distinct movies in input_path
dis_movies = sc.textFile(args.input_path).map(movie_ID).distinct()

print("distinct user num in " + args.input_path + " is :" + str(dis_users.count()))
print("distinct movie num in " + args.input_path + " is :" + str(dis_movies.count()))





