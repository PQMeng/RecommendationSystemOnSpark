import argparse
from pyspark import SparkContext

sc = SparkContext()

def user_movie_pair(record_line):
    MovieID, UserID, Rating = record_line.split(',')
    return (UserID, MovieID)

def movie_user_pair(record_line):
    MovieID, UserID, Rating = record_line.split(',')
    return (MovieID, UserID)

def cal_overlaping(key_value_pair_fun, sampleNum, test_path, train_path):
    # Get the rated mivies of users in TestingRatings.txt

    # Return a sampled [] containing [movie ID]
    Test_dataset = sc.textFile(test_path).map(key_value_pair_fun).groupByKey().mapValues(list).map(lambda x: x[1]).takeSample(True, sampleNum)

    # Return a [] containing [movie ID]
    Train_dataset = sc.textFile(train_path).map(key_value_pair_fun).groupByKey().mapValues(list).map(lambda x: x[1]).collect()

    # Part 1. Get the estimated average overlap of items for user-user
    test_movie_numOfUser = []

    for test_user in Test_dataset: # For each user in sampled Test_dataset
        test_user_dic = {} # get the movie id and number of user in training set who rated this movie 
        for movie in test_user:
            item_overlap_num = 0
            for train_user in Train_dataset:
                if movie in train_user:
                    item_overlap_num += 1
            test_user_dic[movie] = item_overlap_num
        test_movie_numOfUser.append(test_user_dic)

    print(test_movie_numOfUser)

    overlap_oneUser = []
    for dic in test_movie_numOfUser:
        ave_item_lap = sum(dic.values()) / len(dic)
        overlap_oneUser.append(ave_item_lap)

    overlap_allUser = sum(overlap_oneUser)/len(overlap_oneUser)
    print(overlap_allUser)
    
    return 0

    # Part 2. item-item
    # Just change function from user_movie_pair to movie_user_pair

if __name__ == "__main__":
    # Set arguments
    test_path = 'file:/home/cloudera/Desktop/netflix/netflix_subset/TestingRatings.txt' 
    train_path = 'file:/home/cloudera/Desktop/netflix/netflix_subset/TrainingRatings.txt'
    sampleNum = 10
    cal_overlaping(movie_user_pair, sampleNum, test_path, train_path)
