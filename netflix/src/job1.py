import math
import sys
from pyspark import SparkContext
from util import movie_rating, calculate_similarity, user_movie_rating

sc = SparkContext()

args = sys.argv
training_file_path = args[1] # training file input
output_file_path = args[2] # output path
trainDataset = sc.textFile(training_file_path).map(movie_rating).groupByKey().mapValues(list).sortByKey().collect()


# create a dictionary to store average rating per movie_id
# { movie_id: average }
movie_rating_dic = {}

for (movie_id, rating_list) in trainDataset:
  total = 0
  for rating in rating_list:
    total += rating
  average = float(total) / len(rating_list)
  average = round(average, 3)
  movie_rating_dic[movie_id] = average


# calculate item-item similarity

# (user_id, [(movie_id, rating)])
user_id_rating_list = sc.textFile(training_file_path).map(user_movie_rating).groupByKey().mapValues(list)

# create data needed to compute pearson correlation
# [(r1, r1^2), (r2, r2^2), r1*r2), (), ...]
def movie_pearson((pair, index)):
  user_id, movie_rating_list = pair
  res = []
  for i in range(len(movie_rating_list)):
    movie_id_1, rating_1 = movie_rating_list[i]
    for j in range(i + 1, len(movie_rating_list)):
      movie_id_2, rating_2 = movie_rating_list[j]
      normalized_rating_1 = rating_1 - movie_rating_dic[movie_id_1]
      normalized_rating_2 = rating_2 - movie_rating_dic[movie_id_2]
      res.append(((movie_id_1, movie_id_2), ((normalized_rating_1, math.pow(normalized_rating_1, 2)), (normalized_rating_2, math.pow(normalized_rating_2, 2)), normalized_rating_1 * normalized_rating_2)))
  return res

# ((movie_id_1, movie_id_2), [])
movie_pairs_list = user_id_rating_list.zipWithIndex().flatMap(movie_pearson).groupByKey().mapValues(list).zipWithIndex()

# ((movie_id_1, movie_id_2), similarity)
movie_similarity = movie_pairs_list.map(calculate_similarity)

# save result to file
movie_similarity.saveAsTextFile(output_file_path)

