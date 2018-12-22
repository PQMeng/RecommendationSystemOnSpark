import sys
import math
from pyspark import SparkContext
from util import user_movie_rating, create_movie_similarity


sc = SparkContext()

args = sys.argv
test_file_path = args[1] # file you want to test
train_file_path = args[2] # file need to create map from
similarity_path = args[3] # output from job1
output_file_path = args[4] # output path
error_outout_path = args[5] # output error and k

# k most similar movies
top_ks = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

# create user rated movie map
# { user_id: [(movie, rating), ...] }
user_id_rating_list = sc.textFile(train_file_path).map(user_movie_rating).groupByKey().mapValues(list)
user_rated_map = user_id_rating_list.collectAsMap()

# output of job1
# [((movie_id_1, movie_id_2), similarity), ...]
movie_similarity = sc.textFile(similarity_path).map(create_movie_similarity).collect()

# store the most similar movies
# { movie_id: [(movie, similarity)] }
similarity_dic = {}
for similarity in movie_similarity:
  if similarity[0][0] not in similarity_dic:
    similarity_dic[similarity[0][0]] = []
  if similarity[0][1] not in similarity_dic:
    similarity_dic[similarity[0][1]] = []

  similarity_dic[similarity[0][0]].append((similarity[0][1], similarity[1]))
  similarity_dic[similarity[0][1]].append((similarity[0][0], similarity[1]))

for movie_id in similarity_dic:
  similarity_list = similarity_dic[movie_id]
  similarity_dic[movie_id] = sorted(similarity_list, key = lambda t: t[1], reverse = True)


# (user_id, movie_id, rating)
testDataset = sc.textFile(test_file_path).map(user_movie_rating)
def predict_rating(pair):
  user_id, (movie_id, true_rating) = pair
  dic = {} # { movie_id: (rank, similarity) }
  most_similar_list = similarity_dic[movie_id]
  for i in range(top_ks[-1]):
    if i < len(most_similar_list):
      similar_movie_id, similarity = most_similar_list[i]
      dic[similar_movie_id] = (i, similarity)

  user_rated_movie = user_rated_map[user_id]

  # declare arrays to store data corresponding to different k
  length = len(top_ks)
  total_similarity = [0] * length
  total_weighted_rating = [0] * length
  predict_rating = [0] * length
  sqr_error = [0] * length

  for movie, rated in user_rated_movie:
    if dic.has_key(movie):
      rank, similarity = dic[movie]

      j = length - 1
      while j >= 0 and rank < top_ks[j]:
        total_similarity[j] += similarity
        total_weighted_rating[j] += rated * similarity
        j = j - 1

  for k in range(length):
    # if the user does not watch enough movies, the divisor can be 0
    # in this case, prediction will be 0
    predict_rating[k] = total_weighted_rating[k] / total_similarity[k] if total_weighted_rating[k] != 0 else 0
    sqr_error[k] = math.pow((predict_rating[k] - true_rating), 2)
  return (movie_id, user_id, true_rating, predict_rating, sqr_error)

# (movie, user, true_rating, [prediction_to_k1, prediction_to_k2, ...])
prediction_rdd = testDataset.map(predict_rating)

# save as text file
prediction_rdd.map(lambda pair: (pair[0], pair[1], pair[2], pair[3])).saveAsTextFile(output_file_path)
prediction = prediction_rdd.collect()

# compute average square error corresponding to k
error_list = [0] * len(top_ks)
for t in prediction:
  for i in range(len(top_ks)):
    error_list[i] += t[4][i]

average_error = [0] * len(top_ks)
for i in range(len(top_ks)):
  average_error[i] = (top_ks[i], error_list[i] / len(prediction))

sc.parallelize(average_error).saveAsTextFile(error_outout_path)