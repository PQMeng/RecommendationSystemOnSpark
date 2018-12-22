import math

# create (movie_id, rating) pair
def movie_rating(record_line):
  MovieID, UserID, Rating = record_line.split(',')
  return (int(MovieID), int(float(Rating)))

# create (user, movie, rating) pair
def user_movie_rating(record_line):
  movie_id, user_id, rating = record_line.split(',')
  return (int(user_id), (int(movie_id), int(float(rating))))

# create ((id_1, id_2), similarity) pair
def create_movie_similarity(record_line):
  new_line = record_line.replace('(','').replace(')', '').replace(' ', '')
  movie_id_1, movie_id_2, similarity = new_line.split(',')
  return ((int(movie_id_1), int(movie_id_2)), float(similarity))

# compute similarity
# (id_1, id_2, similarity)
def calculate_similarity((pair, index)):
  (movie_id_1, movie_id_2), rating_list = pair
  numerator = 0
  r1_sqr_accumulator = 0
  r2_sqr_accumulator = 0
  for ((r1, r1_sqr), (r2, r2_sqr), r1_r2) in rating_list:
    numerator += round(r1_r2, 3)
    r1_sqr_accumulator += round(r1_sqr, 3)
    r2_sqr_accumulator += round(r2_sqr, 3)

  if (math.sqrt(r1_sqr_accumulator) * math.sqrt(r2_sqr_accumulator)) == 0:
    similarity = 0
    return (movie_id_1, movie_id_2, similarity)
  similarity = numerator / (math.sqrt(r1_sqr_accumulator) * math.sqrt(r2_sqr_accumulator))
  similarity = round(similarity, 3)
  return (movie_id_1, movie_id_2, similarity)
