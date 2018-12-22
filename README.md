<<<<<<< HEAD
hi
=======
# RecommendationSystem
input: (movie-id, user-id, rating) tuples
output: ((movie-id, movie-id), similarity) pairs

# Check how to run the model on src/run.txt

# Algorithms
## 1 preprocessing mapreduce job:
normalization

## 2 MapReduce Jobs
## job1: for every user, find pairs of movies that have been both rated (overlap) --> movie co-occurrence

## job2: for every pair of movies aggregate statistics across all users --> movie similarity



>>>>>>> c59ce8d6ad30ca56e0907a96edb8e514a3a8f64c
