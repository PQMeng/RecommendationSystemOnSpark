# Scalable recommendation systems based on Spark

[![Binder](https://img.shields.io/badge/launch-Jupyter-blue.svg)](https://mybinder.org/v2/gh/GokuMohandas/practicalAI/master)
[![Binder](https://img.shields.io/hexpm/l/plug.svg)](https://github.com/PQMeng/RecommendationSystemOnSpark/blob/master/LICENSE)

This repo was for building scalable recommendation systems, currently containing:

- item-item based collaborative filtering algorithms on [Netflix Prize dataset](https://www.kaggle.com/netflix-inc/netflix-prize-data)
- alternating least squares algorithms on [Movielens dataset](https://grouplens.org/datasets/movielens/)

These projects were both deployed on AWS Elastic MapReduce (EMR) to improve their scalability. Feedback and contribution are welcome.

## Usage
There is a READEME.md file in each filder above, explaining the deployment details and usage, respectively.

## AWS EMR Configuration
Considering the latested movielens dataset containing 27,000,000 ratings and 1,100,000 tag applications applied to 58,000 movies by 280,000 users, It's hard to load all the data into the memory in a single personal computer, on longer the precessing and computations required by the algorithm. AWS allows us to do	some real	cloud computing using Amazon Elastic Map
Reduce (EMR), which offers Hadoop hosted through Amazon Web Services.

Following these steps, you will configure the clusters you need to build a recommendation system using Spark as stated in the above folders.
- step 1. **Set up a AWS account**: https://aws.amazon.com/
- step 2. **Launch an EC2 Instance**. You can find EC2 in the service panel and launch it, and then select an AMI (Amazon Machine Image) used as vietual machien in the cloud. Use anything that qualifies as **Free tier eligible**. Next, you will have to select an instance type. Again, use anything that qualifies as **Free tier eligible**. Finally, launch it You will get prompted to create an Amazon EC2 Key Pair (.pem file). Save it and remember where it is stored. you might need it later. Then launch the instance.
- step 3. **Setup S3 Bucket**. Now go to https://console.aws.amazon.com/s3/ and create an Amazon S3 Bucket by following the instructions here http://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-bucket.html). 

## Reference
1. https://www.codementor.io/jadianes/building-a-recommender-with-apache-spark-python-example-app-part1-du1083qbw
2. https://sites.wustl.edu/neumann/courses/cse427s/fl18/
