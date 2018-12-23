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
- step 3. **Setup S3 Bucket**. 
    >1).Now go to https://console.aws.amazon.com/s3/ and create an Amazon S3 Bucket by following the instructions here http://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-bucket.html). <br>
    2).Once	your S3 bucket is setup correctly, click on the bucket name and	create two directories called ```input``` and ```logs```.<br>
    3). Navigate to the input directory (you can always access your buckets via the S3 console https://console.aws.amazon.com/s3/)	and create two floders ```input``` to store dataset and ```logs``` to store logs from Hadoop Master node.

- step 4. **Launch the Cluster**. 

    >1). Go to https://console.aws.amazon.com/elasticmapreduce/ and click **"Create Cluster"**<br>
    2). You can name the cluster as you want.<br>
    3). Enable logging and select the directory you created earlier called logs.<br>
    4). Under Software Configuration, select the latest version available and click **Spark** to run a Spark application<br>
    5). Use default Hardware Configuration values.<br>
    6). Under Security and Access select the EC2 Key Pair you created earlier.<br>
    7). Leave the rest of the values at their default selections. Press **"Create Cluster"**.<br>
    8). It will take a few minutes for the cluster to be provisioned and eventually start.
    **Note**: If you get an error message saying that the instance type is not available, create the cluster again using a different instance type (e.g. m4.large) under Hardware Confirguration. Now, you can choose to either submit a MR or Spark job.

- step 5. **Preparing and Submitting a Spark job**

    >1). From the EMR console https://console.aws.amazon.com/elasticmapreduce/, select your cluster and then select the **"Steps"** tab.<br>
    2). Select the blue button **"Add step"**<br>
    3). type key words like this and you will run the Spark job successfully.
    


## Reference
1. https://www.codementor.io/jadianes/building-a-recommender-with-apache-spark-python-example-app-part1-du1083qbw
2. https://sites.wustl.edu/neumann/courses/cse427s/fl18/
