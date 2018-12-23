These two files:

1. **System Development.ipynb** elaborates every detail of how to develop a recommendation system using ALS algorithm with Saprk

2. **engine.py** is the the Recommender class, in which includes the parameters searched in the development phase in the **System Development.ipynb** file. When deploy this system on AWS EMR, you will need to change the 
```data_path = 's3://rmsmovielens/input/complete'```
to where you store the data in AWS S3. The details of how to use AWS S3 to store the data are in the README.md file in the previous level folder.
