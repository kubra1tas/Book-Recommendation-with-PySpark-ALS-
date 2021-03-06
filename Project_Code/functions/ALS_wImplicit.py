# -*- coding: utf-8 -*-
"""Recommender_wImplicit.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1EzteK2tcIvqhj0lZ6WZwVzgMcQXBugIm
"""

# !pip install pyspark

import warnings
warnings.simplefilter(action='ignore', category=Warning)
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import desc
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
# from google.colab import files
# from google.colab import drive
# drive.mount("/content/drive")
import logging
import os
logging.getLogger("py4j").setLevel(logging.ERROR)

#Processed Data Path
dir = os.getcwd()
path =(dir + "/Processed_Data/")

def als_wImp():



    # Start the session and read the data
    # -----------------------------------
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "15g") \
        .appName('Recommendation_system').getOrCreate()

    # Read and Show the data
    # ------------
    df = spark.read \
        .options(header=False) \
        .csv((path + 'JulAugSep_Processed.csv'))

    df = df.dropna()
    df.show(10, truncate=True)
    print("Shape of the processed data : ", (df.count(), len(df.columns)))

    df = df.withColumnRenamed("_c3", "user")
    df = df.withColumnRenamed("_c5", "item")

    df = df.groupBy("user", "item", "_c6", "_c7", "_c8").count().orderBy(desc('count'))
    df = df.withColumnRenamed("count", "rating")
    df.show(10, truncate=False)

    # Filter data to eliminate seldom visitors
    df = df.filter(df.rating > 2)
    print(">>>>>Users with less than 2 visits are eliminated and statistics about remaining data")
    df.describe().show()



    # String Indexer
    # --------------

    # Define a list of stages in your pipeline. The string indexer will be one stage
    stages = []
    categoricalColumns = [item[0] for item in df.dtypes if
                          item[1].startswith('string')]  # Stack all string type columns into
    # categoricalColumns

    for categoricalCol in categoricalColumns:
        # create a string indexer for those categorical values and assign a new name including the word 'Index'
        stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + 'Index', handleInvalid="keep")

        # Append the string Indexer to our list of stages
        stages += [stringIndexer]

    # Create the pipeline. Assign the stages list to the pipeline key word stages
    pipeline = Pipeline(stages=stages)
    # fit the pipeline to our dataframe
    pipelineModel = pipeline.fit(df)
    # transform the dataframe
    df = pipelineModel.transform(df)
    # df.show(10, truncate=False)

    df.orderBy(desc('itemIndex')).show(50, truncate = False)


    (train, test) = df.randomSplit([0.7, 0.3], seed=0)

    # Total number of the users
    print("Training Data Size :", train.count(), '\n' ,"Test Data Size: ", test.count())
    train.show(10, truncate=False)

    train = train.withColumnRenamed("item", "ProductName")
    train = train.withColumnRenamed("user", "IP")
    train = train.withColumnRenamed("userIndex", "user")
    train = train.withColumnRenamed("itemIndex", "item")


    # Model Training
    #--------------

    #GridSearchCV will be implemented for optimization
    als = ALS(rank=5, maxIter=5, alpha=float(10), implicitPrefs=True, seed=0)
    model = als.fit(train)


    #Model Evaluation
    test = test.withColumnRenamed("item", "ProductName")
    test = test.withColumnRenamed("user", "IP")
    test = test.withColumnRenamed("userIndex", "user")
    test = test.withColumnRenamed("itemIndex", "item")
    model.userFactors.orderBy("id").collect()

    #predictions = sorted(model.transform(test).collect(), key=lambda r: r[0])

    user_recs = model.recommendForAllUsers(15)
    # item_recs = model.recommendForAllItems(3)
    # item_recs.where(item_recs.item == 2).select("recommendations.user", "recommendations.rating").collect()

    user_recs.where(user_recs.user == 0).select("recommendations.item", "recommendations.rating").collect()


    print(">>>>>User history:")
    test.filter(test.user == 0).show()


    first = user_recs.where(user_recs.user == 0).select("recommendations.item").collect()
    bound = 3
    final = 0
    print(">>>>Recommended items are listed as follows:")
    while final < 6:
        for item in first:
            for i in range(0, bound):
                # print('item:', item[0][i], '\n')
                all = test.filter(test.item == item[0][i])
                if len(all.collect()) == 0:
                    continue

                else:
                    test.filter(test.item == item[0][i]).show()

                final += 1

    #RMSE Error Measurement will be implemented...!


