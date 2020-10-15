from pyspark.sql import SparkSession, SQLContext
import pandas as pd
import os
def dataPrep():

    # Root File Paths
    # -----------------
    dir = os.getcwd()
    pathProcessed = (dir + "/Processed_Data/")
    pathRaw = (dir + "/Raw_Data/")

    # Start the session and read the data
    # -----------------------------------
    spark = SparkSession.builder \
        .master('local[*]') \
        .config("spark.driver.memory", "15g") \
        .appName('Recommendation_system').getOrCreate()

    # Read the input data
    # -------------------
    df = spark.read \
        .options(header=False, inferSchema=True) \
        .csv((pathRaw + "September30Data.csv"))  # MUST BE CHANGED WITH EVERY NEW DOCUMENT !!!!

    # Drop any na rows
    df.dropna(how='any')

    # Shape and length of the raw data
    print("Shape of the raw read data : ", (df.count(), len(df.columns)))
    df.printSchema()


    def cust(row):
        return row._c0, row._c1, row._c3, row._c5, row._c6, row._c7, row._c8, row._c9

    cnt = 0

    Event = []
    IP = []
    EventTime = []
    ProductName = []
    ProductPerson = []
    ProductCategory = []
    ProductBrand = []
    ProductPrice = []
    for row in df.rdd.toLocalIterator():
        cnt += 1
        evName, ip, evTime, prName, prPers, prBrn, prCat, prPrc = cust(row)

        if prPers == None or evName[:3] == '"pr':
            continue
        elif evName[:3] == '"ev':
            evName = evName[13:-1]

            ip = ip[6:-1]
            evTime = evTime[15:-1]
            prName = prName[15:-1]
            prPers = prPers[17:-1]
            prBrn = prBrn[16:-1]
            prCat = prCat[19:-1]
            prPrc = prPrc[16:-1]

            Event.append(evName)
            IP.append(ip)
            EventTime.append(evTime)
            ProductName.append(prName)
            ProductPerson.append(prPers)
            ProductBrand.append(prBrn)
            ProductCategory.append(prCat)
            ProductPrice.append(prPrc)

    lst = list(zip(Event, IP, EventTime, ProductName,
                   ProductPerson, ProductBrand, ProductCategory, ProductPrice))
    cols = ['Event', 'IP', 'EventTime', 'ProductName',
            'ProductPerson', 'ProductBrand', 'ProductCategory', 'ProductPrice']
    data = spark.createDataFrame(lst, cols)

    data = data.filter(data.ProductName != 'ul')  # Filter the null lines
    data.toPandas().to_csv((pathProcessed + "September30_Processed.csv"), header=True)