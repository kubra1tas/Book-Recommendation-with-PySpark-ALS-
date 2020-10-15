
#from functions.s3Lister import sess
from functions.pyspark_dataPrep import dataPrep
from functions.ALS_wImplicit import als_wImp


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(">>>>>AWS Session has started... !")
    # sess() # THE SESSION MUST BE STARTED WTIH AWS

    print(">>>>>Raw data is acquired...!")
    dataPrep() #TO PROCESS RAW DATA, DATA PREP IS USED

    print(">>>>>Processed data is acquired...!")
    als_wImp() #BASIC RECOM WITH ALS IN PYSPARK

    print(">>>>>Recommendation is done.")

