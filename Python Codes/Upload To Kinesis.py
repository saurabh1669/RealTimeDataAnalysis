# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 22:06:02 2019

@author: Pushkar Vengurlekar
"""

import numpy as np
import pandas as pd
import boto3
S3 = boto3.client('s3')

ACCESS_KEY = '<Key>'
SECRET_KEY = '<Secret>'

def create_client(service, region):
    return boto3.client(service, region_name=region,
                        aws_access_key_id = 'AKIAJDSSMV5SLQPM2H5A',
                        aws_secret_access_key = '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')

# function to load data from CSV
def load_data(filename):
    df = pd.read_csv(filename, nrows = 1000)
    return df

def send_kinesis(kinesis_client, kinesis_stream_name, kinesis_shard_count, data):
    kinesisRecords = [] # empty list to store data
    (rows, columns) = data.shape # get rows and columns off provided data
    currentBytes = 0 # counter for bytes
    rowCount = 0 # as we start with the first
    totalRowCount = rows # using our rows variable we got earlier
    sendKinesis = False # flag to update when it's time to send data
    shardCount = 1 # shard counter
    # loop over each of the data rows received
    for _, row in data.iterrows():
        values = ','.join(str(value) for value in row) # join the values togetherby a '|'
        print(values)
#        print('\n')
        encodedValues = bytes(values, 'utf-8') # encode the string to bytes
        # create a dict object of the row
        kinesisRecord = {
            "Data": encodedValues, # data byte-encoded
            "PartitionKey": str(shardCount) # some key used to tell Kinesis which shard to use
            }


        kinesisRecords.append(kinesisRecord) # add the object to the list
        stringBytes = len(values.encode('utf-8')) # get the number of bytes from the string
        currentBytes = currentBytes + stringBytes # keep a running total

        # check conditional whether ready to send
        if len(kinesisRecords) == 500: # if we have 500 records packed up, then proceed
            sendKinesis = True # set the flag

        if currentBytes > 50000: # if the byte size is over 50000, proceed
            sendKinesis = True # set the flag

        if rowCount == totalRowCount - 1: # if we've reached the last record in the results
            sendKinesis = True # set the flag

        # if the flag is set
        if sendKinesis == True:

            # put the records to kinesis
            response = kinesis_client.put_records(
                Records=kinesisRecords,
                StreamName = kinesis_stream_name
            )

            # resetting values ready for next loop
            kinesisRecords = [] # empty array
            sendKinesis = False # reset flag
            currentBytes = 0 # reset bytecount

            # increment shard count after each put
            shardCount = shardCount + 1

            # if it's hit the max, reset
            if shardCount > kinesis_shard_count:
                shardCount = 1

        # regardless, make sure to incrememnt the counter for rows.
        rowCount = rowCount + 1


    # log out how many records were pushed
    print('Total Records sent to Kinesis: {0}'.format(totalRowCount))

def main():

    # start timer
    start = time. time()

    # create a client with kinesis
    kinesis = create_client('kinesis','us-east-2')

    # load in data from the csv
    cols = ['index','host', 'gap1', 'gap2', 'method', 'status', 'status2', \
       'url', 'model', 'gap3', 'time']
    data = pd.read_csv('access_log_sample.csv', names = cols, skiprows = 1)
    data = data[['host','gap1']]

    stream_name = "DataStream"
    stream_shard_count = 1

    send_kinesis(kinesis, stream_name, stream_shard_count, data) # send it!

    # end timer
    end = time. time()

    # log time
    print("Runtime: " + str(end - start))


if __name__ == "__main__":

    # run main
    main()


######## Upload Filesd to S3 Bucket
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

uploaded = upload_to_aws(r'C:\Users\pmven\Google Drive\1. myDocs\MSBA\MSBA 6330 Big Data\Trends\s3\log_sample.csv',\
                         "trends-markerplace", 'trends-markerplace/logs/logs.csv')



#### Upload to Redshift Cluster
query = "copy public.Trends1 from 's3://trends2/logs1/data1.csv' credentials'aws_access_key_id=AKIAIYFVEFO4R72OS7LA;aws_secret_access_key=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' csv;"

q3 = 'call simple_loop_when(10);'
q2 = 'select * from viz_1 limit 5'
connection = pg.connect(dbname= 'dev', \
                         host = 'redshift-cluster-2.ciolmv48cipb.us-east-1.redshift.amazonaws.com', \
                         port= '5439', \
                         user= 'alok', password= 'Alok-1234')

cursor = connection.cursor()

cursor.execute(q3)


data = pd.DataFrame(cursor.fetchall())

cursor.close
connection.close()
