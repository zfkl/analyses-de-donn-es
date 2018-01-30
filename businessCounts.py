"""
Simulate business sales of 3 items coming from 3 cities 
Map and Reduce them with Spark Streaming on standalone mode with 2 cores
Use of web sockets to transfer processed data to a web browser
Final user can follow in real time business data on interactive and animated dashboard
import sys
from operator import add
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import numpy as np
import ast
import re
import psutil
from websocket import create_connection
import time
import json

#function to send json data to visualization client
def sendToVisu(time, rdd):
    url = 'ws://'+host+':443/websocket'
    taken = rdd.collect()
    if(len(taken)):
   
       ws = create_connection(url)
       ws.send('{{"produit": {{"produit_A": {0}, "produit_B": {1}, "produit_C": {2}}}, "City": \"{3}\"}}'.format(taken[0][1][0],taken[0][1][1],taken[0][1][2],str(taken[0][0])))
       #ws.send(json.dumps(taken))
       #taken[1],taken[2],taken[3],taken[0]
       ws.close()

   
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: business_count <host> <port number>" 
        print sys.argv[0]
        #file=sys.stderr
        exit(-1)
    try:
      port = int(sys.argv[2])
      host = sys.argv[1]
    except :
      print "Unexpected error:", sys.exc_info()[0]
      print "Usage: business_count <host> <port number>"
      raise

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[2]", "NetworkBusinessCount")
    ssc = StreamingContext(sc, 1)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("localhost", 7777)

    #lines = lines.map(lambda line: ast.literal_eval(line))

    # Get tupples for reduce actions
    sales_Paris = lines.map(lambda line: ast.literal_eval(line)).filter(lambda line: line['City']=="Paris").map(lambda line: (line['City'],(np.array([line['produit']['produit_A'],line['produit']['produit_B'],line['produit']['produit_C']]))))

    sales_Montreal = lines.map(lambda line: ast.literal_eval(line)).filter(lambda line: line['City']=="Montreal").map(lambda line: (line['City'],(np.array([line['produit']['produit_A'],line['produit']['produit_B'],line['produit']['produit_C']]))))

    sales_Pekin = lines.map(lambda line: ast.literal_eval(line)).filter(lambda line: line['City']=="Pekin").map(lambda line: (line['City'],(np.array([line['produit']['produit_A'],line['produit']['produit_B'],line['produit']['produit_C']]))))

    
    #Send data to visual client host 
    BusinessCounts_Paris = sales_Paris.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1].tolist())).foreachRDD(sendToVisu)
    BusinessCounts_Montreal = sales_Montreal.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1].tolist())).foreachRDD(sendToVisu)
    BusinessCounts_Pekin = sales_Pekin.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1].tolist())).foreachRDD(sendToVisu)
    #will add checkpoint later

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
