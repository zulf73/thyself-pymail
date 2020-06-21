from pyspark import SparkConf, SparkContext
import pymongo
import pymongo_spark
import smtplib
from operator import add
import sys


def send_email2( mfrom, mto, msg ):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    #Next, log in to the server
    server.login("youremailusername", "password")
    #Send the mail
    #msg = "
    #Hello!" # The /n separates the message from the headers
    server.sendmail(mfrom, mto, msg)
    return mto+' ok'

def send_email( mfrom, mto, msg):
    print('sent ' + msg + ' to '+mto)

## Constants
APP_NAME = " HelloWorld of Big Data"
##OTHER FUNCTIONS/CLASSES
mongo_url = 'mongodb://unique:unique@cluster0.3cmqe.mongodb.net/'
def main(sc, db_dot_collection):
    mfrom = 'myself@whatever.com'
    #textRDD = sc.textFile(filename)
    #words = textRDD.flatMap(lambda x: x.split(' ')).map(lambda to: send_email(mfrom,to,msg)
    #wordcount = words.reduceByKey(add).collect()
    #for wc in wordcount:
    #    print wc[0],wc[1]
    msg = "hello"
    pymongo_spark.activate()
    rdd = (sc.mongoRDD('{0}{1}'.format(mongo_url,db_dot_collection)).map(lambda doc: send_email(mfrom, doc.get('email'), msg)))
    rdd.collect()

if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   # Execute Main functionality
   main(sc, 'test.emailsvc')
