from pyspark import SparkConf, SparkContext
import smtplib
from operator import add
import sys


def send_email( mfrom, mto, msg ):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    #Next, log in to the server
    server.login("youremailusername", "password")
    #Send the mail
    #msg = "
    #Hello!" # The /n separates the message from the headers
    server.sendmail(mfrom, mto, msg)
    return mto+' ok'

## Constants
APP_NAME = " HelloWorld of Big Data"
##OTHER FUNCTIONS/CLASSES

def main(sc,filename):
    mfrom = 'myself@whatever.com'
   textRDD = sc.textFile(filename)
   words = textRDD.flatMap(lambda x: x.split(' ')).map(lambda to: send_email(mfrom,to,msg)
   wordcount = words.reduceByKey(add).collect()
   for wc in wordcount:
      print wc[0],wc[1]

if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   filename = sys.argv[1]
   # Execute Main functionality
   main(sc, filename)