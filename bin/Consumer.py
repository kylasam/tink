from configparser import ConfigParser
from collections import namedtuple
from datetime import datetime
import logging
import sys
from kafka import KafkaConsumer
from json import loads
import os
import json
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

CURRENT_DATE=datetime.now().strftime('%Y%m%d')
LOG_FILE_NAME=datetime.now().strftime('../log/KafkaConsumerStreaming_%Y%m%d.log')
logging.basicConfig(filename=LOG_FILE_NAME,
                    format='%(levelname)s::%(asctime)s.%(msecs)03d  From Module = ":%(funcName)s:" Message=> %(message)s.',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def get_config_details(config_path):
    config=ConfigParser()
    config.read(config_path)
    conf_evaluation={'host' : config.get('database','host'),
          'user' : config.get('database','user'),
          'password' : config.get('database','password'),
          'database' : config.get('database','database'),
          'DataDirectory' : config.get('workspaces','DataDirectory'),
          'SourceDataDirectory' : config.get('workspaces','SourceDataDirectory'),
          'log_path': config.get ('workspaces', 'log_path'),
          'plot_path' : config.get ('workspaces', 'plot_path'),
          'TargetDataDirectory' :config.get ('workspaces', 'TargetDataDirectory'),
          'WALDirectory' :config.get ('workspaces', 'WALDirectory'),
          'HTML_PATH' :config.get ('workspaces', 'HTML_PATH'),
          'TIME_SPAN_BETWEEN_PUSHES' :config.get ('workspaces', 'TIME_SPAN_BETWEEN_PUSHES'),
          'DASHBOARD_REFRESH_TIME' :config.get ('workspaces', 'DASHBOARD_REFRESH_TIME'),
          'KAFKA_TOPIC_SPARK_PROCESSING' :config.get ('workspaces', 'KAFKA_TOPIC_SPARK_PROCESSING')
          }
    conf_contents = namedtuple("Config", conf_evaluation.keys ()) (*conf_evaluation.values ())
    return conf_contents


def ConsumerMessages(config):
    print("Cosuming")
    sc = SparkContext (appName="PythonSparkStreamingKafka_RM_01")
    sc.setLogLevel ("WARN")
    ssc = StreamingContext (sc, 60)
    kvs = KafkaUtils.createStream (ssc, 'localhost:9092', 'spark-streaming', {'KafkaSparkDataInestion': 1})
    lines = kvs.map (lambda x: x[1])
    coords = lines.map (lambda line: line)

    def SaverecordsinTempLocation(rdd):
        rdd.foreach (
            lambda rec: open ('C:\\Users\\user\\PycharmProjects\\KafkaDataStreaming\\data\\temp\\temp.txt', "a").write (
                rec + "\n"))
        print ("Rdd written to file")
    coords.foreachRDD (SaverecordsinTempLocation)
    coords.pprint ()
    ssc.start ()
    ssc.awaitTermination ()
def main():
    # Get configuration values from Config file
    conf ="..\config.ini"
    config = get_config_details(conf)
    logging.info("Initiating DataStreaming Process from the Source System....")
    MessageProcessing=ConsumerMessages(config)



if __name__ == '__main__':
 RETVAL = main()
 sys.exit(RETVAL)