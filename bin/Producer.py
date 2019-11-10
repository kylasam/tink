from configparser import ConfigParser
from collections import namedtuple
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer
import logging
import sys
import os
import json
import time

CURRENT_DATE=datetime.now().strftime('%Y%m%d')
LOG_FILE_NAME=datetime.now().strftime('../log/DataStreamingProducer_%Y%m%d.log')
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
          'TargetDataDirectory' :config.get ('workspaces', 'TargetDataDirectory'),
          'WALDirectory' :config.get ('workspaces', 'WALDirectory'),
          'TIME_BETWEEN_SUCCESSSIVE_PUSHES' : config.get ('workspaces', 'TIME_BETWEEN_SUCCESSSIVE_PUSHES'),
          'KAFKA_TOPIC_SPARK_PROCESSING' : config.get ('workspaces', 'KAFKA_TOPIC_SPARK_PROCESSING')
          }
    conf_contents = namedtuple("Config", conf_evaluation.keys ()) (*conf_evaluation.values ())
    return conf_contents

def CleanWAL(config):
    logging.info("Cleansing Process Initiated in this process...")
    logging.info("WAL Files which has been created in the previous run will be deleted in this phase..")
    WAL_FILE_DIR=os.path.join(config.DataDirectory,os.path.join(config.WALDirectory))
    WAl_FILE=WAL_FILE_DIR+CURRENT_DATE+'_WAL.dat'
    try:
        logging.info("Delete any WAL files exists for the day!!!")
        os.remove(WAl_FILE)
        logging.info("WAL FILES has been removed for the day SUCCESSFULLY!!!")
    except:
        logging.warning("WARNING!!!Can't cleanse the WAL file for the day!!!. File May not exists!!!")
    finally:
        return WAl_FILE

def ParseFileNames(config_details,WALFile):
    producer = KafkaProducer (bootstrap_servers=['localhost:9092'],value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
    IN_FILE_NM=(os.path.join(config_details.DataDirectory,os.path.join(config_details.SourceDataDirectory)) + 'SourceData.txt')
    with open(IN_FILE_NM) as FILE_CONTENTS:
        DATA_1=json.load(FILE_CONTENTS)
        with open(WALFile,"at") as WAL_FILE:
            logging.info ("Data Will be Published into Kafka From now...")
            logging.info ("Please check the status file for data push status in LOC=" + WALFile)

            for DATA in DATA_1:
                print(DATA)
                try:
                    producer.send("KafkaSparkDataInestion",value=DATA)
                    #producer.send("KafkaSparkDataInestion",value={"name" : "kylash"})
                    EDW_PUBLICATION_ID = str (datetime.now ().strftime ('%Y%m%d%H%M%S'))
                    WAL_FILE.write ("[id]=" + str (DATA['id']) + ":SUCCESS:" + EDW_PUBLICATION_ID + '\n')
                    WAL_FILE.flush()
                    time.sleep(int(config_details.TIME_BETWEEN_SUCCESSSIVE_PUSHES))
                except:
                    EDW_PUBLICATION_ID = str (datetime.now ().strftime ('%Y%m%d%H%M%S'))
                    WAL_FILE.write ("[id]=" + str (DATA['id']) + ":FAILURE:" + EDW_PUBLICATION_ID + '\n')
                    WAL_FILE.flush()
                    time.sleep (int (config_details.TIME_BETWEEN_SUCCESSSIVE_PUSHES))

    #   WAL_FILE.close()



def main():
    # Get configuration values from Config file
    conf ="..\config.ini"
    config = get_config_details(conf)
    logging.info("Initiating DataStreaming Process from the Source System....")


    WALFile=CleanWAL(config)
    MessagingData=ParseFileNames(config,WALFile)













if __name__ == '__main__':
 RETVAL = main()
 sys.exit(RETVAL)