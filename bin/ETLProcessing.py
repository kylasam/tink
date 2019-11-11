import pyspark
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark import SparkContext
from configparser import ConfigParser
from collections import namedtuple
from pyspark.sql.functions import *
import Schema.Message_Schema as JSON_SCHEMA
from pyspark.sql.types import StringType
from netaddr import valid_ipv4
from datetime import datetime
import os
import logging
import time

CURRENT_DATE=datetime.now().strftime('%Y%m%d')
LOG_FILE_NAME=datetime.now().strftime('../log/KafkaETLProcessing_%Y%m%d.log')
logging.basicConfig(filename=LOG_FILE_NAME,
                    format='%(levelname)s::%(asctime)s.%(msecs)03d  From Module = ":%(funcName)s:" Message=> %(message)s.',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def get_config_details(config_path):
    config=ConfigParser()
    config.read(config_path)
    conf_evaluation={'SourceDataDirectory' : config.get('workspaces','SourceDataDirectory'),
          'log_path': config.get ('workspaces', 'log_path'),
          'plot_path' : config.get ('workspaces', 'plot_path'),
          'TargetDataDirectory' :config.get ('workspaces', 'TargetDataDirectory'),
          'DataDirectory': config.get ('workspaces', 'DataDirectory'),
          'WALDirectory' :config.get ('workspaces', 'WALDirectory'),
          'Temp_File' :config.get ('workspaces', 'Temp_File'),
          'HTML_PATH' :config.get ('workspaces', 'HTML_PATH'),
          'WAITTIME': config.get ('workspaces', 'WAITTIME'),
          'TIME_SPAN_BETWEEN_PUSHES' :config.get ('workspaces', 'TIME_SPAN_BETWEEN_PUSHES'),
          'DASHBOARD_REFRESH_TIME' :config.get ('workspaces', 'DASHBOARD_REFRESH_TIME'),
          'MOVE_TEMP_FILE_TO_OTHER_NAME_FOR_PROCESSING' :config.get ('workspaces', 'MOVE_TEMP_FILE_TO_OTHER_NAME_FOR_PROCESSING'),
          'KAFKA_TOPIC_SPARK_PROCESSING' :config.get ('workspaces', 'KAFKA_TOPIC_SPARK_PROCESSING')
          }
    conf_contents = namedtuple("Config", conf_evaluation.keys ()) (*conf_evaluation.values ())
    return conf_contents

def IP_ADDRESS_VALIDATE(ip):
    if(valid_ipv4(ip)):
        return ip
    else:
        return "000.00.00.000"

ip_address_validat= udf(lambda z: IP_ADDRESS_VALIDATE(z), StringType())


conf ="..\config.ini"
config = get_config_details(conf)

sc = SparkContext (appName="Streamprocessing")
sc.setLogLevel ("WARN")

sqlContext = pyspark.SQLContext(sc)

def ETLProcessing(config):
    CURRENT_DATE = datetime.now ().strftime ('%Y%m%d')
    WAL_FILE_DIR = os.path.join (config.DataDirectory, os.path.join (config.WALDirectory))
    WAl_FILE = WAL_FILE_DIR + CURRENT_DATE + '_WAL.dat'
    while True:
        with open(WAl_FILE) as Datawrites:
            LATEST_PUBLISHRECORD=(list (Datawrites)[-1]).replace("\n","")
            LATEST_PUBLISHRECORD_TIMESTAMP =LATEST_PUBLISHRECORD.split(":")[2]
            EDW_PUBLICATION_ID=(datetime.now ().strftime ('%Y%m%d%H%M%S'))
            print(datetime.strptime(EDW_PUBLICATION_ID,'%Y%m%d%H%M%S'),datetime.strptime(LATEST_PUBLISHRECORD_TIMESTAMP,'%Y%m%d%H%M%S'),int(config.MOVE_TEMP_FILE_TO_OTHER_NAME_FOR_PROCESSING))
            if(((datetime.strptime(EDW_PUBLICATION_ID,'%Y%m%d%H%M%S')-datetime.strptime(LATEST_PUBLISHRECORD_TIMESTAMP,'%Y%m%d%H%M%S')).seconds) < int(config.MOVE_TEMP_FILE_TO_OTHER_NAME_FOR_PROCESSING) or ((datetime.strptime(EDW_PUBLICATION_ID,'%Y%m%d%H%M%S')-datetime.strptime(LATEST_PUBLISHRECORD_TIMESTAMP,'%Y%m%d%H%M%S')).seconds) > int(config.TIME_SPAN_BETWEEN_PUSHES)):
                logging.info("Streaming process is running and data is still pushing into Kafka server")
                print("Data Not Streaming...")
                if(os.stat(config.Temp_File + '/temp.txt').st_size == 0):
                    print("No data has been streamed in the file. Quitting the ETL process..")
                    logging.info("No data has been streamed in the file. Quitting the ETL process at" + EDW_PUBLICATION_ID)
                else:
                     os.rename(config.Temp_File + '/temp.txt',config.Temp_File + '/toconsumenow.txt')
                     open (config.Temp_File + '/temp.txt', 'a').close ()
                     ProcessDataFrame = sqlContext.read.json (config.Temp_File + '/toconsumenow.txt', schema=JSON_SCHEMA.MessageSchmea)
                     ProcessDataFrame = ProcessDataFrame.withColumn ('Country_N', initcap (col ("country"))) \
                                                            .withColumn ("Date_N", date_format (to_date (col ("date"), "dd/MM/yyyy"), "yyyy-MM-dd")) \
                                                            .withColumn ("ip_address_N", ip_address_validat ("ip_address")).drop ("ip_address", "country", "date") \
                                                            .withColumnRenamed ("Country_N", "Country").withColumnRenamed ("Date_N", "Date").withColumnRenamed ("ip_address_N", "ip_address")
                     ProcessDataFrame.printSchema ()
                     ProcessDataFrame.show ()
                     TargetDirectory = config.TargetDataDirectory + "\CustomerData" + "_" + CURRENT_DATE + "\\"

                     #ProcessDataFrame.coalesce (1).write.format ('json').mode ('append').save (TargetDirectory)
                     ProcessDataFrame.coalesce(1).write.format('json').mode('append').save(TargetDirectory)
                     os.remove(config.Temp_File + '/toconsumenow.txt')
            else:
                print("Data Streaming now. Waiting for few secs to try Renaming")
        time.sleep(int(config.WAITTIME))




DataProcessing=ETLProcessing(config)