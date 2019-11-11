from configparser import ConfigParser
from collections import namedtuple
from datetime import datetime
import logging
import sys
import os
import time
import webbrowser
import pandas as pd
import matplotlib.pyplot as plt
import re

CURRENT_DATE=datetime.now().strftime('%Y%m%d')
LOG_FILE_NAME=datetime.now().strftime('../log/BAUJobMonitorStreaming_%Y%m%d.log')
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
          'Report_Path': config.get ('workspaces', 'Report_Path'),
          'plot_path' : config.get ('workspaces', 'plot_path'),
          'TargetDataDirectory' :config.get ('workspaces', 'TargetDataDirectory'),
          'WALDirectory' :config.get ('workspaces', 'WALDirectory'),
          'HTML_PATH' :config.get ('workspaces', 'HTML_PATH'),
          'TIME_SPAN_BETWEEN_PUSHES' :config.get ('workspaces', 'TIME_SPAN_BETWEEN_PUSHES'),
          'DASHBOARD_REFRESH_TIME' :config.get ('workspaces', 'DASHBOARD_REFRESH_TIME')
          }
    conf_contents = namedtuple("Config", conf_evaluation.keys ()) (*conf_evaluation.values ())
    return conf_contents

def JobMonitor(config):
    print(config.DataDirectory)
    logging.info ("Checking whether the Source System is pushing Messages or not..")
    WAL_FILE_DIR=os.path.join(config.DataDirectory,os.path.join(config.WALDirectory))
    WAl_FILE=WAL_FILE_DIR+CURRENT_DATE+'_WAL.dat'
    PROCESSED_FILE = os.path.join(config.DataDirectory,os.path.join(config.TargetDataDirectory)) + CURRENT_DATE + '_Processed.dat'
    with open(WAl_FILE) as Datawrites:
        LATEST_PUBLISHRECORD=(list (Datawrites)[-1]).replace("\n","")
        LATEST_PUBLISHRECORD_TIMESTAMP =LATEST_PUBLISHRECORD.split(":")[2]
        EDW_PUBLICATION_ID=(datetime.now ().strftime ('%Y%m%d%H%M%S'))
        if(((datetime.strptime(EDW_PUBLICATION_ID,'%Y%m%d%H%M%S')-datetime.strptime(LATEST_PUBLISHRECORD_TIMESTAMP,'%Y%m%d%H%M%S')).seconds) > int(config.TIME_SPAN_BETWEEN_PUSHES)):
            logging.info("Streaming process is running and data is still pushing into Kafka server")
            print("Data Not Streaming...")
            return "Not Streaming Now",LATEST_PUBLISHRECORD,WAl_FILE,PROCESSED_FILE
        else:
            print("Data Streaming...")
            return "Streaming Now",LATEST_PUBLISHRECORD,WAl_FILE,PROCESSED_FILE

def HTMLReporting(config,streaming_status,HTML_REPORT,LATEST_RECORD,WALFILE,ETLFILE):
    print(HTML_REPORT)
    if(streaming_status=="Streaming Now"):
        Streaming='''<font color =Green size=6 >''' + streaming_status + '''...</font>'''
    else:
        Streaming ='''<font color =Red size =6 >''' + streaming_status + '''</font>'''


    WAL_CONTENT=pd.read_csv(WALFILE,delimiter=":",names=['ID','STATUS','TIMESTAMP'])
    try:
        ETL_CONTENT=pd.read_csv(ETLFILE,delimiter=":")
        ETL_COUNT=ETL_CONTENT.count()['ID']
        ETL_PROCESS='''<font color =Green size=6 >''' + "ETL PROCESS STARTED CONSUMING" + '''...</font>'''
    except:
        logging.info("Processed ETL file might not have been created since, process might not have started consuming")
        ETL_COUNT=0
        ETL_PROCESS = '''<font color =Green size=6 >''' + "ETL PROCESS NOT STARTED.." + '''...</font>'''
    PUBLISHED_RECORD_COUNT=WAL_CONTENT.count()['ID']
    PUBLISHED_FAILURE_COUNT = len(WAL_CONTENT[WAL_CONTENT['STATUS'] == 'FAILURE'])
    PUBLISHED_SUCCESS_COUNT = len (WAL_CONTENT[WAL_CONTENT['STATUS'] == 'SUCCESS'])
    PUBLISHED_TIMESTAMP = LATEST_RECORD.split (":")[2]

    WAL_CONTENT.groupby (['STATUS']).sum().plot(kind="bar")
    plt.xlabel("Status on Published Message into Kafka Messanger")
    plt.ylabel("Status Counts")
    plt.title("Status of Message Publish in Kafka server")
    plt.savefig(config.plot_path + "MessagePubishingStatus.png",bbox_inches = "tight")
    plt.savefig(config.log_path + "\plot\\" + "MessagePubishingStatus.png", bbox_inches="tight")
    plt.close()
    SourceDirectory = config.TargetDataDirectory + "CustomerData" + "_" + CURRENT_DATE + "/"
    contents = []
    #path_to_json = 'C:/Users/user/PycharmProjects/KafkaDataStreaming/data/ProcessedData/CustomerData_20191111'
    json_files = [pos_json for pos_json in os.listdir (SourceDirectory) if pos_json.endswith ('.json')]
    with open (config.Report_Path + '\BAUFILEDATA.json', 'w') as outfile:
        for FILE_NM in json_files:
            FILE_NM_RESOLVED_PATH = (SourceDirectory + '/' + FILE_NM)
            Uniue_elemnt = []
            COuntry_List = []
            with open (FILE_NM_RESOLVED_PATH) as infile:
                for line in infile:
                    values = (line.split (",")[0].split (":")[1].replace ('"', ''))
                    Country = (line.split (",")[5].split (":")[1].replace ('"', ''))
                    Uniue_elemnt.append (values)
                    COuntry_List.append (Country)
                    outfile.write (line)
            NUMBER_OF_UNIQ_IDS=len (set (Uniue_elemnt))
            NUMBER_OF_UNIQ_COUNTRY_LIST=len (set (COuntry_List))
            MAXIMUM_LISTED_COUNTRY= max (COuntry_List, key=COuntry_List.count)
            MINIMUM_LISTED_COUNTRY =min (COuntry_List, key=COuntry_List.count)
            print("....................................",COuntry_List)

            plt.hist(COuntry_List)
            #plt.bar(COuntry_List)
            plt.xlabel ("Country List")
            plt.ylabel ("Frequency(From minimum to Maximum Listed)")
            plt.title ("Country Representation")
            plt.savefig (config.plot_path + "CountryRepresentatio.png",bbox_inches="tight")
            plt.savefig (config.log_path + "\plot\\" + "CountryRepresentatio.png", bbox_inches="tight")
            #plt.show()

    #Below are the code for Adhoc Reporting for the busienss in the Dashboard
    with open (FILE_NM_RESOLVED_PATH) as f:
        contents = f.read ()
        TOTAL_MALE_COUNT = sum (1 for match in re.finditer (r"\bMale\b", contents))
        TOTAL_FEMALE_COUNT = sum (1 for match in re.finditer (r"\Female\b", contents))


    print(PUBLISHED_SUCCESS_COUNT,PUBLISHED_FAILURE_COUNT,PUBLISHED_TIMESTAMP)
    HTML_CONTENT='''<html>
    <head>
        <link rel="stylesh  eet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
        <style>body{ margin:0 100; background:whitesmoke; }</style>
    </head>
    <body>
        <h1 style=\"color:blue;\"style=\"font-size:146%;\"><center><b>Welcome to BAU DataStreaming Dashboard</h1></b></center>
    '''
    HTML_CONTENT+='''<style>
      .blink {
      animation: blinker 0.6s linear infinite;
      color: #1c87c9;
      font-size: 30px;
      font-weight: bold;
      font-family: sans-serif;
      }
      @keyframes blinker {  
      50% { opacity: 0; }
      }
      .blink-one {
      animation: blinker-one 1s linear infinite;
      }
      @keyframes blinker-one {  
      0% { opacity: 0; }
      }
      .blink-two {
      animation: blinker-two 1.4s linear infinite;
      }
      @keyframes blinker-two {  
      100% { opacity: 0; }
      }
    </style><h2><p class="blink">
    <span style='font-size:40px;'>&#10173;
    </span><span style='font-size:40px;'>&#10173;
    </span><span style='font-size:40px;'>&#10173;
    </span> ''' + Streaming +  '''</p></h2> </div>'''
    HTML_CONTENT+='''<style>
                      table {                   
                      font-family: arial, sans-serif;
                      border-collapse: collapse;
                      width: 100%;
                    }                 td, th {
                      border: 1px solid #dddddd;
                      text-align: left;
                      padding: 8px;
                    }
                    
                    tr:nth-child(even) {
                      background-color: #dddddd;
                    }
                    </style><h2>Data Transfer Status:</h2>
                    
                    <table>
                      <tr>
                        <th>S.No</th>
                        <th>NO.OF RECORDS PUBLISHED</th>
                        <th>PUBLISHED SUCCESSFULLY</th>
                        <th>PUBLISHED FAILURES</th>
                        <th>LATEST TIMESTAMP</th>
                        <th>ETL PROCESS COUNT</th>
                      </tr></center>
                      <tr>
                        <td><center>01</center></td>
                        <td><center>''' + str(PUBLISHED_RECORD_COUNT )+  '''</center></td>
                        <td><center>''' + str(PUBLISHED_SUCCESS_COUNT) + '''</center></td>
                        <td><center>''' + str(PUBLISHED_FAILURE_COUNT) + '''</center></td>
                        <td><center>''' + str(PUBLISHED_TIMESTAMP   )  + '''</center></td>
                        <td><center>''' + str(ETL_COUNT)               + '''</center></td>
                      </tr>
                     </table>'''
    PLOT=(config.plot_path + "MessagePubishingStatus.png")
    PLOT2=(config.plot_path + "CountryRepresentatio.png")
    HTML_CONTENT+='''<style>
* {
  box-sizing: border-box;
}

.column {
  float: left;
  width: 30.33%;
  padding: 3px;
}

/* Clearfix (clear floats) */
.row::after {
  content: "";
  clear: both;
  display: table;
}
</style><div class="row"><h3>Visual Analysis on Last Processed Data</h2>
                     <div class="column"> <img src="''' + PLOT + '''" alt="Snow" style="width:100%">
                     </div>
                     <div class="column"> <img src="''' + PLOT2 + '''" alt="Country" style="width:100%">
                     </div>
                     </div>'''
    HTML_CONTENT+='''<br></style><h2>ETL Process Status:</h2>
                    <table>
                      <tr>
                        <th>S.No</th>
                        <th>UNIQ ID's</th>
                        <th>UNIQ COUNTRY</th>
                        <th>MAXIMUM LISTED COUNTRY</th>
                        <th>MINIMUM LISTED COUNTRY</th>
                        <th>MALE COUNTS</th>
                        <th>FEMALE COUNTS</th>
                      </tr></center>
                      <tr>
                        <td><center>01</center></td>
                        <td><center>''' + str(NUMBER_OF_UNIQ_IDS )+  '''</center></td>
                        <td><center>''' + str(NUMBER_OF_UNIQ_COUNTRY_LIST) + '''</center></td>
                        <td><center>''' + str(MAXIMUM_LISTED_COUNTRY) + '''</center></td>
                        <td><center>''' + str(MINIMUM_LISTED_COUNTRY   )  + '''</center></td>
                        <td><center>''' + str(TOTAL_MALE_COUNT)               + '''</center></td>
                        <td><center>''' + str(TOTAL_FEMALE_COUNT)               + '''</center></td>
                      </tr>
                     </table>
                     <br><br><br>'''
    HTML_CONTENT+='''<br><br>'''
    f = open (HTML_REPORT, 'w')
    f.write (HTML_CONTENT)
    f.close ()


def main():
    # Get configuration values from Config file
    conf ="..\config.ini"
    config = get_config_details(conf)
    logging.info("Initiating Stream Data process Monitor System....")
    HTML_REPORT = config.HTML_PATH + "BAU_Monitoring_Process_" + CURRENT_DATE + ".html"
    webbrowser.open (HTML_REPORT)
    while True:
        JobStatus,LATEST_RECORD,WAL_FILE,ETL_FILE=JobMonitor(config)
        Repoting=HTMLReporting(config,JobStatus,HTML_REPORT,LATEST_RECORD,WAL_FILE,ETL_FILE)
        time.sleep(int(config.DASHBOARD_REFRESH_TIME))




if __name__ == '__main__':
 RETVAL = main()
 sys.exit(RETVAL)