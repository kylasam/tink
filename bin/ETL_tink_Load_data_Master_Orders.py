import psycopg2
from configparser import ConfigParser
from collections import namedtuple
import pandas as pd
import numpy as np
import sys,time
from datetime import datetime
import logging
import matplotlib.pyplot as plt

#LOG_FILE_NAME=datetime.now().strftime('ETL_process_%Y%m%d%H%M%S.log')
CURRENT_DATE=datetime.now().strftime('%Y%m%d')
LOG_FILE_NAME=datetime.now().strftime('../log/ETL_tink_masterLoad_%Y%m%d.log')
pd.set_option('display.max_columns', None)
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
          'Key_column' : config.get('database','Key_column'),
          'store_data_file' : config.get('workspaces','store_data_file'),
          'delta_file': config.get ('workspaces', 'delta_file'),
          'Delta_data_table_source_columns': config.get ('database', 'Delta_data_table_source_columns'),
          'processing_data': config.get ('workspaces', 'processing_data'),
          'plot_path': config.get ('workspaces', 'plot_path'),
          'Master_table_columns': config.get ('database', 'Master_table_columns'),
          'Online_order_table' : config.get ('database', 'Online_order_table'),
          'online_data_file': config.get ('workspaces', 'online_data_file'),
          'Master_data_table' : config.get('database','master_data_table'),
          'Delta_data_table' : config.get('database','Delta_data_table'),
          'Master_data_table_source_columns' : config.get('database','Master_data_table_source_columns'),
          'report_file' : config.get ('workspaces', 'report_file')
          }
    conf_contents = namedtuple("Config", conf_evaluation.keys ()) (*conf_evaluation.values ())
    return conf_contents

def db_connect_postgresql(hostname,username,database,password):
    logging.info('Connecting to the PostgreSQL database...')
    try:
        conn = psycopg2.connect (host=hostname, database=database, user=username, password=password)
        cur = conn.cursor ()
        logging.info ("Postgresql Connection established Successfully...")
        return cur,conn
    except:
        logging.error("Postgresql Connection FAILED!!!!")
        return False

def db_table_setup (table_name,db_cur,db_connection):
    logging.warning("Truncate the table " + '"' + table_name + '"' + " will happen in this phase.")
    QUERY_TRUNC_TABLE = """TRUNCATE TABLE  %s""" % (table_name)
    db_cur.execute (QUERY_TRUNC_TABLE)
    logging.info("Query for Dropping table " + '"' + table_name + '"' + " has EXECUTED SUCESSFULLY!!. Table creation process Initiated ")
    logging.info ("Table " + '"' + table_name + '"' + " TRUNCATED SUCCESSFULLY!!! ")
    db_connection.commit ()

def load_csv_to_postgres_db(type_of_load,table_name,source_file,db_cur,db_connection,Master_table_columns,delta_table_columns):
    CSV_COUNT = sum (1 for line in open (source_file)) - 1
    logging.info(" " + type_of_load + " Process Started")
    logging.info("Columns that will be load in this phase are "  + Master_table_columns + ".")
    logging.info ("In the source file " + source_file + " there were total of " + str (CSV_COUNT) + " lines present excluding Header")
    try:
        LOAD_MSTR_TABLE="COPY %s(%s) FROM '%s' DELIMITER ',' CSV HEADER"  %(table_name,Master_table_columns,source_file)
        db_cur.execute (LOAD_MSTR_TABLE)
        db_connection.commit ()
        # Estimate the Total no.of records loaded in the table successfully!!!
        db_cur.execute("SELECT COUNT(*) FROM %s limit 1" %(table_name))
        TOTAL_RECORDS_LOADED_IN_PSTGRESQL=db_cur.fetchone()
        if (CSV_COUNT != TOTAL_RECORDS_LOADED_IN_PSTGRESQL[0] ):
            logging.info ("Table " + '"' + table_name  + '"' +  " is being loaded with only " + str(TOTAL_RECORDS_LOADED_IN_PSTGRESQL[0]) + " records but in the file we had ." + CSV_COUNT + " records. Halting the process!!!" )
            exit(102)
        else:
            logging.info ("Table " + '"' + table_name  + '"' + " is being loaded with " + str(TOTAL_RECORDS_LOADED_IN_PSTGRESQL[0]) + " records Successfully.")
            logging.info ("Table Load for " + table_name + " COMPLETED SUCCESSFULLY!!!")
            return TOTAL_RECORDS_LOADED_IN_PSTGRESQL[0]

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Table Load process failed with Database Error!")
        print (error)
        exit (109)

def db_extract_databse(db_cur,db_connection,Master_table,report_file_dtls):
    logging.info("Process has kick started for Extract the Master data process....")
    try:
        EXTRACT_QUERY="COPY %s TO '%s' DELIMITER ',' CSV HEADER" %(Master_table,report_file_dtls)
        db_cur.execute (EXTRACT_QUERY)
        db_connection.commit ()
        logging.info ("Table " + '"' + Master_table + '"' + " has been EXPORTED SUCCESSFULLY into a file in the path " + '"' + report_file_dtls + '"' + " without any errors!!!")
    except (Exception, psycopg2.DatabaseError) as error:
         logging.error ("Target Table " + '"' + Master_table + '"' + " EXTRACTION FAILED!!!")
         print (error)
         exit(999)

def data_process(store_file,online_data,delta_file):
    #Apply ETL logic in Store File
    #SOURCE 1
    logging.info ("Source Data processing will Happen in this Phase of Execution!! ")
    logging.info ("Source Name ," + store_file + "Will be processed in this Phase." )
    StoreDataFrame=pd.read_csv(store_file,delimiter=",")
    StoreDataFrame['DispatchStatus']=np.where(StoreDataFrame['order_id']%2==0,'NOT DISPATCHED','DISPATCHED')

    #SOURCE 2
    logging.info ("Source Data processing will Happen in this Phase of Execution!! ")
    logging.info ("Source Name ," + online_data + "Will be processed in this Phase." )
    OnlineOrderDataFrame=pd.read_csv(online_data,delimiter=",")
    OnlineOrderDataFrame['DispatchStatus']=np.where(OnlineOrderDataFrame['order_id']%2==0,'NOT DISPATCHED','DISPATCHED')

    #COMBINING DATA SOURCES
    DeltaDataFrame=pd.concat([StoreDataFrame, OnlineOrderDataFrame])
    DeltaDataFrame.to_csv(delta_file,sep=",",header=False,index=False)

    #perform Downloading the report into a file and Email(If value passed from user console)
    #execute_reporting=db_extract_databse(initialze_db,initialze_conn,config.Master_data_table,config.report_file)

def db_perform_Master_load(db_cur,db_connection,Master_table,Delta_table,Master_table_columns):
    logging.info("Initializing the process for the Data Load from " + '"' + Delta_table + '"' + " to the target table " + '"' + Master_table + '".' )
    logging.info ("Try block gets kicked for the Delta process into the table " + '"' + Master_table + '"' + " from the table " + '"' +  Delta_table + '".')
    try:
        INSERT_QUERY="INSERT INTO %s SELECT %s FROM %s" %(Master_table,Master_table_columns,Delta_table)
        db_cur.execute (INSERT_QUERY)
        db_connection.commit()
        logging.info("Table " + '"' + Master_table + '"' + " has been INSERTED with NEW RECORDS SUCCESSFULLY" )
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Insert process FAILED into the table " + '"' + Master_table + '"' + " for New records")
        print(error)

def db_extract_databse(db_cur,db_connection,Master_table,report_file_dtls):
    logging.info("Process has kick started for Extract the Master data process....")
    try:
        EXTRACT_QUERY="COPY %s TO '%s' DELIMITER ',' CSV HEADER" %(Master_table,report_file_dtls)
        db_cur.execute (EXTRACT_QUERY)
        db_connection.commit ()
        logging.info ("Table " + '"' + Master_table + '"' + " has been EXPORTED SUCCESSFULLY into a file in the path " + '"' + report_file_dtls + '"' + " without any errors!!!")
    except (Exception, psycopg2.DatabaseError) as error:
         logging.error ("Target Table " + '"' + Master_table + '"' + " EXTRACTION FAILED!!!")
         print (error)
         exit(999)

def Visualize_data(report_file,plot_pth):
    ReportDataFrame=pd.read_csv(report_file,sep=",")
    ReportDataFrame.groupby (['order_date', 'order_mode','dispatchstatus']).size ().unstack ().plot (kind='bar', stacked=True)
    plt.savefig(plot_pth + "dispatchstatus.png",bbox_inches = "tight")

    OrderWise=ReportDataFrame[["order_mode","order_value"]]
    OrderWise.groupby (['order_mode']).sum().plot(kind="bar")
    #ReportDataFrame.groupby (['order_mode']).sum().plot(kind="bar")
    plt.xlabel("Order Mode")
    plt.ylabel("Sum of Total Orders")
    plt.title("Sales details for Online Vs Offline stores")
    plt.savefig(plot_pth + "OrderValue.png",bbox_inches = "tight")

    ReportDataFrame.groupby (['order_date','dispatchstatus']).size ().unstack ().plot (kind='bar', stacked=True)
    plt.xlabel("Order Date")
    plt.ylabel("No. Of Items Dispatched")
    plt.title("Dispatch status per day status")
    plt.savefig(plot_pth + "DispatchStatus.png",bbox_inches = "tight")

    ReportDataFrame.groupby (['order_mode', 'brand']).size ().unstack ().plot (kind='bar', stacked=True)
    plt.xlabel ("Order Mode")
    plt.ylabel ("No. Of Items Ordered")
    plt.title ("Frequency of brands ordered in Online Vs Offline")
    plt.savefig (plot_pth + "BrandAnalysis.png", bbox_inches="tight")

    plt.show ()


def main():
    # Get configuration values from external file
    #conf = args.config
    conf ="C:\\Users\\user\\PycharmProjects\\tink\\config.ini"
    config = get_config_details(conf)
    logging.info("Started the ETL process....")

    #Evaluate the Connection details for Postgresql
    initialze_db,initialze_conn=db_connect_postgresql(config.host,config.user,config.database,config.password)

    #Extract ONLINE_ORDER table
    Online_Order_Table=db_extract_databse(initialze_db,initialze_conn,config.Online_order_table,config.online_data_file)

    # Create the Table if not exists; Drop if exists
    db_table_setup(config.Delta_data_table, initialze_db,initialze_conn)
    ETL_Process=data_process(config.store_data_file,config.online_data_file,config.delta_file)

    Delta_load=load_csv_to_postgres_db("Delta Table Load",config.Delta_data_table,config.delta_file,initialze_db,initialze_conn,config.Delta_data_table_source_columns,config.Delta_data_table_source_columns)
    print("Total number of Delta load is =",Delta_load)

    #Load the Delta Records into Master Table
    #Master_Data_Load=db_perform_Master_load(initialze_db,initialze_conn,config.Master_data_table,config.Delta_data_table,config.Master_data_table_source_columns)

    #perform Downloading the report into a file and Email(If value passed from user console)
    execute_reporting=db_extract_databse(initialze_db,initialze_conn,config.Master_data_table,config.report_file)

    visualize_data=Visualize_data(config.report_file,config.plot_path)
if __name__ == '__main__':
 RETVAL = main()
 sys.exit(RETVAL)
