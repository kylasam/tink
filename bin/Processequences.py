
import os
import time
from threading import Thread


def start_Producer():
    os.system ('python Producer.py')

def start_consumer():
    os.system ('python Consumer.py')

def start_ETLProcess():
    os.system ('python ETLProcessing.py')

def start_BAUMonitor():
    os.system ('python BAUMonitor.py')


proc1 = Thread(target=start_Producer)
proc2 = Thread(target=start_consumer)
proc4 = Thread(target=start_ETLProcess)
proc3 = Thread(target=start_BAUMonitor)


proc3.start()
time.sleep(5)
proc1.start()
time.sleep(5)
proc2.start()
time.sleep(5)
proc4.start()


proc1.join()
proc2.join()
proc3.join()
proc4.join()


