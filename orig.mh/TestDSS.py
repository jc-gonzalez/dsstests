# TestDSS.py
# Script that generates random text files and ingests them into DSS, and then copies them to other SDCs
#    All local file and DSS transactions are timed and logged for later analysis, 
#    and the local data files are deleted on exit (the parameters are recorded to allow identical reruns)

import os
import collections
import datetime
import time
import uuid
from TestParameters import *     # Import the seed, file size, number of files, and SDC name variables

# Function that generates the fake data by randomly mashing the input text
def fakedata(words):                    
    a = collections.deque(words)
    b = collections.deque(seed)
    while True:
       	yield ' '.join(list(a)[0:1024])
        a.rotate(int(b[0]))
        b.rotate(1)

# Function that generates the fake files
def generatefiles(file_size, number_of_files, file_location, file_type):
    words = open("words.txt", "r").read().replace("\n", '').split()    # Get the input text
    for x in xrange(number_of_files):          # Loop through to generate the number of files specified
        g = fakedata(words)
        file_name = file_location + sdc_name + "-" + file_type + "-test" + str(x) + str(uuid.uuid4().get_hex().upper()[0:8]) + ".out"
        out_file = open(file_name, 'w')
        while os.path.getsize(file_name) < file_size:
             out_file.write(g.next())
        out_file.close()

# Function stores files in the DSS
def storefiles(file_location, file_type):
     file_list = os.listdir(file_location)
     storeTimes = []
     log_file.write("Start storing " + file_type + '\n')
     log_file.write("File times: ")
     storeStartTime = int(round(time.time() * 1000))
     for file in file_list:
         dss_store_command = dss_client_location + " operation=store server=" + store_dss_url + " filename=" + file_location + file + " username=" + dss_username + " password=" + dss_password
         startTime = int(round(time.time() * 1000))
         os.system(dss_store_command)
         endTime = int(round(time.time() * 1000))
         log_file.write(str(endTime - startTime) + ',')
         storeTimes.append(endTime - startTime)
     log_file.write("\n")
     storeEndTime = int(round(time.time() * 1000))
     log_file.write("Total Store Time for " + file_type + ": " + str(storeEndTime - storeStartTime) + 'ms' + '\n')
     log_file.write("Average Store Time for " + file_type + ": " + str((float(sum(storeTimes))/float(len(storeTimes)))) + 'ms' + '\n\n')
 
# Function copies files to a list of SDCs
def copyfiles(sdc_name, sdc_url, file_location, file_type):
    file_list = os.listdir(file_location)
    log_file.write("Start copying " + file_type + '\n')
    log_file.write("Copy times: ")
    sdcCopyTimes = []
    sdcStartTime = int(round(time.time() * 1000))
    for file in file_list:
        dss_copy_command = dss_client_location + " operation=make_local " + "server=" + sdc_url + " filename=" + file + " username=" + dss_username + " password=" + dss_password
        fileStartTime = int(round(time.time() * 1000))
        os.system(dss_copy_command)
        fileEndTime = int(round(time.time() * 1000))
        log_file.write(str(fileEndTime - fileStartTime) + ',')
        sdcCopyTimes.append(fileEndTime - fileStartTime)
        sdcEndTime = int(round(time.time() * 1000))
    log_file.write("\n")
    log_file.write(sdc_name + " " + file_type + " copy total time: " + str(sdcEndTime - sdcStartTime) + 'ms' + '\n')
    log_file.write(sdc_name + " " + file_type + " average time: " + str((float(sum(sdcCopyTimes))/float(len(sdcCopyTimes)))) + 'ms' + '\n\n')        	       	

#####        
# Main Script
#####

# Open the log file for storing results, and record the input parameters
date_stamp = str(datetime.datetime.now()).replace(' ','.')
date_stamp = date_stamp.replace(':', '.')
log_file_name = "dssTest-" + sdc_name + "-" + date_stamp + ".log"
log_file = open(log_file_name, 'w')	
log_file.write(sdc_name + " - " + str(datetime.datetime.now()) + '\n')
log_file.write('Seed = ' + str(seed) + '\n')

# Set the file sizes and counts for each type of file
small_file_size = 1000000 # 1MB
number_of_small_files = 1000
small_file_location = base_file_location + "/small/"
if not os.path.isdir(small_file_location):
   os.makedirs(small_file_location)
med_file_size = 1050000000 # 1GB
number_of_med_files = 100
med_file_location = base_file_location + "/medium/"
if not os.path.isdir(med_file_location):
   os.makedirs(med_file_location)
large_file_size = 10900000000 # 10GB
number_of_large_files = 5
large_file_location = base_file_location + "/large/"
if not os.path.isdir(large_file_location):
   os.makedirs(large_file_location)

# Create the fake files for all three sizes on the workspace
generateStartTime = int(round(time.time() * 1000))
generatefiles(small_file_size, number_of_small_files, small_file_location, "small")
generatefiles(med_file_size, number_of_med_files, med_file_location, "medium")
generatefiles(large_file_size, number_of_large_files, large_file_location, "large")
generateEndTime = int(round(time.time() * 1000))
log_file.write("Total file generation time: " + str(generateEndTime - generateStartTime) + 'ms' + '\n')

# Store the files in the DSS and time each operation (DSS location is determined by store_dss_url parameter)
log_file.write('=========================================\n')
log_file.write('=== Storing Files =======\n')
log_file.write('=========================================\n')
storefiles(small_file_location, "small")
storefiles(med_file_location, "medium")
storefiles(large_file_location, "large")
log_file.write('\n')

# Parse list of SDCs and copy all three sets of files to each one, recording the copy times
log_file.write('=========================================\n')
log_file.write('=== Copying Files =======\n')
log_file.write('=========================================\n')
sdc_list = [line.strip() for line in open("SdcList.txt", 'r')]
for sdc in sdc_list:
    sdc_name = sdc.split(" ")[0]
    sdc_url = sdc.split(" ")[1]
    log_file.write('===== Copy to ' + sdc_name + '=====\n')
    copyfiles(sdc_name, sdc_url, small_file_location, "small")
    copyfiles(sdc_name, sdc_url, med_file_location, "medium")
    copyfiles(sdc_name, sdc_url, large_file_location, "large")
log_file.write('\n')

# Delete the test data files
#for file in file_list:
    #file_path = file_location + file
    #os.remove(file_path)
    #dss_delete_command = dss_delete_location + " deletefile " + file
    #os.system(dss_delete_command)
    #log_file.write(dss_delete_command + "\n")
			
# Close the log file
log_file.close()


