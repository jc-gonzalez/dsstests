#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''This module will create two different graphs:
    The first one shows the ingestionTime of each file for different
    concommitent fileNumber ingestions.
    The second one show the ingestionProcessTime of the overall files
    concommitently ingested.'''

import numpy as np
import matplotlib.pyplot as plt
from numpy import std
from numpy import mean
from csvData import csvData

fieldsNames = ['total_time', 'username', 'datamodel_time', 'fileload_time',
               'database_time', 'privileges', 'error', 'object_id', 'filename',
               'project', 'result', 'ingest_time', 'commit', 'message',
               'logfile', 'cpu']
filesPath = '../logs/'
numOfRuns = 1
width = 0.30
bar_width = 0.30
numOfIngestion = [10, 100, 1000]
x = np.arange(len(numOfIngestion))
fig, ax = plt.subplots(nrows=1, ncols=2)
inputData = [['ingest_DpdNisOutput1_SDC-NL_10.csv',
              'ingest_DpdNisOutput1_SDC-NL_100.csv',
              'ingest_DpdNisOutput1_SDC-NL_1000.csv'],
             ['ingest_DpdNisOutput10_SDC-NL_10.csv',
              'ingest_DpdNisOutput10_SDC-NL_100.csv',
              'ingest_DpdNisOutput10_SDC-NL_1000.csv'],
             ['ingest_DpdNisOutput20_SDC-NL_10.csv',
              'ingest_DpdNisOutput20_SDC-NL_100.csv',
              'ingest_DpdNisOutput20_SDC-NL_1000.csv']]


def autolabel(rects, height, numOfIngestion, plotNum):
    # attach some text labels
    for i in range(len(rects)):
        ax[plotNum].text(rects[i].get_x() + rects[i].get_width()/2.,
                         1.05*height[i],
                         '%d' % int(numOfIngestion[i]),
                         ha='center', va='bottom')

for setOfData in range(len(inputData)):
    meanDatamodelTime = []
    meanFileLoadTime = []
    meanDatabaseTime = []
    meanIngestTime = []
    meanTotalTime = []
    datamodelTime = []
    fileLoadTime = []
    databaseTime = []
    ingestTime = []
    stdev = []
    realIngestionNum = []
    for filename in range(len(inputData[setOfData])):
        data = csvData(inputData[setOfData][filename], filesPath, fieldsNames)
        meanDatamodelTime.append(mean(data.getValue('datamodel_time')))
        meanFileLoadTime.append(mean(data.getValue('fileload_time')))
        meanDatabaseTime.append(mean(data.getValue('database_time')))
        meanIngestTime.append(mean(data.getValue('ingest_time')))
        datamodelTime.append(sum(data.getValue('datamodel_time')))
        fileLoadTime.append(sum(data.getValue('fileload_time')))
        databaseTime.append(sum(data.getValue('database_time')))
        ingestTime.append(sum(data.getValue('ingest_time')))
        stdev.append(std(data.getValue('total_time')))
        realIngestionNum.append(len(data.getValue('total_time')))
    # Here xml File graph construction
    # First setting the graph of the mean ingestion values
    meanYdatamodelTime = np.array(meanDatamodelTime)
    meanYdataBaseTime = np.array(meanDatabaseTime)
    meanYingestTime = np.array(meanIngestTime)
    meanYfileLoad = np.array(meanFileLoadTime)
    print(realIngestionNum)
    meanHeight = meanYingestTime+meanYdataBaseTime + meanYdatamodelTime
    meanFileLoad = ax[0].bar(x+width, meanYfileLoad, bar_width,
                             color='green',
                             bottom=meanYingestTime+meanYdataBaseTime +
                             meanYdatamodelTime, yerr=stdev)
    meanIngest = ax[0].bar(x+width, meanYingestTime, bar_width, color='orange',
                           bottom=meanYdataBaseTime+meanYdatamodelTime)
    meanDatabase = ax[0].bar(x+width, meanYdataBaseTime, bar_width, color='red',
                             bottom=meanYdatamodelTime)
    meanDatamodel = ax[0].bar(x+width, meanYdatamodelTime, bar_width,
                              color='lightblue')
    autolabel(meanIngest, meanHeight, realIngestionNum, 0)
    # Second setting the graph of the total ingestion values
    yDatamodelTime = np.array([k / numOfRuns for k in datamodelTime])
    yDataBaseTime = np.array([k / numOfRuns for k in databaseTime])
    yIngestTime = np.array([k / numOfRuns for k in ingestTime])
    yFileLoad = np.array([k / numOfRuns for k in fileLoadTime])
    print(type(yIngestTime))
    fileLoad = ax[1].bar(x+width, yFileLoad, bar_width,
                         color='green',
                         bottom=yIngestTime+yDataBaseTime+yDatamodelTime,
                         yerr=stdev)
    ingest = ax[1].bar(x+width, yIngestTime, bar_width, color='orange',
                       bottom=yDataBaseTime+yDatamodelTime)
    database = ax[1].bar(x+width, yDataBaseTime, bar_width, color='red',
                         bottom=yDatamodelTime)
    datamodel = ax[1].bar(x+width, yDatamodelTime, bar_width,
                          color='lightblue')
    height = yIngestTime+yDataBaseTime+yDatamodelTime
    autolabel(datamodel, height, realIngestionNum, 1)
    width += 0.30

# Setting up the graphs scales and legend
# Graph 1
ax[0].legend((meanFileLoad[0], meanIngest[0], meanDatabase[0],
              meanDatamodel[0]),
             ('fileLoad', 'ingest', 'database', 'datamodel'), loc='upper left')
ax[0].set_xticks(x+(2*bar_width)+(bar_width/2))
ax[0].set_xticklabels(numOfIngestion)
ax[0].set_xlabel('Num of parallel xml ingested')
ax[0].set_ylabel('Mean ingestion time (sec)')
ax[0].set_title('Mean ingestion for 1 file')
ax[0].set_yticks(range(0, 31, 2))

# Graph 2
ax[1].legend((fileLoad[0], ingest[0], database[0], datamodel[0]),
             ('fileLoad', 'ingest', 'database', 'datamodel'), loc='upper left')

ax[1].set_xticks(x+(2*bar_width)+(bar_width/2))
ax[1].set_xticklabels(numOfIngestion)
ax[1].set_xlabel('Num of parallel xml ingested')
ax[1].set_ylabel('Ingestion time (sec)')
ax[1].set_title('Total ingestion time for x files')

plt.suptitle('DpdNisOutput ingestion in SDC-NL from Tlse'
             ' num of ingestion effect')

plt.show()
