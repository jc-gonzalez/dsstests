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
fig, ax = plt.subplots()
inputData = [['simpleQueryDpdVisOutputNL10.csv',
              'simpleQueryDpdVisOutputNL100.csv',
              'simpleQueryDpdVisOutputNL1000.csv'],
             ['complexQueryDpdVisOutputNL10.csv',
              'complexQueryDpdVisOutputNL100.csv',
              'complexQueryDpdVisOutputNL1000.csv']]


def autolabel(rects, height, numOfIngestion):
    # attach some text labels
    for i in range(len(rects)):
        ax.text(rects[i].get_x() + rects[i].get_width()/2.,
                         1.05*height[i],
                         '%d' % int(numOfIngestion[i]),
                         ha='center', va='bottom')

meanSimpleUnitTime = []
meanSimpleTotalProcessingTime = []
meanComplexUnitTime = []
meanComplexTotalProcessingTime = []
stdev = []
complexStdev = []
realIngestionNum = []
realComplexIngestionNum = []

for filename in range(len(inputData[0])):
    data = csvData(inputData[0][filename], filesPath, fieldsNames)
    meanSimpleUnitTime.append(mean(filter(lambda a: a>1, data.getValue('unitTime'))))
    meanSimpleTotalProcessingTime.append(mean(data.getValue('totalProcessingTime')))
    stdev.append(std(data.getValue('unitTime')))
    realIngestionNum.append(len(filter(lambda a: a>1, data.getValue('unitTime'))))

for filename in range(len(inputData[1])):
    complexData = csvData(inputData[1][filename], filesPath, fieldsNames)
    meanComplexUnitTime.append(mean(filter(lambda a: a>1, complexData.getValue('unitTime'))))
    meanComplexTotalProcessingTime.append(mean(complexData.getValue('totalProcessingTime')))
    complexStdev.append(std(complexData.getValue('unitTime')))
    realComplexIngestionNum.append(len(filter(lambda a: a>1, complexData.getValue('unitTime'))))
    
# Here xml File graph construction
# First setting the graph of the mean ingestion values
meanYunitTime = np.array(meanSimpleUnitTime)
meanYtotalProcessingTime = np.array(meanSimpleTotalProcessingTime)
meanYComplexUnitTime = np.array(meanComplexUnitTime)
meanYComplexTotalProcessingTime = np.array(meanComplexTotalProcessingTime)
print(meanYunitTime)
meanTotalProcessingTime = ax.bar(x+width, meanYtotalProcessingTime,
                                 bar_width,
                                 color='orange')
meanUnitTime = ax.bar(x+width, meanYunitTime, bar_width,
                      color='green', yerr=stdev)
meanComplexTotalProcessingTime = ax.bar(x+(2*width), meanYComplexTotalProcessingTime,
                                 bar_width,
                                 color='orange')
meanComplexUnitTime = ax.bar(x+(2*width), meanYComplexUnitTime, bar_width,
                      color='lightgreen', yerr=complexStdev, error_kw=dict(ecolor='blue'))

autolabel(meanUnitTime, meanYtotalProcessingTime, realIngestionNum)
autolabel(meanComplexUnitTime, meanYComplexTotalProcessingTime, realComplexIngestionNum)
width += 0.30

# Setting up the graphs scales and legend
# Graph 1
ax.legend((meanTotalProcessingTime[0], meanUnitTime[0], meanComplexUnitTime[0]),
           ('total time', 'single query mean time', 'complex query mean time'), 
          loc='upper left')
ax.set_xticks(x+(bar_width)+(bar_width/2))
ax.set_xticklabels(numOfIngestion)
ax.set_xlabel('Num of parallel xml queries')
ax.set_ylabel('Ingestion times (sec)')

plt.suptitle('DpdVisOutput query in SDC-NL from Tlse'
             ' num of ingestion effect')

plt.show()
