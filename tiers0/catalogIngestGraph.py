#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''This module will create graphs from catalog's ingestion log file'''

import numpy as np
import matplotlib.pyplot as plt
from csvData import csvData


fieldnames = ['username', 'dbcommit', 'result', 'ingest_time', 'message',
              'project', 'privileges', 'object_id', 'filename',
              'catalog_file_load', 'total_time', 'database_time', 'nrows',
              'catalog', 'logfile', 'dbingest', 'datamodel_time', 'fitsload',
              'catalog_id', 'error', 'fitsprocess', 'commit', 'fileload_time',
              'catalog_file']

filesPath = '../logs/'
width = 0.30
bar_width = 0.30
fig, ax = plt.subplots(nrows=1, ncols=2)
inputData = ['TlseCatalogNL26-12-16_19h07.csv']

for data in range(len(inputData)):
    csvFile = csvData(inputData[data], filesPath, fieldnames)
    totalTime = csvFile.getValue('total_time')
    numOfRows = csvFile.getValue('nrows')
    catalogFileLoad = csvFile.getValue('catalog_file_load')
    databaseTime = csvFile.getValue('database_time')
    ingestTime = csvFile.getValue('ingest_time')
    dbIngest = csvFile.getValue('dbingest')
    datamodelTime = csvFile.getValue('datamodel_time')
    fitsProcess = csvFile.getValue('fitsprocess')
    fileLoadTime = csvFile.getValue('fileload_time')
    # Left plot: total time for each numOf Rows
    x = np.arange(len(numOfRows))
    labels = numOfRows
    '''['10', '100', '1000', '10m', '100m', '500m', '1M',
              '1.5M', '2M', '5M', '10M', '20M', '30M']'''
    xLabels = labels[:len(x)]
    fileLoad = ax[0].bar(x+width, fileLoadTime, bar_width, color='green',
                         bottom=[float(i) + float(db) + float(dm)
                                 for i, db, dm in
                                 zip(ingestTime, databaseTime, datamodelTime)])
    ingest = ax[0].bar(x+width, ingestTime, bar_width, color='orange',
                       bottom=[float(db) + float(dm)
                               for db, dm in
                               zip(databaseTime, datamodelTime)])
    database = ax[0].bar(x+width, databaseTime, bar_width, color='red',
                         bottom=datamodelTime)
    datamodel = ax[0].bar(x+width, datamodelTime, bar_width, color='lightblue')

    # Right plot: mean ingestion time for a unique row depending on the total
    # ingested rows
    uniqueTotalRow = [float(n) / float(d) for n, d in zip(totalTime, numOfRows)]
    uniqueCatalogFileLoad = [float(n) / float(d)
                             for n, d in zip(catalogFileLoad, numOfRows)]
    uniqueDatabaseTime = [float(n) / float(d)
                          for n, d in zip(databaseTime, numOfRows)]
    uniqueIngestTime = [float(n) / float(d)
                        for n, d in zip(ingestTime, numOfRows)]
    uniqueDbIngest = [float(n) / float(d)
                      for n, d in zip(dbIngest, numOfRows)]
    uniqueDatamodelTime = [float(n) / float(d)
                           for n, d in zip(datamodelTime, numOfRows)]
    uniqueFitsprocess = [float(n) / float(d)
                         for n, d in zip(fitsProcess, numOfRows)]
    uniqueFileload = [float(n) / float(d)
                      for n, d in zip(fileLoadTime, numOfRows)]
    uniqueFileLoad = ax[1].bar(x+width, uniqueFileload, bar_width,
                               color='green',
                               bottom=[float(i) + float(db) + float(dm)
                                       for i, db, dm in
                                       zip(uniqueIngestTime, uniqueDatabaseTime,
                                           uniqueDatamodelTime)])
    uniqueIngest = ax[1].bar(x+width, uniqueIngestTime, bar_width,
                             color='orange',
                             bottom=[float(db) + float(dm)
                                     for db, dm in zip(uniqueDatabaseTime,
                                                       uniqueDatamodelTime)])
    uniqueDatabase = ax[1].bar(x+width, uniqueDatabaseTime, bar_width,
                               color='red',
                               bottom=uniqueDatamodelTime)
    uniqueDatamodel = ax[1].bar(x+width, uniqueDatamodelTime, bar_width,
                                color='lightblue')
ax[0].legend((fileLoad[0], ingest[0], database[0], datamodel[0]),
             ('fileLoad', 'ingest', 'database', 'datamodel'),
             loc='upper left')
ax[0].set_xticks([thick+bar_width+(bar_width/2) for thick in x])
ax[0].set_xticklabels(xLabels)
ax[0].set_xlabel('Num of parallel rows sent for ingestion')
ax[0].set_ylabel('Ingestion time (sec)')
ax[0].set_title('Total ingestion for x rows')
ax[1].legend((uniqueFileLoad[0], uniqueIngest[0], uniqueDatabase[0],
              uniqueDatamodel[0]),
             ('fileLoad', 'ingest', 'database', 'datamodel'),
             loc='upper right')
ax[1].set_xticks([thick+bar_width+(bar_width/2) for thick in x])
ax[1].set_xticklabels(xLabels)
ax[1].set_xlabel('Num of parallel rows sent for ingestion')
ax[1].set_ylabel('Ingestion time (sec)')
ax[1].set_title('Single row ingestion time for x rows')
plt.suptitle('Catalog ingest results')
plt.show()
plt.tight_layout()
