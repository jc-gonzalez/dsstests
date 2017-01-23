#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''This module will create detailed Ingestion time graphs from Catalog query '''

import numpy as np
import matplotlib.pyplot as plt
from csvData import csvData

filePath = '../logs/'
width = 0.30
bar_width = 0.30
fig, ax = plt.subplots(nrows=1, ncols=2)
fieldnames = ['username', 'create_fitsprocess', 'dbingest', 'datamodel_time',
              'dbcommit', 'nrows', 'sqlprepare', 'catalog_file_load',
              'fileload_time', 'privileges', 'error', 'catalog_id', 'fitsload'
              'object_id', 'filename', 'project', 'database_time', 'fitssave',
              'result', 'fitsprocess', 'ingest_time', 'total_time', 'commit',
              'logfile', 'dbselect', 'catalog_file', 'number_of_rows']
inputData = ['TlseCatalogNL26-12-16_19h07.csv']

for data in range(len(inputData)):
    csvFile = csvData(inputData[data], filePath, fieldnames)
    fitsProcessTime = csvFile.getValue('fitsprocess')
    numOfRowsTime = csvFile.getValue('nrows')
    fitsSaveTime = csvFile.getValue('fitssave')
    dbSelectTime = csvFile.getValue('dbselect')
    fitsloadTime = csvFile.getValue('fitsload')
    catalogFitsProcessTime = csvFile.getValue('fitsprocess')
    dbIngestTime = csvFile.getValue('dbingest')
    dbCommitTime = csvFile.getValue('dbcommit')
    catalogFileLoadTime = csvFile.getValue('catalog_file_load')
    # Left plot: total time for each numOfRows
    x = np.arange(len(numOfRowsTime))
    labels = numOfRowsTime
    xLabels = labels[:len(x)]

    fitsLoad = ax[0].bar(x+width, fitsloadTime, bar_width, color='orange',
                         bottom=[float(i) + float(s) + float(p) + float(f) +
                                 float(a) + float(l)
                                 for i, s, p, f, a, l
                                 in zip(dbIngestTime, dbSelectTime,
                                        catalogFitsProcessTime, fitsProcessTime,
                                        fitsSaveTime, catalogFileLoadTime)])
    catalogFileLoad = ax[0].bar(x+width, catalogFileLoadTime, bar_width,
                                color='dodgerblue',
                                bottom=[float(i) + float(s) + float(p) +
                                        float(f) + float(a)
                                        for i, s, p, f, a
                                        in zip(dbIngestTime, dbSelectTime,
                                               catalogFitsProcessTime,
                                               fitsProcessTime, fitsSaveTime)])
    fitsSave = ax[0].bar(x+width, fitsSaveTime, bar_width, color='red',
                         bottom=[float(i) + float(s) + float(p) + float(f)
                                 for i, s, p, f
                                 in zip(dbIngestTime, dbSelectTime,
                                        catalogFitsProcessTime,
                                        fitsProcessTime)])
    fitsProcess = ax[0].bar(x+width, fitsProcessTime, bar_width,
                            color='green',
                            bottom=[float(i) + float(s) + float(p)
                                    for i, s, p
                                    in zip(dbIngestTime, dbSelectTime,
                                           catalogFitsProcessTime)])

    catalogFitsProcess = ax[0].bar(x+width, catalogFitsProcessTime, bar_width,
                                   color='yellow',
                                   bottom=[float(i) + float(s) for i, s
                                           in zip(dbIngestTime, dbSelectTime)])

    dbSelect = ax[0].bar(x+width, dbSelectTime, bar_width, color='lightblue',
                         bottom=dbIngestTime)

    dbIngest = ax[0].bar(x+width, dbIngestTime, bar_width)

    # Right plot: mean ingestion time for a unique row depending on the total
    # ingested rows
    uniqueFitsLoad = [float(n) / float(d) for n, d in zip(fitsloadTime,
                                                          numOfRowsTime)]
    uniqueCatalogFileLoad = [float(n) / float(d) for n, d in
                             zip(catalogFileLoadTime, numOfRowsTime)]
    uniqueFitsSave = [float(n) / float(d) for n, d in zip(fitsSaveTime,
                                                          numOfRowsTime)]
    uniqueFitsProcess = [float(n) / float(d) for n, d in zip(fitsProcessTime,
                                                             numOfRowsTime)]
    uniqueCatalogFitsProcess = [float(n) / float(d) for n, d in
                                zip(catalogFitsProcessTime, numOfRowsTime)]
    uniqueDbSelect = [float(n) / float(d) for n, d in
                      zip(dbSelectTime, numOfRowsTime)]
    uniqueDbIngest = [float(n) / float(d) for n, d in
                      zip(dbIngestTime, numOfRowsTime)]
    uFitsload = ax[1].bar(x+width, uniqueFitsLoad, bar_width,
                          color='orange',
                          bottom=[c + s + p + f + ds + di
                                  for c, s, p, f, ds, di in
                                  zip(uniqueCatalogFileLoad,
                                      uniqueFitsSave, uniqueFitsProcess,
                                      uniqueCatalogFitsProcess, uniqueDbSelect,
                                      uniqueDbIngest)])
    uCatalogFileLoad = ax[1].bar(x+width, uniqueCatalogFileLoad, bar_width,
                                 color='dodgerblue',
                                 bottom=[fs + fp + cfp + ds + di
                                         for fs, fp, cfp, ds, di in
                                         zip(uniqueFitsSave, uniqueFitsProcess,
                                             uniqueCatalogFitsProcess,
                                             uniqueDbSelect, uniqueDbIngest)])

    uFitsSave = ax[1].bar(x+width, uniqueFitsSave, bar_width,
                          color='red',
                          bottom=[fp + cfp + ds + di
                                  for fp, cfp, ds, di in
                                  zip(uniqueFitsProcess,
                                      uniqueCatalogFitsProcess,
                                      uniqueDbSelect, uniqueDbIngest)])
    uFitsProcess = ax[1].bar(x+width, uniqueFitsProcess, bar_width,
                             color='green',
                             bottom=[cfp + ds + di
                                     for cfp, ds, di in
                                     zip(uniqueCatalogFitsProcess,
                                         uniqueDbSelect, uniqueDbIngest)])
    uCatalogFitsProcess = ax[1].bar(x+width, uniqueCatalogFitsProcess,
                                    bar_width,
                                    color='yellow',
                                    bottom=[ds + di
                                            for ds, di in
                                            zip(uniqueDbSelect,
                                                uniqueDbIngest)])
    uDbSelect = ax[1].bar(x+width, uniqueDbSelect, bar_width,
                          color='lightblue',
                          bottom=uniqueDbIngest)
    uDbIngest = ax[1].bar(x+width, uniqueDbIngest, bar_width)

ax[0].legend((fitsLoad[0], catalogFileLoad[0], fitsSave[0], fitsProcess[0],
              catalogFitsProcess[0], dbSelect[0], dbIngest[0]),
             ('fitsLoad', 'catalogFileLoad', 'fitsSave', 'fitsProcess',
              'catalogFitsProcess', 'dbSelect', 'dbIngest'),
             loc='upper left')
ax[0].set_xticks([thick+bar_width+(bar_width/2) for thick in x])
ax[0].set_xticklabels(xLabels)
ax[0].set_xlabel('Num Of rows returned by query')
ax[0].set_ylabel('Mean ingestion time (sec)')
ax[1].legend((uFitsload[0], uCatalogFileLoad[0], uFitsSave[0], uFitsProcess[0],
              uCatalogFitsProcess[0], uDbSelect[0], uDbIngest[0]),
             ('fitsLoad', 'catalogFileLoad', 'fitsSave', 'fitsProcess',
              'catalogFitsProcess', 'dbSelect', 'dbIngest'),
             loc='upper right')
ax[1].set_xticks([thick+bar_width+(bar_width/2) for thick in x])
ax[1].set_xticklabels(xLabels)
ax[1].set_xlabel('Num Of rows returned by query')
ax[1].set_ylabel('Ingestion time for a single row')

plt.suptitle('Detailed ingest durations for Catalog query')
plt.show()
