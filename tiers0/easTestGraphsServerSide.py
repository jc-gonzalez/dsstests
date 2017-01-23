#!/usr/bin/env python
# -*- coding: utf-8 -*-

''' This module will generate a graph of the Catalog ingestion times from 
Andrey Belikov catalog ingestion script's logs.
@author: Alonzo Magaly
@creation:16/11/2016
@requires: test-log file from Andrey's ingestion script
'''

import csv
import numpy as np
import matplotlib.pyplot as plt
from numpy.oldnumeric.rng_stats import standardDeviation
from numpy import mean


def toDataList(dataList):
    ''' This remove the empty fields of a list '''
    output = filter(None, dataList)  # fastest
    output = map(float, output)
    return output


def keepCatalog(dataToSort):
    '''This function return only the informations relative to Catalog ingestion from Andrey's ingestion logs'''
    # Doing decremential loop because we remove items

    sortedData = []
    for i in range(len(dataToSort) - 5, 0, -5):
        sortedData.append(dataToSort[i])

    for i in range(len(dataToSort) - 5, 0, -5):
        del dataToSort[i]
        sortedData.reverse()
        dataToSort.reverse()
        return sortedData, dataToSort


def transformData(csvfile, columnName, start, stop):
    logData = csv.DictReader(csvfile, delimiter=',')
    dataArray = []
    for row in logData:
        dataArray.append(row[columnName])
    csvfile.seek(0)

    dataList = toDataList(dataArray)
    catalogData, notCatalogData = keepCatalog(dataList)
    if columnName != 'total_time':
        catalogData = np.mean(catalogData[start:stop])
    else:
    	catalogData = catalogData[start:stop]

    return catalogData, notCatalogData
   
def someOtherProcess(totalTime, ):
	other = totalTime -()
	

with open('../test-log') as csvfile:

# retrieving data for 2Mb files

    dbIngest2Mb, nDbIngest2Mb = transformData(csvfile, 'dbingest', 0, 9)
    totalTime2Mb, ntotalTime2Mb = transformData(csvfile, 'total_time', 0, 9)
    std2Mb = np.std([totalTime2Mb])
    ingestTime2Mb, ningestTime2Mb = transformData(csvfile, 'ingest_time', 0, 9)
    datamodelTime2Mb, ndatamodelTime2Mb = transformData(
        csvfile, 'datamodel_time', 0, 9)
    fitsLoad2Mb, nfitsLoad2Mb = transformData(csvfile, 'fitsload', 0, 9)
    dbcommit2Mb, ndbcommit2Mb = transformData(csvfile, 'dbcommits', 0, 9)
    catalogFileLoad2Mb, ncatalogFileLoad2Mb = transformData(
        csvfile, 'catalog_file_load', 0, 9)
    fitsProcess2Mb, nfitsProcess2Mb = transformData(
        csvfile, 'fitsprocess', 0, 9)
    fileloadTime2Mb, nfileloadTime2Mb = transformData(
        csvfile, 'fileload_time', 0, 9)
    databaseTime2Mb, ndatabaseTime2Mb = transformData(
        csvfile, 'database_time', 0, 9)
    
# retrieving data for 200Mb files

    totalTime200Mb, ntotalTime200Mb = transformData(
        csvfile, 'total_time', 10, 19)
    std200Mb = np.std([totalTime200Mb])
    dbIngest200Mb, nDbIngest200Mb = transformData(csvfile, 'dbingest', 10, 19)
    ingestTime200Mb, ningestTime200Mb = transformData(
        csvfile, 'ingest_time', 10, 19)
    datamodelTime200Mb, ndatamodelTime200Mb = transformData(
        csvfile, 'datamodel_time', 10, 19)
    fitsLoad200Mb, nfitsLoad200Mb = transformData(csvfile, 'fitsload', 10, 19)
    dbcommit200Mb, ndbcommit200Mb = transformData(csvfile, 'dbcommits', 10, 19)
    catalogFileLoad200Mb, ncatalogFileLoad200Mb = transformData(
        csvfile, 'catalog_file_load', 10, 19)
    fitsProcess200Mb, nfitsProcess200Mb = transformData(
        csvfile, 'fitsprocess', 10, 19)
    fileloadTime200Mb, nfileloadTime200Mb = transformData(
        csvfile, 'fileload_time', 10, 19)
    databaseTime200Mb, ndatabaseTime200Mb = transformData(
        csvfile, 'database_time', 10, 19)

# retrieving data for 500Mb files

    totalTime500Mb, ntotalTime500Mb = transformData(
        csvfile, 'total_time', 20, 29)
    std500Mb = np.std([totalTime500Mb])
    ingestTime500Mb, ningestTime500Mb = transformData(
        csvfile, 'ingest_time', 20, 29)
    dbIngest500Mb, ndbIngest500Mb = transformData(csvfile, 'dbingest', 20, 29)
    datamodelTime500Mb, ndatamodelTime500Mb = transformData(
        csvfile, 'datamodel_time', 20, 29)
    fitsLoad500Mb, nfitsLoad500Mb = transformData(csvfile, 'fitsload', 20, 29)
    dbcommit500Mb, ndbcommit500Mb = transformData(csvfile, 'dbcommits', 20, 29)
    catalogFileLoad500Mb, ncatalogFileLoad500Mb = transformData(
        csvfile, 'catalog_file_load', 20, 29)
    fitsProcess500Mb, nfitsProcess500Mb = transformData(
        csvfile, 'fitsprocess', 20, 29)
    fileloadTime500Mb, nfileloadTime500Mb = transformData(
        csvfile, 'fileload_time', 20, 29)
    databaseTime500Mb, ndatabaseTime500Mb = transformData(
        csvfile, 'database_time', 20, 29)

    '''
# retrieving data for 1.5Gb files
    
    totalTime15Gb = transformData(csvfile,'total_time', 30, 39)
    std15G = np.std([totalTime15Gb])
    dbIngestd15Gb = transformData(csvfile,'dbingest', 30, 39)
    ingestTime15Gb = transformData(csvfile,'ingest_time', 30, 39)
    datamodelTime15Gb = transformData(csvfile,'datamodel_time', 30, 39)
    fitsLoad15Gb = transformData(csvfile,'fitsload', 30, 39)
    dbcommit15Gb = transformData(dacsvfileta,'dbcommits', 30, 39)
    catalogFileLoad15Gb = transformData(csvfile,'catalog_file_load', 30, 39)
    fitsProcess15Gb = transformData(csvfile,'fitsprocess', 30, 39)
    fileloadTime15Gb = transformData(csvfile,'fileload_time', 30, 39)
    databaseTime15Gb = transformData(csvfile,'database_time', 30, 39)'''
    
    fig, ax = plt.subplots()
	
	# Here catalog graph construction
    standardDeviation = ([std2Mb, std200Mb, std500Mb])
    yDatamodelTime = np.array(
        [datamodelTime2Mb, datamodelTime200Mb, datamodelTime500Mb])
    ydbIngest = np.array([dbIngest2Mb, dbIngest200Mb, dbIngest500Mb])
    yingestTime = np.array([ingestTime2Mb, ingestTime200Mb, ingestTime500Mb])
    yfitsload = np.array([fitsLoad2Mb, fitsLoad200Mb, fitsLoad500Mb])
    yfitsProcess = np.array(
       [fitsProcess2Mb, fitsProcess200Mb, fitsProcess500Mb])
    yDbcommit = np.array([dbcommit2Mb, dbcommit200Mb, dbcommit500Mb])
    ycatalogFileLoad = np.array(
        [catalogFileLoad2Mb, catalogFileLoad200Mb, catalogFileLoad500Mb])
    x = np.arange(3)
    
    ax.bar(x, yDbcommit, color='blue', align='center',bottom=ycatalogFileLoad+yfitsload + yfitsProcess+ydbIngest, yerr=standardDeviation)
    ax.bar(x, ycatalogFileLoad, color='yellow',  align='center', bottom=yfitsload + yfitsProcess+ydbIngest)
    ax.bar(x, yfitsload, color='red', align='center',bottom=yfitsProcess+ydbIngest)
    ax.bar(x, yfitsProcess, color='orange', align='center', bottom = ydbIngest)
    ax.bar(x, ydbIngest,color ='green', align='center')
    ax.set_xticks(x)
    ax.set_yticks([np.mean(totalTime2Mb), np.mean(totalTime200Mb), np.mean(totalTime500Mb)])
    ax.set_yticklabels([str(np.mean(totalTime2Mb))[:4]+' secs', str(np.mean(totalTime200Mb)/60)[:4]+' min',\
					 str(np.mean(totalTime500Mb)/60)[:4]+' min'])
    ax.set_xticklabels(['2Mb', '200Mb', '500Mb'])
    ax.legend(['Db commit', 'Catalog File load', 'fits load',
               'fits process',  'Db ingest'],
              loc='upper left')
      
    plt.show()

