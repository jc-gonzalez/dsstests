#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib as matplot
import csv
import ConfigParser
import os
import matplotlib.pyplot as plt
import re


def removeTimeStamp(floatList, startTime):
	duration = []
	
	if floatList[0]:
		for firstLevel in range(len(floatList)):
			count = 0
			#print('Float List ' +str(floatList[firstLevel]))
			if type(floatList[firstLevel]) is not float:
				for secondLevel in range(len(floatList[firstLevel])):
					duration.append(floatList[firstLevel][count] - startTime)
					count+=1
			else:
				duration.append(floatList[firstLevel] - startTime)
		#print(duration)
		return duration
	else:
		return

def toList(array):
	catalog = []
	for run in range(len(array)):
		for el in array[run]:
			catalog.append(el) 
	return(catalog)

def toDataList(data):

	output = filter(None, data) # fastest
	output = map(float, output)
	return output

startingTime = []
endTime = []
pollingStart = []
pollingStop = []
startDownloadingXml = []
finishDownloadingXml = []
xmlFileSize = []
ppoUpdateStart = []
ppoUpdateFinishes = []
ppoFileSize = []
ingestCatalogStart= []
ingestCatalogFinished = []
xmlCatalogFileSize = []

with open ('data.csv') as csvfile:
	data = csv.DictReader(csvfile, delimiter=',')
	
#Setting the headers

	fieldNames = ['startingTime','pollingStart', 'pollingStop', 'startDownloadingXml', 'finishDownloadingXml',\
				'xmlFileSize', 'ppoUpdateStart', 'ppoUpdateFinish', 'ppoFileSize', \
				'ingestCatalogStart', 'ingestCatalogFinished', 'xmlCatalogFileSize', 'endTime']
	for row in data:
		startingTime.append(row["startingTime"])
		endTime.append(row["endTime"])
		pollingStart.append(row["pollingStart"])
		pollingStop.append(row["pollingStop"])
		startDownloadingXml.append(row["startDownloadingXml"])
		finishDownloadingXml.append(row["finishDownloadingXml"])
		xmlFileSize.append(row["xmlFileSize"])
		ppoUpdateStart.append(row["ppoUpdateStart"])
		ppoUpdateFinishes.append(row["ppoUpdateFinish"])
		ppoFileSize.append(row["ppoFileSize"])
		ingestCatalogStart.append(row["ingestCatalogStart"])
		ingestCatalogFinished.append(row["ingestCatalogFinished"])
		xmlCatalogFileSize.append(row["xmlCatalogFileSize"])
	startingTime = toDataList(startingTime)
	endTime = toDataList(endTime)
	pollingStart = toDataList(pollingStart)
	pollingStop = toDataList(pollingStop)	
	startDownloadingXml = toDataList(startDownloadingXml)
	finishDownloadingXml = toDataList(finishDownloadingXml)
	xmlFileSize = toDataList(xmlFileSize)
	ppoUpdateStart = toDataList(ppoUpdateStart)
	ppoUpdateFinishes = toDataList(ppoUpdateFinishes)
	ppoFileSize = toDataList(ppoFileSize)
	ingestCatalogStart = toDataList(ingestCatalogStart)
	ingestCatalogFinished = toDataList(ingestCatalogFinished)
	xmlCatalogFileSize = toDataList(xmlCatalogFileSize)
		
			
# Removing the fisrt timestamp to have the time at which the process starts

endTime = removeTimeStamp(endTime, startingTime[0])
startDownloadingXmlDuration = removeTimeStamp(startDownloadingXml, startingTime[0])
finishDownloadingXmlDuration = removeTimeStamp(finishDownloadingXml, startingTime[0])
ppoUpdateStartDuration = removeTimeStamp(ppoUpdateStart, startingTime[0])
ppoUpdateFinishesDuration = removeTimeStamp(ppoUpdateFinishes, startingTime[0])
ingestCatalogStartDuration = removeTimeStamp(ingestCatalogStart, startingTime[0])
ingestCatalogFinishedDuration = removeTimeStamp(ingestCatalogFinished, startingTime[0])


step = 1
xmlLoad = []
ppoLoad = []
catalogLoad = []

count = 0
for bine in range(0, 100, step):
	xmlLoad.append(0)
	ppoLoad.append(0)
	catalogLoad.append(0)
	for xmlDuration in range(len(startDownloadingXmlDuration)):

		if 	startDownloadingXmlDuration[xmlDuration] >= bine and finishDownloadingXmlDuration[xmlDuration] <= bine + step \
		or startDownloadingXmlDuration[xmlDuration] <= bine and finishDownloadingXmlDuration[xmlDuration] <= bine + step \
		and finishDownloadingXmlDuration[xmlDuration] >= bine \
		or startDownloadingXmlDuration[xmlDuration] >= bine and finishDownloadingXmlDuration[xmlDuration] >= bine + step \
		and startDownloadingXmlDuration[xmlDuration] <= bine + step \
		or startDownloadingXmlDuration[xmlDuration] <= bine and finishDownloadingXmlDuration[xmlDuration] >= bine + step:
			xmlLoad[count] += xmlFileSize[xmlDuration]
			
					
	for ppoDuration in range(len(ppoUpdateStartDuration)):
		if 	ppoUpdateStartDuration[ppoDuration] >= bine and ppoUpdateFinishesDuration[ppoDuration] <= bine + step \
		or ppoUpdateStartDuration[ppoDuration] <= bine and ppoUpdateFinishesDuration[ppoDuration] <= bine + step \
		and ppoUpdateFinishesDuration[ppoDuration] >= bine \
		or ppoUpdateStartDuration[ppoDuration] >= bine and ppoUpdateFinishesDuration[ppoDuration] >= bine + step \
		and ppoUpdateStartDuration[ppoDuration] <= bine + step \
		or ppoUpdateStartDuration[ppoDuration] <= bine and ppoUpdateFinishesDuration[ppoDuration] >= bine + step:
			ppoLoad[count] += ppoFileSize[ppoDuration]
		
	
	for catalogDuration in range(len(ingestCatalogStartDuration)):
	
		if 	ingestCatalogStartDuration[catalogDuration] >= bine and ingestCatalogFinishedDuration[catalogDuration] <= bine + step \
		or ingestCatalogStartDuration[catalogDuration] <= bine and ingestCatalogFinishedDuration[catalogDuration] <= bine + step \
		and ingestCatalogFinishedDuration[catalogDuration] >= bine \
		or ingestCatalogStartDuration[catalogDuration] >= bine and ingestCatalogFinishedDuration[catalogDuration] >= bine + step \
		and ingestCatalogStartDuration[catalogDuration] <= bine + step \
		or ingestCatalogStartDuration[catalogDuration] <= bine and ingestCatalogFinishedDuration[catalogDuration] >= bine + step:
			catalogLoad[count] += xmlCatalogFileSize[catalogDuration]
	count += 1
x = range(0, 100, step)
xmlBar = plt.bar(x,xmlLoad, color="green", bottom = ppoLoad)
ppoBar = plt.bar(x, ppoLoad, color="red")
catalogBar = plt.bar(x, catalogLoad, color="blue", bottom = ppoLoad)
plt.legend((xmlBar[0], ppoBar[0], catalogBar[0]), ('xml', 'ppo', 'catalog'))
plt.ylabel('Volume of Data')
plt.yticks(range(0, 10000, 500))
plt.xlabel('Time')
plt.show()

