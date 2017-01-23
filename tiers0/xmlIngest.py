#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on: 15/06/16
Author: Magaly Alonzo
Contact: magaly.alonzo@capgemini.com
'''

import ConfigParser
from shutil import copyfile
import os
import datetime
import random
import multiprocessing
from xml.dom.minidom import parse
from Xml import Xml

cfg = ConfigParser.ConfigParser()
cfg.read('easTest.cfg')
PATH = cfg.get('testValues', 'pathToDataFiles')
SDC = cfg.get('testValues', 'sdc')
files = []
for el in cfg.items('xmlFilesToIngest'):
    files.append(el[1])
asyncRuns = []
for el in cfg.items('iterations'):
    asyncRuns.append(el[1])

for ingestFile in files:
    for run in asyncRuns:
        logFile = 'ingest_' + ingestFile.split('.')[0] + '_' + SDC + '_' +\
                  str(run) + '.csv'
        '''Loop on the number of programmed iterations'''
        jobArray = []
        createdFiles = []
        for iteration in range(int(run)):
            # Setting up a new xml object
            xml = Xml(PATH, ingestFile)
            # Setting up the productId
            productId = datetime.datetime.now()
            createdFiles.append(str(productId.second) +
                                str(productId.microsecond))
            xml.productId = createdFiles[iteration]
            # copying the file with a new filename corresponding to the
            # productId
            copyfile(PATH + xml.filename, PATH + xml.filename.split('.')[0] +
                     xml.productId + '.xml')
            # Pointing our object to the newly formed file
            xml.setFilename(xml.filename.split('.')[0] + xml.productId + '.xml')
            # Changing some attributes of the new file to have realistic data
            xmlDocument = parse(PATH + xml.filename)
            xmlDocument.getElementsByTagName('ProductId')[0].childNodes[0].\
                nodeValue = xml.productId
            xmlDocument.getElementsByTagName('RA')[0].childNodes[0].\
                nodeValue = random.randrange(0, 60, 1)
            xmlDocument.getElementsByTagName('Dec')[0].childNodes[0].\
                nodeValue = random.randrange(0, 100, 1)
            file_handle = open(PATH + xml.filename, "wb")
            file_handle.write(xmlDocument.toxml())
            file_handle.close()
            # Sending this newly formed file to be ingested
            process = multiprocessing.Process(target=xml.ingest,
                                              args=(logFile,))
            jobArray.append(process)
            process.start()
            print(iteration)

        for i in jobArray:
            i.join()
        for numOfFiles in range(len(createdFiles)):
            # Deleting all the files created in the previous loop
            print('Deleting File')
            os.remove(PATH + ingestFile.split('.')[0] +
                      createdFiles[numOfFiles] + '.xml')
