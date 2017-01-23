#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on: 15/06/16
Author: Magaly Alonzo
Contact: magaly.alonzo@capgemini.com
'''

import time
import os
import csv
import random
import ConfigParser
import multiprocessing
from Xml import Xml
from xml.dom.minidom import parse

cfg = ConfigParser.ConfigParser()
cfg.read('easTest.cfg')
ITERATIONS = []
for el in cfg.items('iterations'):
    ITERATIONS.append(el[1])
queryTypes = []
for el in cfg.items('queryTypes'):
    queryTypes.append(el[1])
data = []
for el in cfg.items('xmlFilesTypes'):
    data.append(el[1])
PATH = cfg.get('testValues', 'pathToDataFiles')
outputPath = cfg.get('testValues', 'outputPath')
nisId = [1480071846.48]
'''[1480071607.06, 1480071588.93, 1480071846.48, 1480071849.5,
         1480071727.44, 1480071765.74, 1480071773.8, 1480071971.77,
         1480072197.06, 1480071960.68, 1480072119.25, 1480072125.33]'''
visId = [1480597489.07]
'''[1480597487.96, 1480597490.13, 1480597489.07, 1480597482.65,
         1480597594.31, 1480597484.79, 1480597485.85, 1480597486.91,
         1480597483.74, 1480597492.26, 1480597491.2]'''
dec = [50]
ra = [58]
fieldnames = ['totalProcessingTime', 'unitTime', 'url']
result = dict.fromkeys(fieldnames)
jobArray = []

for datum in data:
    for queryType in queryTypes:
        startTime = time.time()
        for k in range(len(ITERATIONS)):
            for iteration in range(int(ITERATIONS[k])):
                # Create xml object
                xml = Xml(PATH, datum)
                # Setting the dataType you want to retrieve
                xmlDocument = parse(xml.inputPath + xml.filename)
                xml.dataType = str(xmlDocument.documentElement.tagName).split(':')[1]
                print(xml.dataType)
                if queryType == 'simple' and \
                        xml.dataType == 'DpdVisOutput':
                    query = 'Header.ProductId.ObjectId=%s' % random.choice(visId)
                elif queryType == 'simple' and \
                        xml.dataType == 'DpdNisOutput':
                    query = 'Header.ProductId.ObjectId=%s' % random.choice(nisId)
                elif queryType == 'complex' and\
                        xml.dataType == 'DpdNisOutput':
                    query = 'Data.SirRawFrame.CommandedFPAPointing.Dec.DegAngle>' +\
                        str(dec[0]) +\
                        '&Data.SirRawFrame.CommandedFPAPointing.RA.DegAngle>' +\
                        str(ra[0])
                elif queryType == 'complex' and\
                        xml.dataType == 'DpdVisOutput':
                    query = 'Data.CommandedFPAPointing.Dec.DegAngle>' +\
                        str(dec[0]) +\
                        '&Data.CommandedFPAPointing.RA.DegAngle>' +\
                        str(ra[0])
                else:
                    print('You entered a wrong query parameter please refer to '
                          'easTest.cfg')
                print(query)
                print('Start ' + str(iteration))
                RETRIEVELOGFILE = queryType + 'Query' + xml.dataType + \
                    xml.SDC + str(ITERATIONS[k]) + '.csv'
                process = multiprocessing.Process(target=xml.retrieve,
                                                  args=(query, RETRIEVELOGFILE,))
                jobArray.append(process)
                process.start()
            for i in jobArray:
                i.join()
            result['totalProcessingTime'] = time.time() - startTime

            if os.path.isfile(outputPath + RETRIEVELOGFILE):
                with open(outputPath + RETRIEVELOGFILE, 'a') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(result)
            else:
                with open(outputPath + RETRIEVELOGFILE, 'w') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerow(result)
