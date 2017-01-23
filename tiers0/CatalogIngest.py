#!/usr/bin/env python
# -*-coding : utf-8 -*-

'''
Created on: 15/06/16
Author: Magaly Alonzo
Contact: magaly.alonzo@capgemini.com
'''

from xml.dom.minidom import parse
from Catalog import Catalog
from shutil import copyfile
from datetime import datetime
import ConfigParser
import os
import multiprocessing
import logging

cfg = ConfigParser.ConfigParser()
cfg.read('easTest.cfg')
PATH = cfg.get('testValues', 'pathToDataFiles')
FILENAME = cfg.get('testValues', 'catalogFilename')
ITERATIONS = []
for el in cfg.items('iterations'):
    ITERATIONS.append(el[1])
fileList = []
for el in cfg.items('catalogToIngest'):
    fileList.append(el[1])
logger = logging.getLogger()
createdFiles = []
for fitsFileToIngest in fileList:
    print(fitsFileToIngest)
    for iteration in range(int(ITERATIONS[0])):
        catalog = Catalog(PATH, FILENAME)
        catalog.productId = str(datetime.now().second) +\
            str(datetime.now().microsecond)
        createdFiles.append(catalog.productId)
        # Copying file with the new filename corresponding to the productId
        copyfile(PATH + catalog.filename, PATH +
                 catalog.filename.split('.')[0] + catalog.productId + '.xml')
        # Changing some attributes of the new file to have realistic data
        catalogDocument = parse(PATH + catalog.filename)
        catalogDocument.getElementsByTagName('ProductId')[0].childNodes[0].\
            nodeValue = catalog.productId
        catalogDocument.getElementsByTagName('FileName')[0].childNodes[0].\
            nodeValue = fitsFileToIngest
        file_handle = open(PATH + catalog.filename, "wb")
        file_handle.write(catalogDocument.toxml())
        file_handle.close()
        # Sending this newly formed file to be ingested
        catalog.ingest()

# Start deleting the used files from the local directory
# process.join()
for numOfFiles in range(len(createdFiles)):
    # Deleting all the files created in the previous loop
    os.remove(PATH + FILENAME.split('.')[0] + createdFiles[numOfFiles] + '.xml')
