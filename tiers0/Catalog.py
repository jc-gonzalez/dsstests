#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on: 15/06/16
Author: Magaly Alonzo
Contact: magaly.alonzo@capgemini.com
'''

import xmlrpclib
import ConfigParser
import logging
import ast
import csv
import time
import os
import socket


class Catalog(object):

    '''This class define a catalog accorging to Euclid DPS ingestion /
    retrieval needs:
        - filename : fullname with extension in string format
        - tableName : the name of the DPS table in which the file has to be
                      ingested
    There are different class attributes that comes from the tests config file:
        - username: EAS username
        - password: EAS password
        - environment: EAS environment on which you are doing the tests
        - project: EAS project on which you are doing the tests
        - logger: a logging service to retrieve some logs to parent script'''

    cfg = ConfigParser.ConfigParser()
    cfg.read('easTest.cfg')
    USERNAME = cfg.get('testValues', 'username')
    PASSWORD = cfg.get('testValues', 'password')
    ENVIRONMENT = cfg.get('testValues', 'environment')
    PROJECT = cfg.get('testValues', 'project')
    LOGFILE = cfg.get('testValues', 'catalogLogfile')
    SDC = cfg.get('testValues', 'sdc')
    # Setting the output format for the logger
    formatter = logging.Formatter('%(asctime)s :: %(name)s :: %(threadName)'
                                  '-10s :: %(message)s',
                                  datefmt='%m-%d-%Y %I:%M:%S')
    LOGGER = logging.getLogger('ingestLogger')
    LOGGER.setLevel(logging.INFO)
    # Creation of a second handler that will manage prompted messages
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    LOGGER.addHandler(stream_handler)

    def __init__(self, path, filename):
        self.filename = filename
        self.inputPath = path
        self.outputPath = Catalog.cfg.get('testValues', 'outputPath')
        self.tableName = ''
        self.ingestResult = dict()
        self.queryResult = ''
        self.productId = ''

    def setFilename(self, filename):
        '''Setter for filename, it should include the extension'''
        self.filename = filename

    def getFilename(self):
        '''Getter for filename'''
        return self.filename

    def setInputPath(self, path):
        '''Setter of inputPath'''
        self.inputPath = path

    def getInputPath(self):
        '''Getter for the path to the Catalog file'''
        return self.inputPath

    def setTableName(self, tableName):
        '''Setter for table linked to the Catalog'''
        self.tableName = tableName

    def getTableName(self):
        '''Getter for tableName'''
        return self.tableName

    def ingest(self):
        '''Method to ingest Catalog into the EAS-DPS server you have to first
        ingest the fits catalog in the DSS, then ingest the metadata calling the
        ingestObjectWithCatalog function so it will retrieve the fits and ingest
        it in the proper EAS-DPS table'''
        # Opening the Catalog metadata
        inputFile = open(self.inputPath + self.filename).read()
        # Connecting to EAS-DPS server
        if Catalog.SDC == 'SOC':
            sdcUrl = '@easdps02.esac.esa.int:8001'
            # '@easdps-mis-' + Catalog.ENVIRONMENT + '.esac.esa.int'
        elif Catalog.SDC == 'SDC-NL':
            sdcUrl = '@eas-dps-mis.' + Catalog.ENVIRONMENT + \
                '.euclid.astro.rug.nl'
        else:
            raise ValueError('You entered a SDC that do not exist or is not '
                             'handled yet by this program please contact '
                             'magaly.alonzo@capgemini.com')
        client = xmlrpclib.ServerProxy('https://' + Catalog.USERNAME + ':' +
                                       Catalog.PASSWORD + sdcUrl)
        socket.setdefaulttimeout(200.0)
        # Sending Catalog ingestion Request
        res = client.IngestObjectWithCatalogAsy(inputFile, Catalog.PROJECT,
                                                True)
        output = self.getIngestOutput(res, client)
        self.ingestResult = dict.fromkeys(output)
        self.outputHandler(output)

        if os.path.isfile(self.outputPath + Catalog.LOGFILE):
            with open(self.outputPath + Catalog.LOGFILE, 'a') as csvfile:
                writer = csv.DictWriter(csvfile,
                                        fieldnames=self.ingestResult.keys())
                writer.writerow(self.ingestResult)
        else:
            with open(self.outputPath + Catalog.LOGFILE, 'w') as csvfile:
                writer = csv.DictWriter(csvfile,
                                        fieldnames=self.ingestResult.keys())
                writer.writeheader()
                writer.writerow(self.ingestResult)
        Catalog.LOGGER.info(output)
        return output

    def query(self, query, logFile):
        '''Querying and downloading the queried Catalog'''
        # Connecting to the server
        client = xmlrpclib.ServerProxy('https://' + Catalog.USERNAME + ':' +
                                       Catalog.PASSWORD +
                                       '@eas-dps-mis.pip.euclid.astro.rug.nl')
        # Sending the query and creating the correspinding Catalog
        self.queryResult = client.CreateCatalogAsy(self.tableName, query,
                                                   Catalog.PROJECT, True)
        output = self.getQueryOutput(self.queryResult, client)
        self.ingestResult = dict.fromkeys(output)
        self.outputHandler(output)
        if os.path.isfile(self.outputPath + logFile):
            with open(self.outputPath + logFile, 'a') as csvfile:
                writer = csv.DictWriter(csvfile,
                                        fieldnames=self.ingestResult.keys())
                writer.writerow(self.ingestResult)
        else:
            with open(self.outputPath + logFile, 'w') as csvfile:
                writer = csv.DictWriter(csvfile,
                                        fieldnames=self.ingestResult.keys())
                writer.writeheader()
                writer.writerow(self.ingestResult)

    def outputHandler(self, args):
        '''This Method goes through output Dict structure and flatten it in
        order to put it in a csv'''
        for key in args:
            if isinstance(args[key], list) and len(args[key]) > 0:
                for innerKey in args[key][0]:
                    innerAttribute = args[key][0][innerKey]
                    if isinstance(innerAttribute, dict) or\
                            isinstance(innerAttribute, list):
                        self.outputHandler(innerAttribute)
                    else:
                        self.ingestResult[innerKey] = innerAttribute
            else:
                self.ingestResult[key] = args[key]
        return self.ingestResult

    def getIngestOutput(self, res, client):
        '''This function will query the server to know if the job is done
        and when it is, it will retrieve the results'''
        # Calling the GetLog function on the server
        res2 = client.GetLog(res['logfile'])
        timeToWait = 2
        while res2['result'] == 'RUNNING':
            # Wait to call the Get Log again
            time.sleep(timeToWait)
            # increase the waiting time, the more you wait the more you will
            # have to wait next time.
            # timeToWait += 2
            # Call the function again
            res2 = client.GetLog(res['logfile'])
        # Use the outputHandler to put the results in csv file
        print(res2)
        keys = []
        values = []
        for key in res2:
            if key == 'message':
                innerFields = ast.literal_eval(res2['message']
                                               .split('FINISHED ')[1])
                print(type(innerFields))
                for catalogKey in innerFields:
                    if catalogKey == 'catalog_ingest':
                        for ingestKey in innerFields[catalogKey]:
                            keys.append(ingestKey)
                            values.append(innerFields[catalogKey][ingestKey])
                    elif catalogKey == 'catalog_create':
                        for createKey in innerFields[catalogKey]:
                            if createKey == 'fitsprocess':
                                keys.append('create_fitsprocess')
                                values.append(innerFields[catalogKey]
                                              [createKey])
                            else:
                                keys.append(createKey)
                                values.append(innerFields[catalogKey]
                                              [createKey])
                    else:
                        keys.append(catalogKey)
                        values.append(innerFields[catalogKey])
            else:
                keys.append(key)
                values.append(res2[key])
        output = dict(zip(keys, values))
        return output

    def getQueryOutput(self, res, client):
        '''This function will query the server to know if the job is done
        and when it is, it will retrieve the results'''
        # Calling the GetLog function on the server
        res2 = client.GetLog(res['logfile'])
        timeToWait = 1
        while res2['result'] == 'RUNNING':
            # Wait to call the Get Log again
            time.sleep(timeToWait)
            # increase the waiting time, the more you wait the more you will
            # have to wait next time.
            timeToWait *= 2
            # Call the function again
            res2 = client.GetLog(res['logfile'])
        # Use the outputHandler to put the results in csv file
        print(res2)
        keys = []
        values = []
        for key in res2:
            if key == 'message':
                innerFields = ast.literal_eval(res2['message']
                                               .split('FINISHED ')[1])
                print(type(innerFields))
                for catalogKey in innerFields[0]:
                    if catalogKey == 'catalog_ingest':
                        for ingestKey in innerFields[0][catalogKey]:
                            keys.append(ingestKey)
                            values.append(innerFields[0][catalogKey][ingestKey])
                    elif catalogKey == 'catalog_create':
                        for createKey in innerFields[0][catalogKey]:
                            if createKey == 'fitsprocess':
                                keys.append('create_fitsprocess')
                                values.append(innerFields[0][catalogKey]
                                              [createKey])
                            else:
                                keys.append(createKey)
                                values.append(innerFields[0][catalogKey]
                                              [createKey])
                    else:
                        keys.append(catalogKey)
                        values.append(innerFields[0][catalogKey])
            else:
                keys.append(key)
                values.append(res2[key])
        output = dict(zip(keys, values))
        return output
