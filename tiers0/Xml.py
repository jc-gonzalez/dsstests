#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on: 15/06/16
Author: Magaly Alonzo
Contact: magaly.alonzo@capgemini.com
'''

import ConfigParser
import logging
import socket
import os
import time
import csv
import subprocess
import xmlrpclib
from xml.dom.minidom import parse


class Xml(object):
    ''' This class correspond to a Pipeline Processing Order.
    it can be ingested, retrieved and updated.
    it has several class attributes defines inside a configuration file:
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
    SDC = cfg.get('testValues', 'sdc')
    LOGGER = logging.getLogger("masterLogger.xmlHandling")

    def __init__(self, path, filename):
        self.productId = ''
        self.dataType = ''
        self.filename = filename
        self.inputPath = path
        self.outputPath = Xml.cfg.get('testValues', 'outputPath')
        self.fieldnames = []
        self.result = {}

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
        '''Getter of inputpath'''
        return self.inputPath

    def setOutputPath(self, path):
        '''Setter of ouputPath'''
        self.outputPath = path

    def getOutputpath(self):
        '''Getter of outputPath'''
        return self.outputPath

    def ingest(self, LOGFILE):
        '''Method to ingest xml file in the EAS-DPS server.
        Needs you to set the fileName through its setter
        By default if no SDC is set, it will raise an error'''
        self.fieldnames = ['total_time', 'username', 'datamodel_time',
                           'fileload_time', 'database_time', 'privileges',
                           'error', 'object_id', 'filename', 'project',
                           'result', 'ingest_time', 'commit', 'message',
                           'logfile', 'cpu']
        self.result = dict.fromkeys(self.fieldnames)

        # Define the server where to ingest data
        if Xml.SDC == 'SOC':
            sdcUrl = '@easdps02.esac.esa.int:8001'
            # '@easdps-mis-' + Xml.ENVIRONMENT + '.esac.esa.int'
        elif Xml.SDC == 'SDC-NL':
            sdcUrl = '@eas-dps-mis.' + Xml.ENVIRONMENT + '.euclid.astro.rug.nl'
        else:
            raise ValueError('You entered a SDC that do not exist or is not '
                             'handled yet by this program please contact '
                             'magaly.alonzo@capgemini.com')
        # Connection to the server
        client = xmlrpclib.ServerProxy('https://' + Xml.USERNAME + ':' +
                                       Xml.PASSWORD + sdcUrl)
        # Setting timeout in seconds
        socket.setdefaulttimeout(200.0)

        # Open the file to ingest and sending it to teh appropriate function
        # on the server
        f_line = open(self.inputPath + self.filename).read()
        res = client.IngestObject(f_line, Xml.PROJECT, True)
        Xml.LOGGER.info(res)

        # Parse the Dict / JSON result to store it in csv file
        for key in res:
            self.result[key] = res[key]
        if os.path.isfile(self.outputPath + LOGFILE):
            with open(self.outputPath + LOGFILE, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writerow(self.result)
        else:
            with open(self.outputPath + LOGFILE, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writeheader()
                writer.writerow(self.result)
        print(self.result)
        return self.result

    def retrieve(self, query, RETRIEVELOGFILE):
        '''Method that is constructing a query through **kwargs and send it to
        the EAS-DPS in order to download an xml file inside the given
        self.outputPath/self.fileName'''
        self.fieldnames = ['totalProcessingTime', 'unitTime', 'url']
        self.result = dict.fromkeys(self.fieldnames)
        processStart = time.time()
        # Define the server where to ingest data
        if Xml.SDC == 'SOC':
            sdcUrl = 'http://easdps-cus.' + Xml.ENVIRONMENT + '.esac.esa.int'
            # '@easdps-mis-' + Xml.ENVIRONMENT + '.esac.esa.int'
        elif Xml.SDC == 'SDC-NL':
            sdcUrl = 'http://eas-dps-cus.' + Xml.ENVIRONMENT + \
                     '.euclid.astro.rug.nl'
        else:
            raise ValueError('You entered a SDC that do not exist or is not '
                             'handled yet by this program please contact '
                             'magaly.alonzo@capgemini.com')
        url =  sdcUrl + '/EuclidXML?project=' + Xml.PROJECT + \
               '&class_name=' + self.dataType + '&' + query
        output = subprocess.PIPE
        p = subprocess.Popen(['/bin/bash', '-c', "wget \"" + url +
                              "\" -O " + self.outputPath + self.filename],
                             stdout=output, stderr=subprocess.PIPE)
        output, err = p.communicate()
        if output:
            print(output)
        if err:
            print(err)
        print(url)
        # Opening a new process to send the url containing the query
        # and saving the output to a file
        self.result['url'] = url
        self.result['unitTime'] = time.time() - processStart
        # Printing the results in a csv file
        if os.path.isfile(self.outputPath + RETRIEVELOGFILE):
            with open(self.outputPath + RETRIEVELOGFILE, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writerow(self.result)
        else:
            with open(self.outputPath + RETRIEVELOGFILE, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writeheader()
                writer.writerow(self.result)
        return output, err

    def update(self, field, newValue):
        '''This function needs the path and filename of the file you want to
        update you also need to specify the field and its new value.
        If the file is not directly updatable by the machine you should use the
        retrieve function first that will allow to retrieve the file locally and
        to modify / ingest it '''
        # Opening and editing file
        fileToUpdate = parse(self.inputPath + self.filename)
        fileToUpdate.getElementsByTagName(field)[0].childNodes[0].nodeValue = \
            newValue
        # Write the changes made on the file and close
        file_handle = open(self.inputPath + self.filename, "wb")
        file_handle.write(fileToUpdate.toxml())
        file_handle.close()
        # Starting the client and connect
        client = xmlrpclib.ServerProxy('https://' + Xml.USERNAME + ':' +
                                       Xml.PASSWORD +
                                       '@eas-dps-mis.' + Xml.ENVIRONMENT +
                                       '.euclid.astro.rug.nl')
        f_line = open(self.inputPath + self.filename).read()
        # Using client client function
        res = client.UpdateObject(f_line, Xml.PROJECT, True)
        Xml.LOGGER.info(res)
        return res, client

    def deletePpo(self, ppoId):
        '''Method to delete a PPO from the EAS-DPS from its Id'''
        client = xmlrpclib.ServerProxy('https://' + Xml.USERNAME + ':' +
                                       Xml.PASSWORD +
                                       '@eas-dps-mis.' + Xml.ENVIRONMENT +
                                       '.euclid.astro.rug.nl')

        res = client.__DeleteObject(ppoId, 'PipelineProcessingOrder', 'DSS',
                                    True)
        return res
