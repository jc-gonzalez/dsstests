#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
 Created on: 15/06/16
 Author: Magaly Alonzo
 Contact: magaly.alonzo@capgemini.com
 '''

from Catalog import Catalog
import ConfigParser

cfg = ConfigParser.ConfigParser()
cfg.read('easTest.cfg')
PATH = cfg.get('testValues', 'outputPath')
FILENAME = cfg.get('testValues', 'catalogFilename')
idsCatalog = [1226190753218837, 1226183202727440, 1226183207743606,
              1226190855685243, 1226190928760032, 1226192108840139]
for iteration in range(len(idsCatalog)):
    query = ' IdCatalog=' + str(idsCatalog[iteration])
    catalog = Catalog(PATH, FILENAME)
    catalog.tableName = 'NirNirObjCatalog0'
    print(query)
    logFile = 'query' + str(len(idsCatalog)) + 'Catalog.csv'
    catalog.query(query, logFile)
