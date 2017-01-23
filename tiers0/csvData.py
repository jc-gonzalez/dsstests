#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''This class takes a csv file and write a given (from its header)
column in a list'''

import csv


class csvData(object):
    ''' This class takes csv data from res client and will make objects of it
    in order to  plot it'''

    def __init__(self, fileName, filePath, fieldNames):
        self.fileName = fileName
        self.filePath = filePath
        self.__fieldNames = fieldNames
        with open(filePath+fileName, 'r') as csvfile:
            self.content = csv.DictReader(csvfile, delimiter=',')

    def getValue(self, fieldname):
        ''' This method extract the fieldname value of each row of
        from a csv file'''
        dataArray = []
        with open(self.filePath+self.fileName, 'r') as csvfile:
            self.content = csv.DictReader(csvfile, delimiter=',')
            for row in self.content:
                try:
                    dataArray.append(float(row[fieldname]))
                except:
                    next
            return dataArray
