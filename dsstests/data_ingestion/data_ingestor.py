#!/usr/bin/env python
"""
dss_operator

Calls the DSS server operation requested on data files

Author: J C Gonzalez
Original code: A. N. Belikov
Last update: 2017-01-24
"""

import traceback, sys
import json

from .data_ingestion import Data_IO, Operation_DSS_Table, messageIAL


class Data_Ingestor(object):
    def __init__(self, args, cfgFile):
        self.args = args
        self.cfgFile = cfgFile
        with open(cfgFile, 'r') as f:
            self.config = json.load(f)

    def operate(self):
        if self.args.operation not in Operation_DSS_Table.keys():
            raise Exception, 'Unknown operation "{}"'.format(self.args.operation)

        op = Operation_DSS_Table[self.args.operation]

        nonfile_operations = ['ping', 'dsstestget', 'dsstestnetwork', 'dsstestconnection']
        is_file_operation = op not in nonfile_operations

        if is_file_operation and not self.args.file:
            raise Exception, 'Need filename'

        if self.args.sdc and self.args.environment:
            self.args.server = self.config['servers']['dss_server'][self.args.environment][self.args.sdc]

        if self.args.server.find("://") > -1:
            (protocol, server) = self.args.server.split("://")
            self.args.secure = (protocol == 'https')
            self.args.server = server

        ds_connect = Data_IO(self.args.server,
                             debug=self.args.debug,
                             secure=self.args.secure,
                             certfile=self.args.certfile,
                             timeout=self.args.timeout,
                             looptime=self.args.looptime,
                             logfile=self.args.logfile,
                             cfgfile=self.cfgFile)
        errormes=''
        self.argsfd=''

        try:
            if self.args.file:
                fd = None
                if self.argsfd:
                    fd = open(self.argsfd, 'w')
                if self.args.query:
                    result = getattr(ds_connect, op)(self.args.file,
                                                     savepath=self.args.local,
                                                     query=self.args.query,
                                                     username=self.args.user,
                                                     password=self.args.passwd,
                                                     fd=fd)
                else:
                    result = getattr(ds_connect, op)(self.args.file,
                                                     savepath=self.args.local,
                                                     username=self.args.user,
                                                     password=self.args.passwd,
                                                     fd=fd)
            elif self.args.operation in ['ping']:
                result = getattr(ds_connect, op)()
            else:
                result = getattr(ds_connect, op)(username=self.args.user,
                                                 password=self.args.passwd)
        except Exception, errmes:
            errormes = str(errmes)
            if ds_connect.response:
                errormes += ' DSS Server message:'
                errormes += str(ds_connect.response.reason)
            result = None
            traceback.print_tb(sys.exc_info()[2])

        if errormes:
            # Message("operation %s failure!  Reason: %s" %(self.args.operation, errmes))
            messageIAL(self.args.operation, self.args.file, self.args.local, False, errormes)
        else:
            # Message("operation %s success!  Result: %s" %(self.args.operation, result))
            if self.args.operation in ['dsstestnetwork', 'dsstestconnection', 'make_local_asy']:
                errormes=result
            messageIAL(self.args.operation, self.args.file, self.args.local, True, errormes)
