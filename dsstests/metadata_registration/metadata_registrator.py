#!/usr/bin/env python
"""
dss_operator

Calls the DSS server operation requested on data files

Author: J C Gonzalez
Original code: A. N. Belikov
Last update: 2017-01-24
"""

import json
import datetime
import xmlrpclib

class Metadata_Registrator(object):
    def __init__(self, args, cfgFile):
        self.args = args
        self.cfgFile = cfgFile
        with open(cfgFile, 'r') as f:
            self.config = json.load(f)

    def operate(self):
        server = None
        if self.args.server:
            server = self.args.server
        else:
            if self.args.sdc and self.args.environment:
                self.args.server = self.config['servers']['ingest_server'][self.args.environment][self.args.sdc]
                server = self.args.server
            else:
                print "ERROR: Either SDC+Environment or a Server must be specified"
                exit(2)

        url_parts = server.split('://')
        addr_parts = url_parts[1].split(':')

        # Generate server URI (orig: https://easdps02.esac.esa.int:8002)
        eas = {'protocol': url_parts[0],
               'addr': addr_parts[0],
               'port': addr_parts[1],
               'user': self.args.user,
               'pwd': self.args.passwd}
        eas_uri = "%s://%s:%s@%s:%s" % (eas['protocol'], eas['user'], eas['pwd'], eas['addr'], eas['port'])

        now_datetime = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")

        log_file = None
        if self.args.logfile:
            # Generate automatic log file name
            log_file = open(self.args.logfile, "w")
            log_file.write("%s: Creating connection to %s://%s:%s\n" % (now_datetime, eas['protocol'], eas['addr'], eas['port']))
        else:
            print "%s: Creating connection to %s://%s:%s\n" % (now_datetime, eas['protocol'], eas['addr'], eas['port'])

        client = xmlrpclib.ServerProxy(eas_uri)

        # Read metadata file name to send
        with open(self.args.metafile, "r") as handle:
            file_content = handle.read()

        # Ingest file (content) into server
        if log_file:
            log_file.write("Ingesting %s (%d bytes)\n" % (self.args.metafile, len(file_content)))
        else:
            print "Ingesting %s (%d bytes)\n" % (self.args.metafile, len(file_content))

        res = client.IngestObject(file_content, 'TEST', True)  #, 1)

        # Check ingestion
        if res['result'].find("ok") == -1:
            if log_file:
                log_file.write("ERROR: %s\n" % res['error'])
                log_file.close()
            print res['error']
            exit(2)

        if log_file:
            log_file.write("Times: fileload=%f database=%f datamodel=%f ingest=%f total=%f\n"
                           % (res['fileload_time'], res['database_time'], res['datamodel_time'],
                          res['ingest_time'], res['total_time']))
            log_file.close()
        else:
            print "Times: fileload=%f database=%f datamodel=%f ingest=%f total=%f\n" % (res['fileload_time'],
                                                                                        res['database_time'],
                                                                                        res['datamodel_time'],
                                                                                        res['ingest_time'],
                                                                                        res['total_time'])
