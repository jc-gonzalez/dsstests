#!/usr/bin/env python
'''
QLA Processing Framework - Metadata Ingestion in the EAS
====================================================================

This script uses a simple XML RPC library to store received metadata
files.  These metadata files are provided by the QLA Proc. Framework
in a temporary area in incoming/meta.
'''

import argparse
import os, datetime, time
import xmlrpclib, shutil

__author__ = "J C Gonzalez"


def get_args():
    # Command line parser
    parser = argparse.ArgumentParser(description='RAW and INFO data processor')
    parser.add_argument('-f', '--meta_file', help='Path to input metadata file', dest='meta_file', required=True)
    parser.add_argument('-s', '--server',    help='URL of the server, in the form http(s)://address:port', dest='server', required=True)
    parser.add_argument('-u', '--user', help='Name of the connecting user', dest='user', required=True)
    parser.add_argument('-p', '--passwd', help='Password', dest='passwd', required=True)
    return parser.parse_args()


def main():
    # Parse command line arguments
    args = get_args()

    # Generate automatic log file name
    now_datetime = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = open("./log/meta/%s.log" % now_datetime, "w")

    url_parts = args.server.split('://');
    addr_parts = url_parts[1].split(':')

    # Generate server URI (orig: https://easdps02.esac.esa.int:8002)
    eas = {'protocol' : url_parts[0],
           'addr'     : addr_parts[0],
           'port'     : addr_parts[1],
           'user'     : args.user,
           'pwd'      : args.passwd}
    eas_uri = "%s://%s:%s@%s:%s" % (eas['protocol'], eas['user'], eas['pwd'], eas['addr'], eas['port'])

    # Open XML RPC session
    log_file.write("Creating connection to %s://%s:%s\n" % (eas['protocol'], eas['addr'], eas['port']))
    client = xmlrpclib.ServerProxy(eas_uri)

    # Read metadata file name to send
    with open(args.meta_file, "r") as handle:
        file_content = handle.read()

    # Ingest file (content) into server
    log_file.write("Ingesting %s (%d bytes)\n" % (args.meta_file, len(file_content)))
    res = client.IngestObject(file_content, 'TEST', True)  #, 1)

    # Check ingestion
    if res['result'].find("ok") == -1:
        log_file.write("ERROR: %s\n" % res['error'])
        log_file.close()
        print res['error']
        exit(2)

    log_file.write("Times: fileload=%f database=%f datamodel=%f ingest=%f total=%f\n"
                   % (res['fileload_time'], res['database_time'], res['datamodel_time'],
                      res['ingest_time'], res['total_time']))
    log_file.close()


if __name__ == '__main__':
    main()
