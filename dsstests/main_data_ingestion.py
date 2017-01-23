#!/usr/bin/env python
"""
main_data_ingestion

Calls the DSS server operation requested on data files

Current vesion: J C Gonzalez
Original code: A. N. Belikov
Last update: 2017-01-23
"""

from data_ingestion.data_ingestion import *

import argparse
from time import time

from xml.parsers import expat


DSSserver_current = { 'SDC-NL':'application14.target.rug.nl:8008',
                      'SDC-FR':'',
                      'SDC-IT':'',
}

DSSserver_test    = { 'SOC': 'euclidsoc.esac.esa.int:443',
                      'SDC-NL': 'application14.target.rug.nl:18443',
                      'SDC-FR': '',
                      'SDC-IT': '',
}

DSSserver_dev     = { 'SDC-NL': 'application11.target.rug.nl:18443',
                      'SDC-FR': '',
                      'SDC-IT': '',
}

DSSserver_pip     = { 'SDC-NL': 'application11.target.rug.nl:18443',
                      'SDC-FR': 'cceuclidial.in2p3.fr:8008',
                      'SDC-IT':' 140.105.72.210:443',
}

DSSserver         = { 'current': DSSserver_current,
                      'test': DSSserver_test,
                      'dev': DSSserver_dev,
                      'pip': DSSserver_pip,
}

Ingestserver      = { 'current':'https://%s:%s@eas-dps-mis.euclid.astro.rug.nl',
                      'test':'https://%s:%s@eas-dps-mis.test.euclid.astro.rug.nl',
                      'dev':'https://%s:%s@eas-dps-mis.dev.euclid.astro.rug.nl',
                      'pip':'https://%s:%s@eas-dps-mis.pip.euclid.astro.rug.nl',
}

Cusserver         = { 'current':'http://eas-dps-cus.euclid.astro.rug.nl',
                      'test':'http://eas-dps-cus.test.euclid.astro.rug.nl',
                      'dev':'http://eas-dps-cus.dev.euclid.astro.rug.nl',
                      'pip':'http://eas-dps-cus.pip.euclid.astro.rug.nl',
}


def get_args():
    parser = argparse.ArgumentParser(description='DSS Test Application',
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog,
                                                                                         max_help_position=78))

    valid_ops = ', '.join(Operation_DSS_Table.keys())
    valid_ops_str = "[{}]".format(valid_ops)

    parser.add_argument('-o', '--operation', help="wanted operation, one of: " + valid_ops_str,        type=str, required=True)
    parser.add_argument('-s', '--server',    help="wanted server(s): [http(s)://]hostname:portnumber", type=str)
    parser.add_argument('-f', '--file',      help="filename",                                          type=str, default=None)
    parser.add_argument('-l', '--local',     help="local filename to be used instead of file (opt)",   type=str, default=None)
    parser.add_argument('-c', '--certfile',  help="file with certificate and key [no secure connect]", type=str, default=None)
    parser.add_argument('-d', '--debug',     help="debuglevel, 0 to 255 [0]",                          type=int, default=0)
    parser.add_argument('-b', '--secure',    help="True or False (don't need a certfile) [False]",     type=bool, default=False)
    parser.add_argument('-q', '--query',     help="query to be attached to the filename",              type=str, default=None)
    parser.add_argument('-u', '--user',      help="username, or ticket for SSO ticket",                type=str, default=None)
    parser.add_argument('-p', '--passwd',    help="password or SSO ticket",                            type=str, default=None)
    parser.add_argument('-t', '--timeout',   help="timeout for connection to DSS server, in sec",      type=int, default=None)
    parser.add_argument('-m', '--looptime',  help="loop between requests for async commands, in sec",  type=int, default=None)
    parser.add_argument('-g', '--logfile',   help="logfile",                                           type=str, default=None)

    return parser.parse_args()


def main():
    start_time = time()

    args = get_args()

    if args.operation not in Operation_DSS_Table.keys():
        raise Exception, 'Unknown operation "{}"'.format(args.operation)

    op = Operation_DSS_Table[args.operation]

    nonfile_operations = ['ping', 'dsstestget', 'dsstestnetwork', 'dsstestconnection']
    is_file_operation = op not in nonfile_operations

    if is_file_operation and not args.file:
        raise Exception, 'Need filename'

    if args.server.find("://") > -1:
        (protocol, server) = args.server.split("://")
        args.secure = (protocol == 'https')
        args.server = server

    ds_connect = Data_IO(args.server,
                         debug=args.debug,
                         secure=args.secure,
                         certfile=args.certfile,
                         timeout=args.timeout,
                         looptime=args.looptime,
                         logfile=args.logfile)
    errormes=''

    try:
        if args.file:
            fd = None
            if args.fd:
                fd = open(args.fd, 'w')
            if args.query:
                result = getattr(ds_connect, op)(args.file,
                                                 savepath=args.local,
                                                 query=args.query,
                                                 username=args.user,
                                                 passwd=args.passwd,
                                                 fd=fd)
            else:
                result = getattr(ds_connect, op)(args.file,
                                                 savepath=args.local,
                                                 username=args.user,
                                                 passwd=args.passwd,
                                                 fd=fd)
        elif args.operation in ['ping']:
            result = getattr(ds_connect, op)()
        else:
            result = getattr(ds_connect, op)(username=args.user,
                                             passwd=args.passwd)
    except Exception, errmes:
        errormes = str(errmes)
        if ds_connect.response:
            errormes += ' DSS Server message:'
            errormes += str(ds_connect.response.reason)
        result = None
        traceback.print_tb(sys.exc_info()[2])

    if errormes:
#        Message("operation %s failure!  Reason: %s" %(args.operation, errmes))
        messageIAL(args.operation, args.file, args.local, False, errormes)
    else:
#        Message("operation %s success!  Result: %s" %(args.operation, result))
        if args.operation in ['dsstestnetwork', 'dsstestconnection', 'make_local_asy']:
            errormes=result
        messageIAL(args.operation, args.file, args.local, True, errormes)

    elapsed_time = time() - start_time
    print("Execution time: {0:.2f} s".format(elapsed_time))


if __name__ == '__main__':
    main()
