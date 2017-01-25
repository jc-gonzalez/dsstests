#!/usr/bin/env python
"""
main_data_ingestion

Calls the DSS server operation requested on data files

Current vesion: J C Gonzalez
Original code: A. N. Belikov
Last update: 2017-01-24
"""

import argparse
from time import time

from data_ingestion.data_ingestor import Data_Ingestor
from data_ingestion.data_ingestion import Operation_DSS_Table
from metadata_registration.metadata_registrator import Metadata_Registrator


def get_args():
    parser = argparse.ArgumentParser(description='DSS Test Application',
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog,
                                                                                         max_help_position=78))

    valid_ops = ', '.join(Operation_DSS_Table.keys() + ['register','ingest-meta'])
    valid_ops_str = "[{}]".format(valid_ops)

    parser.add_argument('-C', '--cfgfile',   help="Configuration file",                                type=str, required=True)
    parser.add_argument('-o', '--operation', help="wanted operation, one of: " + valid_ops_str,        type=str, required=True)
    parser.add_argument('-s', '--server',    help="wanted server(s): [http(s)://]hostname:portnumber", type=str)
    parser.add_argument('-E', '--environment', help="Environment (current,test,pip,dev)",              type=str)
    parser.add_argument('-S', '--sdc',       help="SDC identifier",                                    type=str)
    parser.add_argument('-f', '--file',      help="filename",                                          type=str, default=None)
    parser.add_argument('-m', '--metafile',  help="Path to input metadata file",                       type=str, required=True)
    parser.add_argument('-l', '--local',     help="local filename to be used instead of file (opt)",   type=str, default=None)
    parser.add_argument('-c', '--certfile',  help="file with certificate and key [no secure connect]", type=str, default=None)
    parser.add_argument('-d', '--debug',     help="debuglevel, 0 to 255 [0]",                          type=int, default=0)
    parser.add_argument('-b', '--secure',    help="True or False (don't need a certfile) [False]",     type=bool, default=False)
    parser.add_argument('-q', '--query',     help="query to be attached to the filename",              type=str, default=None)
    parser.add_argument('-u', '--user',      help="username, or ticket for SSO ticket",                type=str, default=None)
    parser.add_argument('-p', '--passwd',    help="password or SSO ticket",                            type=str, default=None)
    parser.add_argument('-t', '--timeout',   help="timeout for connection to DSS server, in sec",      type=int, default=None)
    parser.add_argument('-i', '--looptime',  help="loop between requests for async commands, in sec",  type=int, default=1.0)
    parser.add_argument('-g', '--logfile',   help="logfile",                                           type=str, default=None)

    return parser.parse_args()


def main():
    start_time = time()

    args = get_args()
    cfgFile = args.cfgfile   # "dsstests_config.json"

    if (args.operation == 'register') or (args.operation == 'ingest-meta'):
        metaop = Metadata_Registrator(args, cfgFile)
        metaop.operate()
    else:
        dataop = Data_Ingestor(args, cfgFile)
        dataop.operate()

    print("Execution time: {0:.2f} s".format(time() - start_time))


if __name__ == '__main__':
    main()
