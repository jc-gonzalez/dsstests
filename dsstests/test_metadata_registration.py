#!/usr/bin/env python
"""
test_metadata_registration

Performs a series of tests of metadata files registration

Author: J C Gonzalez
Last update: 2017-01-24
"""
import json
import argparse

from time import time

from metadata_registration.metadata_registrator import Metadata_Registrator


def get_args():
    parser = argparse.ArgumentParser(description='Test - Metadata Registration',
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog,
                                                                                         max_help_position=78))
    parser.add_argument('-C', '--cfgfile',   help="Configuration file",                                type=str, required=True)
    parser.add_argument('-s', '--server',    help="wanted server(s): [http(s)://]hostname:portnumber", type=str)
    parser.add_argument('-E', '--environment', help="Environment (current,test,pip,dev)",              type=str)
    parser.add_argument('-S', '--sdc',       help="SDC identifier",                                    type=str)
    parser.add_argument('-P', '--project',   help="Project name",                                      type=str, default="TEST")
    parser.add_argument('-T', '--testfile',  help="File name of test parameters file",                 type=str, required=True)
    parser.add_argument('-m', '--metafile',  help="Path to input metadata file",                       type=str, required=True)
    parser.add_argument('-d', '--debug',     help="debuglevel, 0 to 255 [0]",                          type=int, default=0)
    parser.add_argument('-b', '--secure',    help="True or False (don't need a certfile) [False]",     type=bool, default=False)
    parser.add_argument('-u', '--user',      help="username, or ticket for SSO ticket",                type=str, default=None)
    parser.add_argument('-p', '--passwd',    help="password or SSO ticket",                            type=str, default=None)
    parser.add_argument('-t', '--timeout',   help="timeout for connection to DSS server, in sec",      type=int, default=None)
    parser.add_argument('-i', '--looptime',  help="loop between requests for async commands, in sec",  type=int, default=1.0)
    parser.add_argument('-g', '--logfile',   help="logfile",                                           type=str, default=None)


def get_test_params(parfile):
    return json.load(parfile)

def main():
    start_time = time()

    args = get_args()
    cfgFile = args.cfgFile   # "dsstests_config.json"

    params = get_test_params(args.testfile)



    metaop = Metadata_Registrator(args, cfgFile)
    metaop.operate()

    print("Execution time: {0:.2f} s".format(time() - start_time))


if __name__ == '__main__':
    main()
