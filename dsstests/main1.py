#!/usr/bin/env python

'''
  ingest_client.py retrieve inputXMLfile|datafile  [--SDC=SDC_to_retriev_files_from] [--localdirectory=/path/to/store/file] [--environment=Euclid environment] [--noXML]
  ingest_client.py store  inputXMLfile|datafile|directory [--SDC=SDC_to_store_files] [--localdirectory=/path/to/local/files] [--environment=Euclid environment] [--noXML] [--username=username] [--password=password] [--createDDO] [--useoldfiles]

'''

from .dss_ingest_client import *


def main():
    exe_cfg = {
        'operation'     : '',
        'server'        : '',
        'filename'      : '',
        'localfilename' : '',
        'certfile'      : '',
        'debug'         : 0,
        'secure'        : True,
        'query'         : '',
        'fd'            : '',
        'username'      : '',
        'password'      : '',
        'timeout'       : None,
        'looptime'      : 1.0,
        'logfile'       : '',
    }

    # def get_params(args):
    #     path = args[3]
    #     localpath = None
    #     debug = 0
    #     if len(args) == 4:
    #         localpath = args[3]
    #     elif len(args) == 5:
    #         localpath = args[3]
    #         try:
    #             debug = int(args[4])
    #         except:
    #             debug = 0
    #             localpath = args[4]
    #     elif len(args) == 6:
    #         localpath = args[4]
    #         debug = int(args[5])
    #     else:
    #         usage()
    #         sys.exit(0)
    #     return path, localpath, debug

    def usage():
        Message('''\
    Usage:  %s operation=<o> server=<s> filename=<f> certfile=<c> debug=<d> secure=<b> query=<q>

            <o>     wanted operation, one of %s
            <s>     wanted server(s) in hostname:portnumber format
            <f>     filename
            <c>     certfile with certificate and key [no secure connect]
            <d>     debuglevel, 0 to 255 [0]
            <b>     True or False (don't need a certfile) [False]
            <q>     query to be attached to the filename

    ''' % (os.path.basename(sys.argv[0]), string.join([k for k in Operation_Table.keys()], ',')))
        sys.exit(0)

    def usage_dss():
        Message('''\
    Usage:  %s operation=<o> server=<s> filename=<f> [localfilename=<l>] certfile=<c> debug=<d> secure=<b> query=<q> username=<u> password=<p> [timeout=<t>] [looptime=<l>] [logfile=<g>]

            <o>     wanted operation, one of %s
            <s>     wanted server(s) in [http://|https://]hostname:portnumber format
            <f>     filename
            <l>     local filename to be used instead of filename, optional
            <c>     certfile with certificate and key [no secure connect]
            <d>     debuglevel, 0 to 255 [0]
            <b>     True or False (don't need a certfile) [False]
            <q>     query to be attached to the filename
            <u>     username, username=ticket for SSO ticket
            <p>     password or SSO ticket
            <t>     timeout for connection to DSS server, in sec
            <l>     loop between requests to DSS server for asynchronious commands, in sec
            <g>     logfile

    ''' % (os.path.basename(sys.argv[0]), string.join([k for k in Operation_DSS_Table.keys()], ', ')))
        sys.exit(0)

    import argparse
    from time import time

    def get_args():
        parser = argparse.ArgumentParser(description='QLA Diagnostics Processing Tool',
                                         formatter_class=lambda prog: argparse.HelpFormatter(prog,
                                                                                             max_help_position=60))

        parser.add_argument('-i', '--input_dir', help='input directory', dest='input_dir', default='./')
        parser.add_argument('-r', '--result_file', help='results file', dest='result_file', default='results.out')
        parser.add_argument('-l', '--log_file', help='execution log file', dest='log_file', default=None)
        parser.add_argument('-p', '--processors', help='number of processors', default='1', type=int)
        parser.add_argument('-v', '--verbose', help='show logging', action='store_true')

        # conf parameters
        parser.add_argument('--vis_bpm', type=str)
        parser.add_argument('--vis_sat_map_fits', type=str)
        parser.add_argument('--intermediate_products', action='store_true')
        parser.add_argument('--checks_file', type=str, default=None)

        return parser.parse_args()

    if len(sys.argv) < 3 or len(sys.argv) > 15: usage_dss()
    for arg in sys.argv[1:]:
        (key, val) = string.split(arg, '=', maxsplit=1)
        if key in exe_cfg.keys():
            if type(exe_cfg[key]) == str:
                exe_cfg[key] = val
            else:
                exe_cfg[key] = eval(val)
        else:
            raise Exception, 'Unknown key "%(key)s"' % vars()

    if exe_cfg['operation'] not in Operation_DSS_Table.keys():
        raise Exception, 'Unknown operation "%s"' % exe_cfg['operation']

    # if table[exe_cfg['operation']] not in ['getcaps', 'getid', 'getstats', 'ping', 'release', 'reload', 'testcache', 'teststore'] and not exe_cfg['filename']: raise Exception, 'Need filename'
    nonfile_operations = ['ping', 'dsstestget', 'dsstestnetwork', 'dsstestconnection']
    is_file_operation = Operation_DSS_Table[exe_cfg['operation']] not in nonfile_operations

    if is_file_operation and not exe_cfg['filename']:
        raise Exception, 'Need filename'

    if exe_cfg['server'].find("://") >-1:
        (protocol,server)=exe_cfg['server'].split("://")
        if protocol=='https':
            exe_cfg['secure']=True
        else:
            exe_cfg['secure']=False
        exe_cfg['server']=server

    ds_connect = Data_IO(exe_cfg['server'],
                         debug=exe_cfg['debug'],
                         secure=exe_cfg['secure'],
                         certfile=exe_cfg['certfile'],
                         timeout=exe_cfg['timeout'],
                         looptime=exe_cfg['looptime'],
                         logfile=exe_cfg['logfile'])
    errormes=''

    try:
        if exe_cfg['filename']:
            fd = None
            if exe_cfg['fd']:
                fd = open(exe_cfg['fd'], 'w')
            if exe_cfg['query']:
                result = getattr(ds_connect, Operation_DSS_Table[exe_cfg['operation']])(exe_cfg['filename'],
                                                                                        savepath=exe_cfg['localfilename'],
                                                                                        query=exe_cfg['query'],
                                                                                        username=exe_cfg['username'],
                                                                                        password=exe_cfg['password'],
                                                                                        fd=fd)
            else:
                result = getattr(ds_connect, Operation_DSS_Table[exe_cfg['operation']])(exe_cfg['filename'],
                                                                                        savepath=exe_cfg['localfilename'],
                                                                                        username=exe_cfg['username'],
                                                                                        password=exe_cfg['password'],
                                                                                        fd=fd)
        elif exe_cfg['operation'] in ['ping']:
            result = getattr(ds_connect, Operation_DSS_Table[exe_cfg['operation']])()
        else:
            result = getattr(ds_connect, Operation_DSS_Table[exe_cfg['operation']])(username=exe_cfg['username'],
                                                                                    password=exe_cfg['password'])
    except Exception, errmes:
        errormes=str(errmes)
        if ds_connect.response:
            errormes+=' DSS Server message:'
            errormes+=str(ds_connect.response.reason)
        result = None
        traceback.print_tb(sys.exc_info()[2])

    if errormes:
#        Message("operation %s failure!  Reason: %s" %(exe_cfg['operation'], errmes))
        messageIAL(exe_cfg['operation'], exe_cfg['filename'], exe_cfg['localfilename'], False, errormes)
    else:
#        Message("operation %s success!  Result: %s" %(exe_cfg['operation'], result))
        if exe_cfg['operation'] in ['dsstestnetwork','dsstestconnection','make_local_asy']:
            errormes=result
        messageIAL(exe_cfg['operation'], exe_cfg['filename'], exe_cfg['localfilename'], True, errormes)


if __name__ == '__main__':
    main()
