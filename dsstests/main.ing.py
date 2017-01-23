def main():
    def usage():
        Message('''\nUsage: \n %s retrieve inputXMLfile|datafile  [--SDC=SDC_to_retriev_files_from] [--localdirectory=/path/to/store/file] [--environment=Euclid environment] [--noXML] [--project=EAS_project] [--nocert]
 %s store  inputXMLfile|datafile|directory [--SDC=SDC_to_store_files] [--localdirectory=/path/to/local/files] [--environment=Euclid environment] [--noXML] [--username=username] [--password=password] [--createDDO] [--useoldfiles] [--project=EAS_project] [--nocert]
                                              ''' % (os.path.basename(sys.argv[0]),os.path.basename(sys.argv[0])))


    debug = 0
    errmes=None
    dss_server=''
    ec_environment=None
    SDC='SDC-NL'
    localpath=None
    path=None
    debug=0
    username=''
    password=''
    useoldfiles=False
    createDDO=False
    noXML=False
    ddo_input=''
    load_metadata=True
    userproject='ALL'
    nocert=False

    if len(sys.argv) < 3 or sys.argv[1] not in ['retrieve', 'store']:
        usage()
    elif sys.argv[1] == 'retrieve':
        if '--noXML' in sys.argv:
            for param in sys.argv:
                p_kv=param.split("=")
                if len(p_kv)==2:
                    if p_kv[0]=='--environment':
                        ec_environment=p_kv[1]
                    if p_kv[0]=='--SDC':
                        SDC=p_kv[1]
                    if p_kv[0]=='--localdirectory':
                        localpath=p_kv[1]
                    if p_kv[0]=='--username':
                        username=p_kv[1]
                    if p_kv[0]=='--password':
                        password=p_kv[1]
                    if p_kv[0]=='--project':
                        userproject=p_kv[1]
                    if p_kv[0]=='--nocert':
                        nocert=p_kv[1]
            try:
                dss_server=DSSserver[ec_environment][SDC]
            except:
                Message('no such environment or SDC: --environment=%s --SDC=%s' % (ec_environment,SDC))
                sys.exit()

            ds_connect = Data_IO(connect_string(dss_server), debug=debug, nocert=nocert)
            path=sys.argv[2]
            path_check=path
            if localpath:
                path_check=os.path.join(localpath,path)
                localpath=path_check
            if os.path.exists(path_check):
                Message("Data file already exist on local disk %s" % (path_check))
            else:
                errormes=''
                try:
                    t1=time.time()
                    r = ds_connect.get(path=path, savepath=localpath, username=username, password=password)
                    t2=time.time()
                    Message('Data file retrieved for %.1f sec: %s' % (t2-t1,path_check))
                except Exception, errmes:
                    errormes=str(errmes)
                    if ds_connect.response:
                        errormes+=' DSS Server message:'
                        errormes+=str(ds_connect.response.reason)
                    r = False
                    messageIAL('retrieve',path,localpath,r,errormes)
        else:
            filenames=load_xml_file(sys.argv[2])
            for param in sys.argv:
                p_kv=param.split("=",1)
                if len(p_kv)==2:
                    if p_kv[0]=='--environment':
                        ec_environment=p_kv[1]
                    if p_kv[0]=='--SDC':
                        SDC=p_kv[1]
                    if p_kv[0]=='--localdirectory':
                        localpath=p_kv[1]
                    if p_kv[0]=='--username':
                        username=p_kv[1]
                    if p_kv[0]=='--password':
                        password=p_kv[1]
                    if p_kv[0]=='--project':
                        userproject=p_kv[1]
                    if p_kv[0]=='--nocert':
                        nocert=p_kv[1]
            try:
                dss_server=DSSserver[ec_environment][SDC]
            except:
                Message('no such environment or SDC: --environment=%s --SDC=%s' % (ec_environment,SDC))
                sys.exit()
            ds_connect = Data_IO(connect_string(dss_server), debug=debug,nocert=nocert)
            for f_i in filenames:
                errormes=''
                path_check=f_i
                localpath_i=localpath
                if localpath:
                    path_check=os.path.join(localpath,f_i)
                    localpath_i=path_check
                if os.path.exists(path_check):
                    Message("Data file already exist on local disk %s" % (path_check))
                else:
                    try:
                        t1=time.time()
                        r = ds_connect.get(path=f_i, savepath=localpath_i, username=username, password=password)
                        t2=time.time()
                        Message('Data file retrieved for %.1f sec: %s' % (t2-t1,path_check))
                    except Exception, errmes:
                        errormes=str(errmes)
                        if ds_connect.response:
                            errormes+=' DSS Server message:'
                            errormes+=str(ds_connect.response.reason)
                        r = False
                        messageIAL('retrieve',f_i,localpath,r,errormes)
                        sys.exit()
    elif sys.argv[1] == 'store':
        path=sys.argv[2]
        for param in sys.argv:
            p_kv=param.split("=")
            if len(p_kv)==2:
                if p_kv[0]=='--environment':
                    ec_environment=p_kv[1]
                if p_kv[0]=='--SDC':
                    SDC=p_kv[1]
                if p_kv[0]=='--localdirectory':
                    localpath=p_kv[1]
                if p_kv[0]=='--username':
                    username=p_kv[1]
                if p_kv[0]=='--password':
                    password=p_kv[1]
                if p_kv[0]=='--useoldfiles':
                    useoldfiles=p_kv[1]
                if p_kv[0]=='--project':
                    userproject=p_kv[1]
                if p_kv[0]=='--nocert':
                    nocert=p_kv[1]
        if '--useoldfiles' in sys.argv:
            useoldfiles=True
        if '--createDDO' in sys.argv:
            createDDO=True
        if '--noXML' in sys.argv:
            noXML=True
        if not ec_environment:
            Message('Specify environment')
            sys.exit()
        if os.path.isdir(path):
            parentddotype='TEST'
            parentddoid=int(100*(datetime.datetime.now()-datetime.datetime(1990,1,1,1,1,1)).total_seconds())
            purpose='TEST'
            if not username or not password:
                Message('Specify username and password')
                sys.exit()
            for root, dirnames, inpfilenames in os.walk(path):
                Message('Processing directory: %s' % (root))
                for xmlfilename in fnmatch.filter(inpfilenames,'*.xml'):
                    load_metadata=True
                    Message('Found xml file: %s' % (xmlfilename))
                    filenames=load_xml_file(os.path.join(root,xmlfilename))
                    for f_i in filenames:
                        if not store_datafile(f_i,os.path.join(root,f_i),ec_environment,SDC,username=username,password=password,useoldfiles=useoldfiles,nocert=nocert):
                            load_metadata=False
                            break
                    if not noXML and load_metadata:
                        load_metadata = store_metadatafile(os.path.join(root,xmlfilename),ec_environment, userproject, username, password)
                    else:
                        Message('Due to previous errors in ingesting data files metadata file is not ingested: %s ' % xmlfilename)

                    if createDDO and not noXML and not load_metadata:
                        createDDO=False
                        Message('Due to errors in ingesting metadata file DDO will not be created for files in %s' % (xmlfilename))

                    if createDDO:
                        for sdc_i in SDCList:
                            if sdc_i != SDC:
                                ddo_input=fillDDO(filenames,parentddotype,parentddoid,sdc_i,purpose)
                                store_metadatafile(ddo_input,ec_environment,'DSS',username,password)
                    if not noXML and not load_metadata:
                        Message('Due to previous errors in ingesting data files or metadata metadata file is not ingested: %s ' % xmlfilename)
        else:
            if noXML:
                if localpath:
                    localpath=os.path.join(localpath,path)
                store_datafile(path,localpath,ec_environment,SDC,username=username,password=password,useoldfiles=useoldfiles,nocert=nocert)
                if createDDO:
                    head, filename = os.path.split(path)
                    parentddotype='TEST'
                    parentddoid=int(100*(datetime.datetime.now()-datetime.datetime(1990,1,1,1,1,1)).total_seconds())
                    purpose='TEST'
                    for sdc_i in SDCList:
                        if sdc_i != SDC:
                            ddo_input=fillDDO([filename],parentddotype,parentddoid,sdc_i,purpose)
                            store_metadatafile(ddo_input,ec_environment,'DSS',username,password)
            else:
                if not username or not password:
                    Message('Specify username and password')
                    sys.exit()
                filenames=load_xml_file(sys.argv[2])
                for f_i in filenames:
                    localpath_f_i=None
                    if localpath:
                        localpath_f_i=os.path.join(localpath,f_i)
                    if not store_datafile(f_i,localpath_f_i,ec_environment,SDC,username=username,password=password,useoldfiles=useoldfiles,nocert=nocert):
                        sys.exit()

                if not noXML:
                    load_metadata = store_metadatafile(path,ec_environment,userproject, username, password)
                else:
                    Message('Metadata files is skipped: %s' % (xmlfilename))

                if createDDO and not noXML and not load_metadata:
                    createDDO=False
                    Message('Due to errors in ingesting metadata file DDO will not be created for files in %s' % (xmlfilename))

                if createDDO:
                    parentddotype='TEST'
                    parentddoid=int(100*(datetime.datetime.now()-datetime.datetime(1990,1,1,1,1,1)).total_seconds())
                    purpose='TEST'
                    for sdc_i in SDCList:
                        if sdc_i != SDC:
                            ddo_input=fillDDO(filenames,parentddotype,parentddoid,sdc_i,purpose)
                            store_metadatafile(ddo_input,ec_environment,'DSS',username,password)

    else:
        usage()




if __name__ == '__main__':
    main()
