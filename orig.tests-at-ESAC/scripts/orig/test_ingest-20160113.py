import os, datetime
import xmlrpclib, shutil


n=50000
print n
fp_log=open("ingest_%s.log" % (datetime.datetime.now().strftime("%d-%m-%Y-%H-%M-%S"))+"_"+str(n),"w")

filename="CoadedRegriddedFrame1.tgz"
#filename="dummyRawFrame.xml"

sum_fileload=0
sum_database=0
sum_datamodel=0
sum_ingest=0
sum_total=0

client=xmlrpclib.ServerProxy('https://ECSOCDSS:gDKNnk6p@easdps02.esac.esa.int:8002')
with open(filename,"r") as handle:
    input_xml=handle.read()

for i in range(n):
    res=client.IngestObject(input_xml,'ALL',True,1)
    if res['result'].find("ok")==-1:
        print res['error']
        exit(2)
    fp_log.write("%f %f %f %f %f\n" % (res['fileload_time'],res['database_time'],res['datamodel_time'],res['ingest_time'],res['total_time']))

    sum_fileload=sum_fileload+res['fileload_time']
    sum_database=sum_database+res['database_time']
    sum_datamodel=sum_datamodel+res['datamodel_time']
    sum_ingest=sum_ingest+res['ingest_time']
    sum_total=sum_total+res['total_time']
    print i

fp_log.write("%f %f %f %f %f\n" % (sum_fileload,sum_database,sum_datamodel,sum_ingest,sum_total))
fp_log.close()
