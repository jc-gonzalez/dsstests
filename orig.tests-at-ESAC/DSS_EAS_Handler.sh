#!/bin/bash
##############################################################################
# DSS_EAS_HAndler.sh
#
# Script to store to DSS binary products, or ingest metadata into EAS,
# coming from the QLA PRocessing Framework.
#
# J.C.Gonzalez for EUCLID/ESA
##############################################################################

# Connection details

ENVIRONMENT=pip
PROJECT=TEST
SDC=SOC

##-- DSS Connection (data)
DSS_SERVER=https://eucdss.n1data.lan:8300
#DSS_USERNAME=ECSOCDSS
#DSS_PASSWORD=gDKNnk6p
DSS_USERNAME=ECSNIETO
DSS_PASSWORD=3ucl1d

DPS_SERVER=https://easdps02.esac.esa.int:8001
#DPS_SERVER=https://easdpsdb02.n1data.lan:8001
#DPS_SERVER=https://easdps-mis-${ENVIRONMENT}.esac.esa.int
DPS_USERNAME=ECMALONZO
DPS_PASSWORD=xNQ7dNk4

# Declare variables

DSS=1
EAS=2

curdir=$(pwd)

declare -A indir
indir[$DSS]="${curdir}/incoming/data"
indir[$EAS]="${curdir}/incoming/meta"

declare -A logdir
logdir[$DSS]="${curdir}/log/data"
logdir[$EAS]="${curdir}/log/meta"

declare -A archdir
archdir[$DSS]="${curdir}/archived/data"
archdir[$EAS]="${curdir}/archived/meta"

# Action to perform once the archival is done: MOVE to move to
# archived folder, else the file will be DELETED (in fact is moved to
# the archived folders, and trimmed to 0)
after_archiving="MOVE"

# Create file to continue running
running_file=${curdir}/lock
rm -rf ${running_file}
touch ${running_file}

#==================================================
# Main loop
#==================================================

while [ -f ${running_file} ]; do

    # process data files
    infld="${indir["$DSS"]}"
    archfld="${archdir["$DSS"]}"
    data_files=$(find ${infld} -type f)

    if [ -n "data_files" ]; then
        for d in ${data_files}; do
            date_tag=$(date +"%Y%m%dT%H%M%S")
            logfld="${logdir["$DSS"]}"
            logfile=${logfld}/${date_tag}.log

            python ${curdir}/scripts/dss-client.py \
                   operation=store \
                   server=${DSS_SERVER} \
                   filename=${d} \
                   debug=255 secure=True \
                   username=${DSS_USERNAME} \
                   password=${DSS_PASSWORD} 1>${logfile} 2>&1

            status=$?
            if [ $status -eq 0 ]; then
                if [ "$after_archiving" = "MOVE" ]; then
                    mv ${d} ${archfld}/
                else
                    cat /dev/null > ${d}
                    mv ${d} ${archfld}/
                fi
            fi
        done
    fi

    # process metadata files
    infld="${indir["$EAS"]}"
    archfld="${archdir["$EAS"]}"
    meta_files=$(find ${infld} -type f)

    if [ -n "meta_files" ]; then
        for d in ${meta_files}; do
            python ${curdir}/scripts/eas-ingest.py -f ${d} \
                   --server ${DPS_SERVER} \
                   --user   ${DPS_USERNAME} \
                   --passwd ${DPS_PASSWORD}

            status=$?
            if [ $status -eq 0 ]; then
                if [ "$after_archiving" = "MOVE" ]; then
                    mv ${d} ${archfld}/
                else
                    cat /dev/null > ${d}
                    mv ${d} ${archfld}/
                fi
            fi
        done
    fi

    sleep 5

done

exit 0
