#/bin/sh
set -e

TPCH_SOURCEFILE=$(mktemp)
$(which python) - "${0}" "${@}" "--source" "${TPCH_SOURCEFILE}" <<END
import argparse
import sys
import tempfile


sys.argv = sys.argv[1:]

common = argparse.ArgumentParser(add_help=False)
common.add_argument('--source', help=argparse.SUPPRESS)
common.add_argument('-v', '--verbose', action='store_true',
                    help='enable VERBOSE mode')
common.add_argument('-l', '--logfile', default=tempfile.mkstemp()[1],
                    help='set log file')

parser = argparse.ArgumentParser(description='TPC-H on Apache Impala Tool')
subparsers = parser.add_subparsers(title='These are common TPC-H commands '
                                         'used in various situations',
                                   dest='command', metavar='')

dbgen = subparsers.add_parser('dbgen', parents=[common],
                              help='populate data for use with '
                                   'the TPC-H benchmark')
load = subparsers.add_parser('load', parents=[common],
                             help='load populated data in to Apache Kudu')
                             
qgen = subparsers.add_parser('qgen', parents=[common],
                             help='generate queries for use with '
                                  'the TPC-H benchmark')

dbgen.add_argument('-s', dest='scalefactor', type=int, default=1,
                  help='set Scale Factor (SF) to <n> (default: 1)')
dbgen.add_argument('-C', dest='chunk', type=int, default=2,
                   help='separate data set into <n> chunks '
                        '(requires -S, default: 1)')
dbgen.add_argument('-F', dest='fromstep', type=int, default=1,
                   help='build starting from the <n>th step of the data set '
                        '(used with -C)')
dbgen.add_argument('-T', dest='tostep', type=int, default=2,
                   help='build until the <n>th step of the data set '
                        '(used with -C)')
dbgen.add_argument('filedir', metavar='FILEDIR', nargs='+',
                   help='set flat file directory')

load.add_argument('-k', dest='kmaster', action='append', required=True,
                  help='The <host:port> of Kudu master address(es)')
load.add_argument('-i', dest='impalad', action='append', required=True,
                  help='The <host:port> of impalad address(es)')
load.add_argument('-D', dest='database', default='tpch',
                  help='Issues a use database command for Impala on startup')
load.add_argument('-P', dest='procs', type=int, default=6,
                  help='Run up to procs concurrent load processes at a time')
load.add_argument('-S', dest='schema_refresh', action='store_true',
                  help='drop and create Kudu/Impala tables')
load.add_argument('-t', dest='table', choices=['lineitem', 'customer',
                                               'orders', 'part', 'partsupp',
                                               'supplier', 'nation',
                                               'region'],
                  required=True, help='target table name')
load.add_argument('filedir', metavar='FILEDIR', nargs='+',
                  help='set flat file directory')

args = parser.parse_args()

if args.command == 'dbgen':
    if args.scalefactor < 1:
        raise argparse.ArgumentTypeError('-s must be a positive integer')
    elif args.fromstep < 1:
        raise argparse.ArgumentTypeError('-F must be a positive integer')
    elif args.fromstep > args.tostep:
        raise argparse.ArgumentTypeError('-F must be less than or equal to -T')
    elif args.chunk < 2:
        raise argparse.ArgumentTypeError('-C must be greater than or equal to 2')
    elif args.tostep > args.chunk:
        args.tostep = args.chunk
elif args.command == 'load':

    def check_hostport(somestring, default_port):
        tokens = somestring.split(':')
        if len(tokens) < 2:
            return '%s:%d' % (somestring, default_port)
        return somestring

    args.impalad = map(lambda x: check_hostport(x, 21000), args.impalad)
    args.kmaster = map(lambda x: check_hostport(x, 7051), args.kmaster)
    if len(args.impalad) != len({}.fromkeys(args.impalad).keys()):
        raise argparse.ArgumentTypeError('duplicate impalads found')
    elif len(args.kmaster) != len({}.fromkeys(args.kmaster).keys()):
        raise argparse.ArgumentTypeError('duplicate kudu masters found')

if args.command in ['dbgen', 'load']:
    if len(args.filedir) != len({}.fromkeys(args.filedir).keys()):
        raise argparse.ArgumentTypeError('duplicate file paths found')

with open(args.source, 'w') as f:
    for arg in vars(args):
        if arg == 'source':
            continue
        val = getattr(args, arg)
        if isinstance(val, list):
            val = ','.join(val)
        f.write('TPCH_%s="%s"\n' % (arg.upper(), val))
END

source ${TPCH_SOURCEFILE}

DBGEN_HOME=2.17.3/dbgen
export DSS_CONFIG=${DBGEN_HOME}
export DSS_QUERY=${DBGEN_HOME}/queries

Dbgen() {
    IFS=',' read -r -a directories <<< "${TPCH_FILEDIR}"
    for d in ${directories[@]}
    do
        if [ ! -d "${d}" ]
        then
            mkdir -p ${d} 2>/dev/null || { echo "${d} permission denied" >&2; exit 1; }
        fi
        if [ ! -w "${d}" ]
        then
            >&2 echo "${d} permission denied"
            exit 1
        fi
    done
    local cpu_utl=$(awk '/cpu /{printf("%.4f", ($2+$4)/($2+$4+$5))}' /proc/stat)
    local cpu_cnt=$(awk '/^processor\t/{print$3}' /proc/cpuinfo | wc -l)
    local cpu_use=$(echo "(1-${cpu_utl})*${cpu_cnt}" | bc | awk -F. '{print$1}')
    if [ ${cpu_use} -lt 1 ]; then
        >&2 echo "System too busy to generate data"
        exit 1
    fi
    local var=""
    local filepath=""
    local cnt=0
    for t in L c O P S s
    do
        for x in $(seq ${TPCH_FROMSTEP} ${TPCH_TOSTEP})
        do
            filepath=${directories[$((${cnt}%${#directories[@]}))]}
            var+="${filepath} ${DBGEN_HOME} ${TPCH_SCALEFACTOR} ${t}
                  ${TPCH_CHUNK} $x "
            cnt=$((cnt+1))
        done
    done
    if [ "$TPCH_VERBOSE" = "True" ]
    then
        {
        echo "$(date "+%FT%T") Starting dbgen"
        echo $var | xargs -n 6 -P ${cpu_use} sh -c 'echo $(date "+%FT%T") \
        - Starts - table: ${4} step: ${6}; DSS_PATH=${1} \
        ${2}/dbgen -s ${3} -T ${4} -C ${5} -S ${6} -f -q; \
        echo $(date "+%FT%T") - Finish - table: ${4} step: ${6}; ' sh
        echo "$(date "+%FT%T") Finished dbgen"
        echo
        cat ${TPCH_SOURCEFILE}
        } | tee -a ${TPCH_LOGFILE} > /dev/null
    else
        echo $var | xargs -n 6 -P ${cpu_use} sh -c 'DSS_PATH=${1} \
        ${2}/dbgen -s ${3} -T ${4} -C ${5} -S ${6} -f -q' sh
    fi
    DSS_PATH=${directories[0]} $DBGEN_HOME/dbgen -s ${TPCH_SCALEFACTOR} \
    -T l -f -q
}

DropImpalaTable() {
    IFS=',' read -r -a impalads <<< "${TPCH_IMPALAD}"
    local hostport=${impalads[0]}
    local flag=$(impala-shell --quiet -i ${hostport} \
                              -q "show databases" | \
                 grep " ${TPCH_DATABASE} " | wc -l)
    if [ "${flag}" -lt 1 ]
    then
        return
    fi
    impala-shell --quiet -i ${hostport} -d ${TPCH_DATABASE} \
                 -q "drop table if exists ${TPCH_TABLE}"
}

DropCreateKuduTable() {
$(which python) - "kudu_create_table.py" "-m" "${TPCH_KMASTER}" "-t" "${TPCH_TABLE}" <<END
import argparse
import sys

import kudu
from kudu.client import Partitioning

import kudu_tpch_schema


sys.argv = sys.argv[1:]

# Parse arguments
parser = argparse.ArgumentParser(description='TPC-H Table Creation Tool '
                                             'for Apache Kudu.')
parser.add_argument('--masters', '-m', default='127.0.0.1:7051',
                    help='The master address(es) to connect to Kudu.')
parser.add_argument('-t', dest='table', choices=['lineitem', 'customer',
                                                 'orders', 'part', 'partsupp',
                                                 'supplier', 'nation',
                                                 'region'],
                    required=True, help='target table name')
args = parser.parse_args()

kudu_master_hosts = [x.split(':')[0] for x in args.masters.split(',')]
kudu_master_ports = [x.split(':')[1] for x in args.masters.split(',')]

# Connect to Kudu master server(s).
client = kudu.connect(host=kudu_master_hosts, port=kudu_master_ports)

k = args.table.upper()
v = kudu_tpch_schema.DDS_DDL[k]

# Delete table if it already exists.
if client.table_exists(k.lower()):
    client.delete_table(k.lower())

pk_column_names = [x[0].lower() for x in v
                   if x[0] in reduce(lambda x, y: x + y,
                                     kudu_tpch_schema.DDS_RI[k].values())]
builder = kudu.schema_builder()
for name, t, n, c, e, bs, d, _ in v:
    builder.add_column(name.lower(), type_=t, nullable=n, compression=c,
                       encoding=e) #, block_size=bs, default=d)
builder.set_primary_keys(pk_column_names)
schema = builder.build()

# Define the partitioning schema.
partitioning = Partitioning()
for idx, cols in kudu_tpch_schema.DDS_RI[k].iteritems():
    partitioning.add_hash_partitions(
        column_names=map(lambda x: x.lower(), cols),
        num_buckets=kudu_tpch_schema.NUM_BUCKETS[idx])

# Create a new table.
client.create_table(k.lower(), schema, partitioning)
END
}

CreateImpalaTable() {
    IFS=',' read -r -a impalads <<< "${TPCH_IMPALAD}"
    local hostport=${impalads[0]}
    impala-shell --quiet -i ${hostport} \
                 -q "create database if not exists ${TPCH_DATABASE}"
    impala-shell --quiet -i ${hostport} -d ${TPCH_DATABASE} \
                 -q "create external table ${TPCH_TABLE} stored as kudu \
                     tblproperties( \
                         'kudu.table_name' = '${TPCH_TABLE}', \
                         'kudu.master_addresses' = '${TPCH_KMASTER}')"
}

KuduPopulateTable() {
$(which python) - "kudu_populate_table.py" "-m" "${1}" "${2}" "${3}" <<END
import argparse
from itertools import islice
import ntpath
import sys

import kudu

import kudu_tpch_schema


sys.argv = sys.argv[1:]

# Parse arguments
parser = argparse.ArgumentParser(description='TPC-H Table Population Tool '
                                             'for Apache Kudu.')
parser.add_argument('--masters', '-m', default='127.0.0.1:7051',
                    help='The master address(es) to connect to Kudu.')
parser.add_argument('--verbose', action='store_true',
                    help='Verbose output')
parser.add_argument('--quiet', action='store_true',
                    help='Disable verbose output')
parser.add_argument('filepath', metavar='FILEPATH',
                    help='Text file created by dbgen.')
args = parser.parse_args()

def convert_value(type_, raw_value):
    if type_ in ['int8', 'int16', 'int32', 'int64']:
        return int(raw_value)
    elif type_ == 'float':
        return float(raw_value)
    elif type_ == 'unixtime_micros':
        return '%sT00:00:00.000000' % raw_value
    else:
        assert(type_ == 'string')
        return raw_value

def next_n_lines(file_opened, N):
    return [x.strip() for x in islice(file_opened, N)]

kudu_master_hosts = [x.split(':')[0] for x in args.masters.split(',')]
kudu_master_ports = [x.split(':')[1] for x in args.masters.split(',')]

# Connect to Kudu master server(s).
client = kudu.connect(host=kudu_master_hosts, port=kudu_master_ports,
                      rpc_timeout_ms=10000)

# Create a new session so that we can apply write operations.
session = client.new_session()

table_name = ntpath.basename(args.filepath).split('.')[0]
schema = kudu_tpch_schema.DDS_DDL[table_name.upper()]
index = [(x[7], x[0].lower(), x[1]) for x in schema]
table = client.table(table_name)
with open(args.filepath) as f:
    if args.verbose:
        print "start %s" % args.filepath
    while True:
        chunk = next_n_lines(f, 30000)
        if not chunk:
            break
        for line in chunk:
            record = map(lambda x: x.strip(), line.split('|'))
            row = [(x[1], convert_value(x[2], record[x[0]])) for x in index]
            if table_name in ['nation', 'region']:
                op = table.new_upsert(dict(row))
            else:
                op = table.new_insert(dict(row))
            session.apply(op)
        try:
            session.flush()
        except kudu.KuduBadStatus:
            print(session.get_pending_errors())
    if args.verbose:
        print "finish %s" % args.filepath
END
}

export -f KuduPopulateTable

Load() {
    local var=""
    local hostport=""
    local cnt=0
    local flag="--quiet"
    if [ "${TPCH_VERBOSE}" = "True" ]
    then
        flag="--verbose"
    fi
    IFS=',' read -r -a kmasters <<< "${TPCH_KMASTER}"
    IFS=',' read -r -a filedirs <<< "${TPCH_FILEDIR}"
    for f in $(find ${filedirs[@]} -name "${TPCH_TABLE}.tbl*" | sort | paste -sd " " -)
    do
        hostport=${kmasters[$((${cnt}%${#kmasters[@]}))]}
        var+="${hostport} ${flag} ${f} "
        cnt=$((cnt+1))
    done
    echo $var | xargs -n 3 -P ${TPCH_PROCS} sh -c 'KuduPopulateTable ${1} ${2} ${3}' sh
}

logdir=$(dirname ${TPCH_LOGFILE})
if [ ! -d "${logdir}" ]
then
    mkdir -p ${logdir} 2>/dev/null || { echo "${logdir} permission denied" >&2; exit 1; }
fi
if [ ! -f "${TPCH_LOGFILE}" ]
then
    touch ${TPCH_LOGFILE} 2>/dev/null || { echo "${TPCH_LOGFILE} permission denied" >&2; exit 1; }
fi

if [ "${TPCH_COMMAND}" = "dbgen" ]
then
    Dbgen
elif [ "${TPCH_COMMAND}" = "load" ]
then
    {
    #TPCH_IMPALASHELL_OPT="--quiet"
    #if [ "$TPCH_VERBOSE" = "True" ]
    #then
    #    TPCH_IMPALASHELL_OPT="--print_header --verbose --show_profiles"
    #fi
    if [ "$TPCH_SCHEMA_REFRESH" = "True" ]
    then
        DropImpalaTable
        DropCreateKuduTable
        CreateImpalaTable
    fi
    Load
    } | tee -a ${TPCH_LOGFILE} > /dev/null
fi
if [ "${TPCH_VERBOSE}" = "True" ]
then
    echo ${TPCH_LOGFILE}
fi
