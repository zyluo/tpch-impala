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

parser = argparse.ArgumentParser(description='Kudu tablet utility script')
subparsers = parser.add_subparsers(title='command can be one of the following:',
                                   dest='command', metavar='<command>')

list_ = subparsers.add_parser('list', parents=[common],
                              help='List all tablet replicas '
                                   'on a Kudu Tablet Server')
list_.add_argument('-t', dest='table', choices=['lineitem', 'customer',
                                                'orders', 'part', 'partsupp',
                                                'supplier', 'nation',
                                                'region'],
                   default='', help='TPC-H table name')
list_.add_argument('ts_addr', metavar='tserver_address',
                   help="Address of a Kudu Tablet Server of form "
                        "'hostname:port'. Port may be omitted "
                        "if the Tablet Server is bound to the default port.")

move_ = subparsers.add_parser('move', parents=[common],
                              help='Move a tablet replica '
                                   'from one tablet server to another')
move_.add_argument('tm_addr', metavar='master_addresses',
                   help="Comma-separated list of Kudu Master addresses "
                        "where each address is of form 'hostname:port'. "
                        "Port may be omitted if the Tablet Server is bound "
                        "to the default port.")
move_.add_argument('from_ts_addr', metavar='from_tserver_address',
                   help="Address of a Kudu Tablet Server to move from.")
move_.add_argument('to_ts_addr', metavar='to_tserver_address',
                   help="Address of a Kudu Tablet Server to move to")
move_.add_argument('tablet_uuid', metavar='tablet_id', nargs='+',
                   help='Tablet Identifier(s)')

args = parser.parse_args()

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

GetTsUuid () {
    echo $(sed -e 's/^"//' -e 's/"$//' <<< $(kudu tserver status ${1} | grep uuid | awk '{print $2}'))
}

if [ "${TPCH_COMMAND}" = "list" ]
then
    if [ "${TPCH_VERBOSE}" = "True" ]
    then
        kudu remote_replica list ${TPCH_TS_ADDR} | pcregrep -M -B 1 -A 2 "RUNNING\nTable name: ${TPCH_TABLE}"
    else
        kudu remote_replica list ${TPCH_TS_ADDR} | pcregrep -M -B 1 -A 2 "RUNNING\nTable name: ${TPCH_TABLE}" | grep "Tablet id:" | awk '{print $3}'
    fi
elif [ "${TPCH_COMMAND}" = "move" ]
then
    from_ts_uuid=$(GetTsUuid ${TPCH_FROM_TS_ADDR})
    to_ts_uuid=$(GetTsUuid ${TPCH_TO_TS_ADDR})
    IFS=',' read -r -a uuids <<< "${TPCH_TABLET_UUID}"
    for uuid in ${uuids[@]}
    do
        if [ "${TPCH_VERBOSE}" = "True" ]
        then
            echo "$(date) INFO Move ${uuid} from ${TPCH_FROM_TS_ADDR}(${from_ts_uuid}) to ${TPCH_TO_TS_ADDR}(${to_ts_uuid})"
            echo "$(date) INFO $(kudu remote_replica list ${TPCH_FROM_TS_ADDR} | pcregrep -M -A 3 "${uuid}\nState: RUNNING" | paste -sd " " -)"
        fi
        kudu tablet change_config move_replica ${TPCH_TM_ADDR} ${uuid} ${from_ts_uuid} ${to_ts_uuid}
        if [ "${TPCH_VERBOSE}" = "True" ]
        then
            echo "$(date) INFO Done ${uuid}"
        fi
    done
fi
