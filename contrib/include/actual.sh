#/bin/sh

servers=( 07 08 09 10 11 12 )
id_regex="Tablet id: (.+)$"

declare -A i_lineitem
declare -A a_lineitem

hash_regex=".+PARTITION ([0-9]+), HASH \(l_linenumber\) PARTITION ([0-9]+), HASH \(l_suppkey\) PARTITION ([0-9]+), HASH \(l_partkey\) PARTITION ([0-9]+),.+"

for i in ${servers[@]}
do
    asdf=$(kudu remote_replica list blazers-clx${i} | pcregrep -M -B 1 -A 1 "RUNNING\nTable name: lineitem$")
    while read -r line
    do
        #echo $line
        if [[ $line =~ $id_regex ]]
        then
            src=${BASH_REMATCH[1]}
        elif [[ $line =~ $hash_regex ]]
        then
            w=${BASH_REMATCH[1]}
            x=${BASH_REMATCH[2]}
            y=${BASH_REMATCH[3]}
            z=${BASH_REMATCH[4]}
        fi

        if [[ -n $w ]]; then
            if [[ -z "${i_lineitem[$w,$x,$y,$z]}" ]]; then
                i_lineitem[$w,$x,$y,$z]=$src
            fi
            val=${a_lineitem[$w,$x,$y,$z]}
            val+="${i} "
            a_lineitem[$w,$x,$y,$z]=${val}
            w=""
        fi
    done <<< "${asdf}"
done

declare -A i_supplier
declare -A a_supplier

hash_regex=".+PARTITION ([0-9]+), HASH \(s_suppkey\) PARTITION ([0-9]+), RANGE \(s_suppkey, s_nationkey\) PARTITION UNBOUNDED"

for i in ${servers[@]}
do
    asdf=$(kudu remote_replica list blazers-clx${i} | pcregrep -M -B 1 -A 1 "RUNNING\nTable name: supplier$")
    while read -r line
    do
        #echo $line
        if [[ $line =~ $id_regex ]]
        then
            src=${BASH_REMATCH[1]}
        elif [[ $line =~ $hash_regex ]]
        then
            w=${BASH_REMATCH[1]}
            x=${BASH_REMATCH[2]}
        fi

        if [[ -n $w ]]; then
            if [[ -z "${i_supplier[$w,$x]}" ]]; then
                i_supplier[$w,$x]=$src
            fi
            val=${a_supplier[$w,$x]}
            val+="${i} "
            a_supplier[$w,$x]=${val}
            w=""
        fi
    done <<< "${asdf}"
done

declare -A i_partsupp
declare -A a_partsupp

hash_regex=".+PARTITION ([0-9]+), HASH \(ps_partkey\) PARTITION ([0-9]+), RANGE \(ps_partkey, ps_suppkey\) PARTITION UNBOUNDED"

for i in ${servers[@]}
do
    asdf=$(kudu remote_replica list blazers-clx${i} | pcregrep -M -B 1 -A 1 "RUNNING\nTable name: partsupp$")
    while read -r line
    do
        #echo $line
        if [[ $line =~ $id_regex ]]
        then
            src=${BASH_REMATCH[1]}
        elif [[ $line =~ $hash_regex ]]
        then
            w=${BASH_REMATCH[1]}
            x=${BASH_REMATCH[2]}
        fi

        if [[ -n $w ]]; then
            if [[ -z "${i_partsupp[$w,$x]}" ]]; then
                i_partsupp[$w,$x]=$src
            fi
            val=${a_partsupp[$w,$x]}
            val+="${i} "
            a_partsupp[$w,$x]=${val}
            w=""
        fi
    done <<< "${asdf}"
done

declare -A i_customer
declare -A a_customer

hash_regex=".+PARTITION ([0-9]+), HASH \(c_custkey\) PARTITION ([0-9]+), RANGE \(c_custkey, c_nationkey\) PARTITION UNBOUNDED"

for i in ${servers[@]}
do
    asdf=$(kudu remote_replica list blazers-clx${i} | pcregrep -M -B 1 -A 1 "RUNNING\nTable name: customer$")
    while read -r line
    do
        #echo $line
        if [[ $line =~ $id_regex ]]
        then
            src=${BASH_REMATCH[1]}
        elif [[ $line =~ $hash_regex ]]
        then
            w=${BASH_REMATCH[1]}
            x=${BASH_REMATCH[2]}
        fi

        if [[ -n $w ]]; then
            if [[ -z "${i_customer[$w,$x]}" ]]; then
                i_customer[$w,$x]=$src
            fi
            val=${a_customer[$w,$x]}
            val+="${i} "
            a_customer[$w,$x]=${val}
            w=""
        fi
    done <<< "${asdf}"
done

declare -A i_orders
declare -A a_orders

hash_regex=".+PARTITION ([0-9]+), HASH \(o_custkey\) PARTITION ([0-9]+), RANGE \(o_orderkey, o_custkey\) PARTITION UNBOUNDED"

for i in ${servers[@]}
do
    asdf=$(kudu remote_replica list blazers-clx${i} | pcregrep -M -B 1 -A 1 "RUNNING\nTable name: orders$")
    while read -r line
    do
        #echo $line
        if [[ $line =~ $id_regex ]]
        then
            src=${BASH_REMATCH[1]}
        elif [[ $line =~ $hash_regex ]]
        then
            w=${BASH_REMATCH[1]}
            x=${BASH_REMATCH[2]}
        fi

        if [[ -n $w ]]; then
            if [[ -z "${i_orders[$w,$x]}" ]]; then
                i_orders[$w,$x]=$src
            fi
            val=${a_orders[$w,$x]}
            val+="${i} "
            a_orders[$w,$x]=${val}
            w=""
        fi
    done <<< "${asdf}"
done

declare -A i_part
declare -A a_part

hash_regex=".+PARTITION ([0-9]+), RANGE \(p_partkey\) PARTITION UNBOUNDED"

for i in ${servers[@]}
do
    asdf=$(kudu remote_replica list blazers-clx${i} | pcregrep -M -B 1 -A 1 "RUNNING\nTable name: part$")
    while read -r line
    do
        #echo $line
        if [[ $line =~ $id_regex ]]
        then
            src=${BASH_REMATCH[1]}
        elif [[ $line =~ $hash_regex ]]
        then
            w=${BASH_REMATCH[1]}
            x=${BASH_REMATCH[2]}
        fi

        if [[ -n $w ]]; then
            if [[ -z "${i_part[$w]}" ]]; then
                i_part[$w]=$src
            fi
            val=${a_part[$w]}
            val+="${i} "
            a_part[$w]=${val}
            w=""
        fi
    done <<< "${asdf}"
done

declare -A i_nation
declare -A a_nation

hash_regex=".+PARTITION ([0-9]+), HASH \(n_nationkey\) PARTITION ([0-9]+), RANGE \(n_nationkey, n_regionkey\) PARTITION UNBOUNDED"

for i in ${servers[@]}
do
    asdf=$(kudu remote_replica list blazers-clx${i} | pcregrep -M -B 1 -A 1 "RUNNING\nTable name: nation$")
    while read -r line
    do
        #echo $line
        if [[ $line =~ $id_regex ]]
        then
            src=${BASH_REMATCH[1]}
        elif [[ $line =~ $hash_regex ]]
        then
            w=${BASH_REMATCH[1]}
            x=${BASH_REMATCH[2]}
        fi

        if [[ -n $w ]]; then
            if [[ -z "${i_nation[$w,$x]}" ]]; then
                i_nation[$w,$x]=$src
            fi
            val=${a_nation[$w,$x]}
            val+="${i} "
            a_nation[$w,$x]=${val}
            w=""
        fi
    done <<< "${asdf}"
done

declare -A i_region
declare -A a_region

hash_regex=".+PARTITION ([0-9]+), RANGE \(r_regionkey\) PARTITION UNBOUNDED"

for i in ${servers[@]}
do
    asdf=$(kudu remote_replica list blazers-clx${i} | pcregrep -M -B 1 -A 1 "RUNNING\nTable name: region$")
    while read -r line
    do
        #echo $line
        if [[ $line =~ $id_regex ]]
        then
            src=${BASH_REMATCH[1]}
        elif [[ $line =~ $hash_regex ]]
        then
            w=${BASH_REMATCH[1]}
            x=${BASH_REMATCH[2]}
        fi

        if [[ -n $w ]]; then
            if [[ -z "${i_region[$w]}" ]]; then
                i_region[$w]=$src
            fi
            val=${a_region[$w]}
            val+="${i} "
            a_region[$w]=${val}
            w=""
        fi
    done <<< "${asdf}"
done
