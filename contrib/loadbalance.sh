#/bin/sh

source ./include/expect.sh
source ./include/actual.sh

GetTsUuid () {
    sed -e 's/^"//' -e 's/"$//' <<< $(kudu tserver status ${1} | grep uuid | awk '{print $2}')
}

for i in "${!e_lineitem[@]}"
do
    answer=${e_lineitem[$i]}
    reality=${a_lineitem[$i]}
    ids=( ${a_lineitem[$i]} )
    for j in ${ids[@]}
    do
        if [[ ${answer} == *"${j}"* ]] && [[ ${reality} == *"${j}"* ]]
        then
            answer=${answer//${j}}
            reality=${reality//${j}}
        fi
    done
    if [[ -z "${answer// }" ]]
    then
        continue
    fi
    src=( $reality )
    dst=( $answer )
    echo "id     : ${i_lineitem[$i]}"
    for j in "${!src[@]}"; do
        echo "${src[$j]} -> ${dst[$j]}"
        from_ts_uuid=$(GetTsUuid "blazers-clx${src[$j]}")
        to_ts_uuid=$(GetTsUuid "blazers-clx${dst[$j]}")
        kudu tablet change_config move_replica blazers-clx06 ${i_lineitem[$i]} ${from_ts_uuid} ${to_ts_uuid}
    done
done

for i in "${!e_supplier[@]}"
do
    answer=${e_supplier[$i]}
    reality=${a_supplier[$i]}
    ids=( ${a_supplier[$i]} )
    for j in ${ids[@]}
    do
        if [[ ${answer} == *"${j}"* ]] && [[ ${reality} == *"${j}"* ]]
        then
            answer=${answer//${j}}
            reality=${reality//${j}}
        fi
    done
    if [[ -z "${answer// }" ]]
    then
        continue
    fi
    src=( $reality )
    dst=( $answer )
    echo "id     : ${i_supplier[$i]}"
    for j in "${!src[@]}"; do
        echo "${src[$j]} -> ${dst[$j]}"
        from_ts_uuid=$(GetTsUuid "blazers-clx${src[$j]}")
        to_ts_uuid=$(GetTsUuid "blazers-clx${dst[$j]}")
        kudu tablet change_config move_replica blazers-clx06 ${i_supplier[$i]} ${from_ts_uuid} ${to_ts_uuid}
    done
done

for i in "${!e_partsupp[@]}"
do
    answer=${e_partsupp[$i]}
    reality=${a_partsupp[$i]}
    ids=( ${a_partsupp[$i]} )
    for j in ${ids[@]}
    do
        if [[ ${answer} == *"${j}"* ]] && [[ ${reality} == *"${j}"* ]]
        then
            answer=${answer//${j}}
            reality=${reality//${j}}
        fi
    done
    if [[ -z "${answer// }" ]]
    then
        continue
    fi
    src=( $reality )
    dst=( $answer )
    echo "id     : ${i_partsupp[$i]}"
    for j in "${!src[@]}"; do
        echo "${src[$j]} -> ${dst[$j]}"
        from_ts_uuid=$(GetTsUuid "blazers-clx${src[$j]}")
        to_ts_uuid=$(GetTsUuid "blazers-clx${dst[$j]}")
        kudu tablet change_config move_replica blazers-clx06 ${i_partsupp[$i]} ${from_ts_uuid} ${to_ts_uuid}
    done
done

for i in "${!e_customer[@]}"
do
    answer=${e_customer[$i]}
    reality=${a_customer[$i]}
    ids=( ${a_customer[$i]} )
    for j in ${ids[@]}
    do
        if [[ ${answer} == *"${j}"* ]] && [[ ${reality} == *"${j}"* ]]
        then
            answer=${answer//${j}}
            reality=${reality//${j}}
        fi
    done
    if [[ -z "${answer// }" ]]
    then
        continue
    fi
    src=( $reality )
    dst=( $answer )
    echo "id     : ${i_customer[$i]}"
    for j in "${!src[@]}"; do
        echo "${src[$j]} -> ${dst[$j]}"
        from_ts_uuid=$(GetTsUuid "blazers-clx${src[$j]}")
        to_ts_uuid=$(GetTsUuid "blazers-clx${dst[$j]}")
        kudu tablet change_config move_replica blazers-clx06 ${i_customer[$i]} ${from_ts_uuid} ${to_ts_uuid}
    done
done

for i in "${!e_orders[@]}"
do
    answer=${e_orders[$i]}
    reality=${a_orders[$i]}
    ids=( ${a_orders[$i]} )
    for j in ${ids[@]}
    do
        if [[ ${answer} == *"${j}"* ]] && [[ ${reality} == *"${j}"* ]]
        then
            answer=${answer//${j}}
            reality=${reality//${j}}
        fi
    done
    if [[ -z "${answer// }" ]]
    then
        continue
    fi
    src=( $reality )
    dst=( $answer )
    echo "id     : ${i_orders[$i]}"
    for j in "${!src[@]}"; do
        echo "${src[$j]} -> ${dst[$j]}"
        from_ts_uuid=$(GetTsUuid "blazers-clx${src[$j]}")
        to_ts_uuid=$(GetTsUuid "blazers-clx${dst[$j]}")
        kudu tablet change_config move_replica blazers-clx06 ${i_orders[$i]} ${from_ts_uuid} ${to_ts_uuid}
    done
done

for i in "${!e_part[@]}"
do
    answer=${e_part[$i]}
    reality=${a_part[$i]}
    ids=( ${a_part[$i]} )
    for j in ${ids[@]}
    do
        if [[ ${answer} == *"${j}"* ]] && [[ ${reality} == *"${j}"* ]]
        then
            answer=${answer//${j}}
            reality=${reality//${j}}
        fi
    done
    if [[ -z "${answer// }" ]]
    then
        continue
    fi
    src=( $reality )
    dst=( $answer )
    echo "id     : ${i_part[$i]}"
    for j in "${!src[@]}"; do
        echo "${src[$j]} -> ${dst[$j]}"
        from_ts_uuid=$(GetTsUuid "blazers-clx${src[$j]}")
        to_ts_uuid=$(GetTsUuid "blazers-clx${dst[$j]}")
        kudu tablet change_config move_replica blazers-clx06 ${i_part[$i]} ${from_ts_uuid} ${to_ts_uuid}
    done
done

#declare -p i_nation
#declare -p a_nation

for i in "${!e_nation[@]}"
do
    answer=${e_nation[$i]}
    reality=${a_nation[$i]}
    ids=( ${a_nation[$i]} )
    for j in ${ids[@]}
    do
        if [[ ${answer} == *"${j}"* ]] && [[ ${reality} == *"${j}"* ]]
        then
            answer=${answer//${j}}
            reality=${reality//${j}}
        fi
    done
    if [[ -z "${answer// }" ]]
    then
        continue
    fi
    src=( $reality )
    dst=( $answer )
    echo "id     : ${i_nation[$i]}"
    for j in "${!src[@]}"; do
        echo "${src[$j]} -> ${dst[$j]}"
        from_ts_uuid=$(GetTsUuid "blazers-clx${src[$j]}")
        to_ts_uuid=$(GetTsUuid "blazers-clx${dst[$j]}")
        kudu tablet change_config move_replica blazers-clx06 ${i_nation[$i]} ${from_ts_uuid} ${to_ts_uuid}
    done
done

for i in "${!e_region[@]}"
do
    answer=${e_region[$i]}
    reality=${a_region[$i]}
    ids=( ${a_region[$i]} )
    for j in ${ids[@]}
    do
        if [[ ${answer} == *"${j}"* ]] && [[ ${reality} == *"${j}"* ]]
        then
            answer=${answer//${j}}
            reality=${reality//${j}}
        fi
    done
    if [[ -z "${answer// }" ]]
    then
        continue
    fi
    src=( $reality )
    dst=( $answer )
    echo "id     : ${i_region[$i]}"
    for j in "${!src[@]}"; do
        echo "${src[$j]} -> ${dst[$j]}"
        from_ts_uuid=$(GetTsUuid "blazers-clx${src[$j]}")
        to_ts_uuid=$(GetTsUuid "blazers-clx${dst[$j]}")
        kudu tablet change_config move_replica blazers-clx06 ${i_region[$i]} ${from_ts_uuid} ${to_ts_uuid}
    done
done
