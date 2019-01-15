#/bin/sh

combinations=( "07 08 09" "07 08 10" "07 08 11" "07 08 12" "07 09 10" "07 09 11" "07 09 12" "07 10 11" "07 10 12" "07 11 12" "08 09 10" "08 09 11" "08 09 12" "08 10 11" "08 10 12" "08 11 12" "09 10 11" "09 10 12" "09 11 12" "10 11 12" )

declare -A e_lineitem

declare -A e_supplier
declare -A e_partsupp
declare -A e_customer
declare -A e_orders

declare -A e_part

declare -A e_nation

declare -A e_region

#lineitem
cnt=0
table=lineitem
for i in {0..4}
do
    for j in {0..1}
    do
        for k in {0..1}
        do
            for l in {0..4}
            do
                e_lineitem[${i},${j},${k},${l}]=${combinations[$((${cnt}%${#combinations[@]}))]}
                cnt=$((cnt+7))
            done
        done
    done
done
#declare -p e_lineitem

for i in {0..9}
do
    for j in {0..9}
    do
        e_supplier[${i},${j}]=${combinations[$((${cnt}%${#combinations[@]}))]}
        cnt=$((cnt+7))
    done
done
#declare -p e_supplier

for i in {0..9}
do
    for j in {0..9}
    do
        e_partsupp[${i},${j}]=${combinations[$((${cnt}%${#combinations[@]}))]}
        cnt=$((cnt+7))
    done
done
#declare -p e_partsupp

for i in {0..9}
do
    for j in {0..9}
    do
        e_customer[${i},${j}]=${combinations[$((${cnt}%${#combinations[@]}))]}
        cnt=$((cnt+7))
    done
done
#declare -p e_customer

for i in {0..9}
do
    for j in {0..9}
    do
        e_orders[${i},${j}]=${combinations[$((${cnt}%${#combinations[@]}))]}
        cnt=$((cnt+7))
    done
done
#declare -p e_orders

for i in {0..99}
do
    e_part[${i}]=${combinations[$((${cnt}%${#combinations[@]}))]}
    cnt=$((cnt+7))
done
#declare -p e_part

combinations=( "07 09 11" "08 10 12" )
cnt=1

for i in {0..4}
do
    for j in {0..4}
    do
        e_nation[${i},${j}]=${combinations[$((${cnt}%${#combinations[@]}))]}
        cnt=$((cnt+1))
    done
done
#declare -p e_nation

cnt=0
for i in {0..4}
do
    e_region[${i}]=${combinations[$((${cnt}%${#combinations[@]}))]}
    cnt=$((cnt+1))
done
#declare -p e_region
