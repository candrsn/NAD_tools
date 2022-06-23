#!/bin/bash

extract_rel() {
    v="$1"
    if [ ! -r "csv" ]; then
        mkfifo csv | :
    fi
    
    for itm in r${v}/NAD*TXT.zip r${v}/NAD*ASCII.zip; do
        if [ -r "$itm" ]; then
            echo "unpack $itm"
            #unzip -j -p $itm NAD_r${v}.txt >> csv &
            #unzip -j -p $itm NAD_*.txt >> csv &
            unzip -j -p $itm TXT/NAD_*.txt >> csv &
        fi
    done
}

import_rel() {
    v="$1"
    extract_rel $v
    sleep 2
    echo "
.mode csv
.headers on
.import csv nad_data     
    " | sqlite3 nad_r${v}.db
}

r6_badlines() {
    extract_rel 6
    cat csv | awk -e 'NR==7737660 { print $0; }
NR==7738139 { print $0; }'
}

r5_badlines() {
    extract_rel 5
    cat csv | awk -e 'NR==19496830 { print $0; }
NR==19496976 { print $0; }'
}
 
