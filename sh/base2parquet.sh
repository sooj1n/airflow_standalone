#!/bin/bash

DT=$1

user="root"
password="qwer123"
#database="history_db"

MYSQL_PWD='qwer123' mysql --local-infile=1 -u"$user"<<EOF
SELECT * FROM history_db.cmd_usage
WHERE dt = '${DT}' 
EOF



