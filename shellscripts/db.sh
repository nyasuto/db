#!/bin/bash

db_set(){
    echo "$1,$2" >>  "${DATABASE_FILE:-database}"
}

db_get(){
    grep "^$1,"  "${DATABASE_FILE:-database}" | sed -e "s/^$1,//" | tail -n 1
}


#db_set 1 "foo"
#db_get 1