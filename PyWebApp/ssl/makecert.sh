#!/bin/bash

NAME=`/bin/uname -n`

/bin/rm -f server.*
/usr/bin/openssl genrsa -des3 -out server.key
/usr/bin/openssl req -new -key server.key -out server.csr
/bin/cp server.key server.key.pw
/usr/bin/openssl rsa -in server.key.pw -out server.key
/usr/bin/openssl x509 -req -days 3652 -in server.csr -signkey server.key -out server.crt
