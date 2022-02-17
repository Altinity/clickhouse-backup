#!/bin/bash
set -x
set -e
mc admin user add local nodelete nodelete_password
mc admin policy add local nodelete <( cat << EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Effect": "Allow",
     "Action": [
       "s3:*"
     ],
     "Resource": [
       "arn:aws:s3:::clickhouse/*"
     ]
   },
   {
     "Effect": "Deny",
     "Action": [
       "s3:DeleteObject"
     ],
     "Resource": [
       "arn:aws:s3:::clickhouse/*"
     ]
   }
 ]
}
EOF
)

mc admin policy set local nodelete user=nodelete
mc alias set nodelete http://localhost:9000 nodelete nodelete_password
mc alias list
