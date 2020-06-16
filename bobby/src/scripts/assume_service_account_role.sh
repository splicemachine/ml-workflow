#!/usr/bin/env bash
aws sts assume-role-with-web-identity \
 --role-arn $AWS_ROLE_ARN \
 --role-session-name mh9test \
 --web-identity-token file://$AWS_WEB_IDENTITY_TOKEN_FILE  > /tmp/irp-cred.txt
