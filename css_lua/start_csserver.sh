#!/bin/sh
nginx_path=`type -p nginx`
config_path=`pwd`
${nginx_path} -p ${config_path} -c ${config_path}/conf/nginx.conf

