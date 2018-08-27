#!/usr/bin/env python


import os
import sys
import json
import urllib


def build_url(url, **params):
    param_list = []

    for k, v in params.iteritems():
        if not v:
            continue
        kv = '{0}={1}'.format(k, v)
        param_list.append(kv)
    
    if not param_list:
        return url  

    query_string = '&'.join(param_list)
    return '{0}?{1}'.format(url, query_string)


def get_cfg_redisip(url):
    ip_list = []

    try:
        opener = urllib.urlopen(url)
        json_data = opener.read()
        dict_data = json.loads(json_data)
        
        for record in dict_data['records']:
            if record['value'] in ip_list:
                continue
            ip_list.append(record['value'])
    finally:
        return ':'.join(ip_list)


if __name__ == '__main__':
    ServerArea, ConfigName = map(lambda k:os.environ.get(k, ''), [
        'ServerArea', 'ConfigName'
    ])

    url = build_url('https://caps.secu100.net/privateApi/getRedisList', **{
        'ServerArea': ServerArea,
        'ConfigName': ConfigName
    })

    print get_cfg_redisip(url)
