#Dockerfile
FROM daocloud.io/geyijun/open_resty_common:v0.47
MAINTAINER geyijun<geyijun@xiongmaitech.com>

#采用supervisor来管理多任务
COPY supervisord.conf /etc/supervisord.conf
COPY hmac.lua /usr/local/openresty/lualib/resty/hmac.lua
COPY crypto.so /usr/local/openresty/luajit/lib/lua/5.1/crypto.so
COPY css_lua/ /xm_workspace/xmcloud3.0/css_lua/
COPY storage_helper/ /xm_workspace/xmcloud3.0/storage_helper/

RUN	chmod 777 /xm_workspace/xmcloud3.0/css_lua/*

WORKDIR /xm_workspace/xmcloud3.0/css_lua/
EXPOSE 6514 6615
CMD ["supervisord"]
