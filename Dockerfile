#Dockerfile
FROM daocloud.io/geyijun/open_resty_common:v0.41
MAINTAINER geyijun<geyijun@xiongmaitech.com>

#采用supervisor来管理多任务
COPY supervisord.conf /etc/supervisord.conf
COPY css_lua/ /xm_workspace/xmcloud3.0/css_lua/
COPY storage_helper/ /xm_workspace/xmcloud3.0/storage_helper/

RUN	chmod 777 /xm_workspace/xmcloud3.0/css_lua/*

WORKDIR /xm_workspace/xmcloud3.0/css_lua/
EXPOSE 6514 6615
CMD ["supervisord"]
