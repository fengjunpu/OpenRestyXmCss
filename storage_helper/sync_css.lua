#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

-----------------����淶˵��-----------------
--[[
���г��������ܶ������Ƶ�
˵��1>�Դ���Ӧ��Ĵ���
	��processmsg�����л���ø��������֧�������֧�����ɹ������ڲ�����httpӦ��
	�������ʧ�ܣ���processmsg�жϷ���ֵͳһӦ��
˵��2>�Լ�Ȩ�ȳ��湲�ԵĶ������ÿ���ͳһ���ű���ȥִ��
˵��3>HTTPӦ��ͷͳһ����OK���������ڲ�����Ӧ�ô��󣬻���ϵͳ����
]]

--[����������ģ��]
local tableutils = require("common_lua.tableutils")
local myconfig = require("config_lua.myconfig")		--������
local redis_iresty = require("common_lua.redis_iresty")
local wanip_iresty = require("common_lua.wanip_iresty")

--[������������]
local local_redis_ip = "127.0.0.1"
local foreign_redis_ip = "127.0.0.1"

local css_redis_port = 5128

local sync_num = 100
local css_interval = 5
local ai_interval = 5
local bucket_interval = 5


local function do_sync_auth (redis_port,server_type)
	--��ȡauth list
	local KEY = nil 
	local Prefix = nil 
	if server_type == "CSS" then 
		KEY = "<SYNC_CSTORAGE>_FLAG"
		Prefix = "<CLOUD_STORAGE>_"
	elseif server_type == "AI" then 
		KEY = "<SYNC_ANALYSIS>_FLAG"
		Prefix = "<AI_ANALYSIS>_"
	elseif server_type == "BUCKET" then 
		KEY = "<SYNC_BUCKETINFO>_FLAG"
		Prefix = "<StorageKey>_"
	else 
		return false 
	end 
	
	--�������ݿ�
	local opt = {["redis_ip"] = local_redis_ip,["redis_port"]=redis_port,["timeout"] = 12}
	local local_handler = redis_iresty:new(opt)
	if not local_handler then
		ngx.log(ngx.ERR,"redis_iresty:new failed")
		return false,"redis_iresty:new failed"
	end
	
	--��ȡ�б���
	local key_length = local_handler:llen(KEY)
	if key_length == nil then
		ngx.log(ngx.ERR,"local handler get key length failed,server_type = ",server_type)
		return false,"local handler get key length failed"
	end
	
	local list_start = 0;
	ngx.log(ngx.INFO,"@@@@@@@@@length = ",key_length," server_type = ",server_type);
	if key_length > sync_num then
		--��list���α��ƶ���������sync_num����λ�ã�Ϊ�˰����ϵ�ͬ���ˣ�
		list_start = key_length - sync_num	
	end
	
	--�ӱ����������ȡԴ������pipeline����
	local read_auth = {}
	local value_list,err = local_handler:lrange(KEY,list_start,-1);
	if not value_list then
		if err then
			ngx.log(ngx.ERR,"lrange local redis failed ",err," server_type: ",server_type)
		end	
		return false
	else
		local_handler:init_pipeline()
		for _,value in pairs(value_list) do
			local_handler:hgetall(Prefix..value)
		end	
		
		local res_status,err = local_handler:commit_pipeline()
		if not res_status or tableutils.table_is_empty(res_status) then
			ngx.log(ngx.ERR,"get server_type: ",server_type," authcode failed ",err)
			return
		else
			for i,value in pairs(res_status) do
				read_auth[value_list[i]] = value;
				ngx.log(ngx.INFO,"serinum: ",value_list[i]," authcode: ",value)
			end	
		end 
	end
	
	
	--��ʼͬ������
	local flag = 0
	local mstart_pos = 0	
	local counter = 0
	while true do
		counter = counter+1
		_,end_pos,line = string.find(foreign_redis_ip,"(%w+.%w+.%w+.%w+)",mstart_pos)		
		if not end_pos or counter > 100 then 
			break
		end	
		if line ~= nil then
			local red_handel,err = redis_iresty:new({["redis_ip"]=line,["redis_port"]=redis_port,["timeout"] = 5})
			if not red_handel then
				ngx.log(ngx.ERR,"redis_iresty new red_handel failed",err," server_type = ",server_type)
			else
				red_handel:init_pipeline();
				for key,value in pairs(read_auth) do
					red_handel:hmset("<AUTHCODE>_"..key,"Read",value,"Write",key)
				end
				local mstatus,err = red_handel:commit_pipeline();
				if not mstatus or tableutils.table_is_empty(mstatus) then
					ngx.log(ngx.ERR,"sync server_type: ",server_type," failed, ip = ",line," err:",err)
					flag = 1
				else
					ngx.log(ngx.INFO,"sync server_type: ",server_type," sucess, ip = ",line)
				end
			end
		end
		if end_pos == nil then
			break
		end
		mstart_pos = end_pos + 1 
	end

	--ɾ������list���Ѿ�ͬ�����豸��
	if flag == 0 then
		local_handler:init_pipeline()
		for _,value in pairs(serinum_list) do
			local_handler:lrem("<SYNC>_AUTHCODE",1,value)
		end
		local del_status,err = local_handler:commit_pipeline()
		if not del_status or tableutils.table_is_empty(del_status) then
			ngx.log(ngx.ERR,"lrem server_type: ",server_type," failed: ",err)
		end	
		if server_type == "CSS" then
			css_interval = 5
		elseif server_type == "AI" then
			ai_interval = 5
		elseif server_type == "BUCKET" then
			bucket_interval = 5
		end
	else 
		if server_type == "CSS" then
			css_interval = 2
		elseif server_type == "AI" then
			ai_interval = 2
		elseif server_type == "BUCKET" then
			bucket_interval = 2
		end
	end
end

--���ض�Ӧ�����ֿռ��IP��ַ������
local function load_css_redis_ip_addr()
	--�ӹ����ڴ��л�ȡ����redis ip������������redis ip
	local_redis_ip = ngx.shared.shared_data:get("myconfig_css_redis4status_ip")
	foreign_redis_ip = ngx.shared.shared_data:get("myconfig_css_foreign_redis4status_ip")
	
	if not local_redis_ip or not foreign_redis_ip then 
	--<1>��ȡiplist
		local redis_ip_list = ngx.shared.shared_data:get("myconfig_redis4css_ip_list")
		if not redis_ip_list then
			local cmd = "python get_redis_ip_list.py"
			local s = io.popen(cmd)
			redis_ip_list = s:read("*all")
			if string.len(redis_ip_list) < 6 then
					ngx.log(ngx.ERR,"get_redis_ip_list.py failed ")
					return false
			end
			ngx.shared.shared_data:set("myconfig_redis4css_ip_list", redis_ip_list)
			ngx.log(ngx.INFO,"get_redis_ip_list:",redis_ip_list)
		end
		
	--<2>���뱾��IP������������
		local counter = 0
		local mstart_pos = 0
		local end_pos = nil 
		local_redis_ip = nil 
		foreign_redis_ip = nil 
		
		while true do 
			counter = counter + 1
			_,end_pos,line = string.find(redis_ip_list,"(%w+.%w+.%w+.%w+)",mstart_pos)
			if not end_pos or counter > 100 then 
				break
			end	
			
			if line ~= nil and counter == 1 then
				local_redis_ip = line 
			end 
			
			if line ~= nil and counter > 1 then 
				if not foreign_redis_ip then
					foreign_redis_ip = line
				else
					foreign_redis_ip = foreign_redis_ip..","..line
				end	
			end 
			
			mstart_pos = end_pos + 1
		end 
		
	--<3>д�빲���ڴ�
		if not local_redis_ip or not foreign_redis_ip then 
			ngx.log(ngx.ERR,"get local redis and foregin redis failed")
			return false 
		end

		ngx.shared.shared_data:set("myconfig_css_redis4status_ip",local_redis_ip)
		ngx.shared.shared_data:set("myconfig_css_foreign_redis4status_ip",foreign_redis_ip)
	end  
	
	return true
end

--����cssͬ����ʱ��
local css_handler = nil
css_handler = function ()
	--ͬ����Ȩ��
	do_sync_auth(css_redis_port,"CSS")
	--������ʱ��
	local ok, err = ngx.timer.at(pms_interval,css_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup reclaim css_handler timer...", err)
	end
end

--����AI��ͬ����ʱ��
local ai_handler = nil
ai_handler = function ()
	--ͬ����Ȩ��
	do_sync_auth(css_redis_port,"AI")
	--������ʱ��
	local ok, err = ngx.timer.at(tps_interval,ai_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup reclaim ai_handler timer...", err)
	end
end

--����Bucket��ͬ����ʱ��
local bucket_handler = nil
bucket_handler = function ()
	--ͬ����Ȩ��
	do_sync_auth(dss_redis_port,"BUCKET")
	--������ʱ��
	local ok, err = ngx.timer.at(css_redis_port,bucket_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup reclaim bucket_handler timer...", err)
	end
end

--�������(����ʱִֻ��һ��)
--��ֻ֤��һ������ʵ��
local ok = ngx.shared.shared_data:add("init_flag",1)
if ok then
	print("startup heartbeart timer")
	local ret = load_css_redis_ip_addr()
	if not ret then
		ngx.log(ngx.ERR,"load css ip failed,need restart")
	else
		--css
		local ok, err = ngx.timer.at(css_interval,css_handler)
		if not ok then
			ngx.log(ngx.ERR, "failed to startup sync css_handler timer...", err)
		end
		--ai
		local ok, err = ngx.timer.at(ai_interval,ai_handler)
		if not ok then
			ngx.log(ngx.ERR, "failed to startup sync ai_handler timer...", err)
		end
		--bucket
		local ok, err = ngx.timer.at(bucket_interval,bucket_handler)
		if not ok then
			ngx.log(ngx.ERR, "failed to startup sync bucket_handler timer...", err)
		end
	end
else
	print("do not start timer")
end
