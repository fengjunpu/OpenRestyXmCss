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

local css_redis_port = 5134
local sync_num = 100
local css_pic_interval = 5
local css_vid_interval = 5
local ai_interval = 5
local bucket_interval = 5


local function do_sync_auth(redis_port,server_type)
	--��ȡauth list
	local KEY = nil 
	local Prefix = nil 
	local args = {}
	if server_type == "CSS_PIC" then 
		KEY = "<SYNC_PIC_CSS>_FLAG"
		Prefix = "<CLOUD_STORAGE>_"
		args = {"PicStgTime","PicStgEndTime","PicStgSize","PicStgType","PicStgBucket"}
	elseif server_type == "CSS_VIDEO" then
		KEY = "<SYNC_VIDEO_CSS>_FLAG"
		Prefix = "<CLOUD_STORAGE>_"
		args = {"VideoStgTime","VideoStgEndTime","VideoStgSize","VideoStgType","VideoStgBucket"}
	elseif server_type == "AI" then 
		KEY = "<SYNC_ANALYSIS>_FLAG"
		Prefix = "<AI_ANALYSIS>_"
		args = {"AnalysisPicTime","PicStgBuck","AnalysisPicType","Pedestrian","Enable"}
	elseif server_type == "BUCKET" then 
		KEY = "<SYNC_BUCKETINFO>_FLAG"
		Prefix = "<StorageKey>_"
		args = {"BucketName","StorageDomain","SecretKey","AccessKey","StorageName","RegionName"}
	else 
		return false 
	end 
	
	--�������ݿ�
	local opt = {["redis_ip"] = local_redis_ip,["redis_port"] = redis_port,["timeout"] = 10}
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
	
	local list_start = 0
	ngx.log(ngx.INFO,"[dosync]length = ",key_length," server_type = ",server_type);
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
	end 
	
	--��ʼ��ȡredis����
	local_handler:init_pipeline()
	for _,value in pairs(value_list) do
		local key = Prefix..value
		for _, arg in pairs(args) do
			local_handler:hget(key,arg)
		end
	end	
	
	local res_status,err = local_handler:commit_pipeline()
	
	if not res_status or next(res_status) == nil then		--û�оͷ���
		ngx.log(ngx.ERR,"get server_type: ",server_type," authcode failed ",err)
		return false 
	end 
	
	local redis_info = {}
	local args_len = #args
	local status_len = (#value_list)*(#args) --�ܳ���
	local key_index = 1	
	for n = 1, status_len, args_len do		--����Ϊ�������ݳ���
		local m_key = value_list[key_index]
		key_index = key_index + 1
		local dev_flag = 0		--�Ƿ�鵽redis����
		local on_arry = {}
		for i = 1, args_len do 
			if res_status[n+i-1] and res_status[n+i-1] ~= ngx.null then 
				on_arry[args[i]] = res_status[n+i-1]
				dev_flag = 1
			end
			
			if dev_flag == 1 then 	--�������Ϣ��Ҫͬ�����ȷ���json��������
				on_arry["syncflag"] = m_key
				redis_info[#redis_info + 1] = on_arry
			end 
		end 
	end 

	if #redis_info == 0 then  --�����������û������˵��û��ʲô��Ҫͬ���ľͷ���
		return false 
	end 
	
	--��ʼͬ������
	local mstart_pos = 0	
	local counter = 0
	local sync_sucess_flag = 0
	while true do
		counter = counter + 1
		_,end_pos,line = string.find(foreign_redis_ip,"(%w+.%w+.%w+.%w+)",mstart_pos)		
		if not end_pos or counter > 100 then 
			break
		end	
		
		if line ~= nil then
			local red_handel,err = redis_iresty:new({["redis_ip"] = line,["redis_port"] = redis_port,["timeout"] = 10})
			if not red_handel then
				ngx.log(ngx.ERR,"redis_iresty new red_handel failed",err," server_type = ",server_type)
			else
				red_handel:init_pipeline();
				--��ʼд������
				for _, syncinfo in pairs(redis_info) do 
					local obj_key = Prefix..syncinfo["syncflag"] 
					if server_type == "CSS_PIC" then 
						--{"PicStgTime","PicStgEndTime","PicStgSize","PicStgType","PicStgBucket"}
						red_handel:hmset(obj_key,"PicStgTime",syncinfo["PicStgTime"],
											"PicStgEndTime",syncinfo["PicStgEndTime"],
											"PicStgSize",syncinfo["PicStgSize"],
											"PicStgType",syncinfo["PicStgType"],
											"PicStgBucket",syncinfo["PicStgBucket"])
					elseif server_type == "CSS_VIDEO" then
						--{"VideoStgTime","VideoStgEndTime","VideoStgSize","VideoStgType","VideoStgBucket"}
						red_handel:hmset(obj_key,"VideoStgTime",syncinfo["VideoStgTime"],
											"VideoStgEndTime",syncinfo["VideoStgEndTime"],
											"VideoStgSize",syncinfo["VideoStgSize"],
											"VideoStgType",syncinfo["VideoStgType"],
											"VideoStgBucket",syncinfo["VideoStgBucket"])
					elseif server_type == "AI" then 
						--{"AnalysisPicTime","PicStgBuck","AnalysisPicType","Pedestrian","Enable"}
						red_handel:hmset(obj_key,"AnalysisPicTime",syncinfo["AnalysisPicTime"],
											"PicStgBuck",syncinfo["PicStgBuck"],
											"AnalysisPicType",syncinfo["AnalysisPicType"],
											"Pedestrian",syncinfo["Pedestrian"],
											"Enable",syncinfo["Enable"])
					elseif server_type == "BUCKET" then 
						--{"BucketName","StorageDomain","SecretKey","AccessKey","StorageName","RegionName"}
						red_handel:hmset(obj_key,"BucketName",syncinfo["BucketName"],
											"StorageDomain",syncinfo["StorageDomain"],
											"SecretKey",syncinfo["SecretKey"],
											"AccessKey",syncinfo["AccessKey"],
											"StorageName",syncinfo["StorageName"],
											"RegionName",syncinfo["RegionName"])
					end 
				end 
				
				local res_status,err = red_handel:commit_pipeline()  --��ʼͬ��
				if not res_status or #res_status == 0 or err then  --ͬ��ʧ��
					sync_sucess_flag = 0
					ngx.log(ngx.ERR,"hmset failed=============> err:",err," server_type:",server_type," redis ip:",line)
				else
					sync_sucess_flag = 1
				end
			end
		end
		
		if end_pos == nil then
			break
		end
		mstart_pos = end_pos + 1 
	end

	--ɾ������list���Ѿ�ͬ�����豸��
	if sync_sucess_flag == 0 then
		local_handler:init_pipeline()
		for _,value in pairs(redis_info) do
			local obj_key = value["syncflag"] 
			local_handler:lrem(KEY,1,obj_key)
		end
		local del_status,err = local_handler:commit_pipeline()
		if not del_status or tableutils.table_is_empty(del_status) then
			ngx.log(ngx.ERR,"lrem server_type: ",server_type," failed: ",err)
		end	
		if server_type == "CSS_PIC" then
			css_pic_interval = 5
		elseif server_type == "CSS_PIC" then
			css_vid_interval = 5
		elseif server_type == "AI" then
			ai_interval = 5
		elseif server_type == "BUCKET" then
			bucket_interval = 5
		end
	else 
		if server_type == "CSS_PIC" then
			css_pic_interval = 2
		elseif server_type == "CSS_PIC" then
			css_vid_interval = 2
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
	local_redis_ip = ngx.shared.shared_data:get("myconfig_css_local_redis4status_ip")
	foreign_redis_ip = ngx.shared.shared_data:get("myconfig_css_foreign_redis4status_ip")
	
	if not local_redis_ip or not foreign_redis_ip then 
	--<1>��ȡiplist
		local redis_ip_list = ngx.shared.shared_data:get("myconfig_redis4status_ip_list")
		if not redis_ip_list then
			local cmd = "python get_redis_ip_list.py"
			local s = io.popen(cmd)
			redis_ip_list = s:read("*all")
			if string.len(redis_ip_list) < 6 then
				ngx.log(ngx.ERR,"get_redis_ip_list.py failed ")
				return false
			end
			ngx.shared.shared_data:set("myconfig_redis4status_ip_list", redis_ip_list)
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

		ngx.shared.shared_data:set("myconfig_css_local_redis4status_ip",local_redis_ip)
		ngx.shared.shared_data:set("myconfig_css_foreign_redis4status_ip",foreign_redis_ip)
	end  
	
	return true
end

--����cssͬ����ʱ��
local css_pic_handler = nil
css_pic_handler = function ()
	--ͬ����Ȩ��
	do_sync_auth(css_redis_port,"CSS_PIC")
	--������ʱ��
	local ok, err = ngx.timer.at(css_pic_interval,css_pic_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup css_pic_handler timer...", err)
	end
end

local css_vid_handler = nil
css_vid_handler = function ()
	--ͬ����Ȩ��
	do_sync_auth(css_redis_port,"CSS_VIDEO")
	--������ʱ��
	local ok, err = ngx.timer.at(css_vid_interval,css_vid_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup rcss_vid_handler timer...", err)
	end
end

--����AI��ͬ����ʱ��
local ai_handler = nil
ai_handler = function ()
	--ͬ����Ȩ��
	do_sync_auth(css_redis_port,"AI")
	--������ʱ��
	local ok, err = ngx.timer.at(ai_interval,ai_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup ai_handler timer...", err)
	end
end

--����Bucket��ͬ����ʱ��
local bucket_handler = nil
bucket_handler = function ()
	--ͬ����Ȩ��
	do_sync_auth(css_redis_port,"BUCKET")
	--������ʱ��
	local ok, err = ngx.timer.at(bucket_interval,bucket_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup bucket_handler timer...", err)
	end
end

--�������(����ʱִֻ��һ��)
--��ֻ֤��һ������ʵ��
local ok = ngx.shared.shared_data:add("init_flag",1)
if ok then
	local ret = load_css_redis_ip_addr()
	if not ret then
		ngx.log(ngx.ERR,"load css ip failed,need restart")
	else
		--css_pic
		local ok, err = ngx.timer.at(css_pic_interval,css_pic_handler)
		if not ok then
			ngx.log(ngx.ERR, "failed to startup sync css_pic_handler timer...", err)
		end
		--css_video
		local ok, err = ngx.timer.at(css_vid_interval,css_vid_handler)
		if not ok then
			ngx.log(ngx.ERR, "failed to startup sync css_video_handler timer...", err)
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
