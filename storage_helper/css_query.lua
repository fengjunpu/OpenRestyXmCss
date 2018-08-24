local redis_iresty = require("common_lua.redis_iresty")
local cjson = require("cjson.safe")
local css_base_iresty = require("storage_helper.css_base")

local _M = {}      
_M._VERSION = '1.0'

local NOT_SUPPORT = 1001
local NOT_OPEN = 1002

local redis_ip = ngx.shared.shared_data:get("xmcloud_css_redis_ip")
local redis_port = 5128

--[[
local cfg_redis_ip = "120.92.117.227"
local cfg_redis_port = 5141
--]]

function internal_send_resp_string(rspstatus,message_type,error_string)
	if not message_type or type(message_type) ~= "string" then
		ngx.log(ngx.ERR, "send_resp_string:type(message_type) ~= string", type(message_type))
		ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
	end
	if not error_string or type(error_string) ~= "string" then
		ngx.log(ngx.ERR, "send_resp_string:type(error_string) ~= string", type(error_string))
		ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
	end
	
	--HTTP应答头统一都是OK，这样便于查找是应用错误，还是系统错误
	local jrsp = {}
	jrsp["CssCenter"] = {}
	jrsp["CssCenter"]["Header"] = {}
	jrsp["CssCenter"]["Header"]["Version"] = "1.0"
	jrsp["CssCenter"]["Header"]["CSeq"] = "1"
	jrsp["CssCenter"]["Header"]["MessageType"] = message_type
	jrsp["CssCenter"]["Header"]["ErrorNum"] = string.format("%d",rspstatus)
	jrsp["CssCenter"]["Header"]["ErrorString"] = error_string
	local resp_str = cjson.encode(jrsp)

	ngx.header.content_length = string.len(resp_str)
	ngx.say(resp_str)
	
end

--检查设备是否支持云存储，以及是否开通了云存储
function _M.handle_dev_query_css(self,jreq)
	--检查参数有效性
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	if not serinum then 
		return false, "invalid request format no serinum"
	end 
	
	local stgtype = jreq["CssCenter"]["Body"]["StorageType"]
	if not stgtype then 
		stgtype = "PIC"
	end
	
	local msg_type = "MSG_CSS_DEV_QUERY_RSP"								  --设备查询是否支持云存储
	if jreq["CssCenter"]["Header"]["MessageType"] == "MSG_CSS_QUERY_REQ" then --APP查询是否支持云存储
			msg_type = "MSG_CSS_QUERY_RSP"
	end
	
	local res,storage_bucket = css_base_iresty:check_abality(serinum,stgtype,"CloudStorage") --查一下支不支持
	if not res and storage_bucket then
		ngx.log(ngx.ERR,"[QueryCss]:Inter Err err:",storage_bucket," SeriNum:",serinum," type:",stgtype)
		return false,storage_bucket
	elseif res and not storage_bucket then
		ngx.log(ngx.ERR,"[QueryCss]:Not Open:",storage_bucket," SeriNum:",serinum," type:",stgtype)
		internal_send_resp_string(NOT_OPEN,msg_type,"Cloud storage device not open")
		return true,NOT_OPEN
	end 
	
	if msg_type == "MSG_CSS_DEV_QUERY_RSP"	then
		internal_send_resp_string(200,"MSG_CSS_DEV_QUERY_RSP","Sucess OK")
	end

	return true,200
end

--APP查询是否支持云存储
function _M.handle_query_css(self,jreq)
	--检查请求的有效性
	if not jreq["CssCenter"]["Body"]["SerialNumber"] or
	   not jreq["CssCenter"]["Body"]["StarTime"] or
	   not jreq["CssCenter"]["Body"]["EndTime"] then
		return false,"Invaild Request"
	end
	--ngx.log(ngx.ERR,"i@@@====>",cjson.encode(jreq))
	local res, errnum = self:handle_dev_query_css(jreq)
	if not res then 
		return false,errnum 
	end 
	
	if res and errnum ~= 200 then 
		return true
	end 
	
	local opt = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
	local red_handler = redis_iresty:new(opt)
	if not red_handler then
		return false,"redis_iresty:new failed"
	end
	
	local serinumber = jreq["CssCenter"]["Body"]["SerialNumber"]
	local redis_key = "<CLOUD_STORAGE>_"..serinumber
	local res, err = red_handler:hmget(redis_key,"VideoStgType","PicStgType")
	if not res and err then 
		return false, err
	end 
	local videostgname = res[1]
	local picstgname = res[2]
	
--[[
	local starttm = jreq["CssCenter"]["Body"]["StarTime"]
	local endtm = jreq["CssCenter"]["Body"]["EndTime"]

	local opts = { ["mysql_ip"] = mysql_ip,["mysql_port"] = mysql_port,
           ["mysql_user"] = mysql_user,["mysql_pwd"] = mysql_pwd,
           ["mysql_db"] = mysql_db,["timeout"] = 3
        }
	local handledb,err = mysql_iresty:new(opts)
	if not handledb then
			return false,err
	end
		
	local sql_cmd = "select * from user_storge_tb where SeriNum = \'"..serinum.."\'"
	local res,err = handledb:update_sql(sql_cmd)
	if not res then
			ngx.log(ngx.ERR,"sql err:",err)
			return false,err
	end
	
	local totalspace = res[1]["StorageBytes"]
	local storagename = res[1]["StorageType"]	
	
	--查询图片占用量
	local pic_cmd = "select sum(PicSize) from alarm_msg_tb where SeriNum = \'"..serinum.."\' and AlarmTime >= \'"..starttm.."\' and AlarmTime <=\'"..endtm.."\'"
	res,err = handledb:update_sql(pic_cmd)
	if not res then
        	return false,err
	end
	local picuseage = res[1]["sum(PicSize)"]

	--查询视频占用量
	local video_cmd = "select sum(VideoSize) from alarm_video_tb where SeriNum = \'"..serinum.."\' and StartTime >= \'"..starttm.."\' and StopTime <=\'"..endtm.."\'"
	res,err = handledb:update_sql(video_cmd)
	if not res then
        	return false,err
	end
	local videouseage = res[1]["sum(VideoSize)"]
	--]]
	
	local picuseage = 10
	local videouseage = 10
 
	local resp_str = {}
	resp_str["CssCenter"] = {}
	resp_str["CssCenter"]["Body"] = {}
	resp_str["CssCenter"]["Header"] = {}
    resp_str["CssCenter"]["Body"]["PicUseAge"] = picuseage
	resp_str["CssCenter"]["Body"]["VideoUseAge"] = videouseage
	resp_str["CssCenter"]["Body"]["TotalUseAge"] = picuseage + videouseage
	resp_str["CssCenter"]["Body"]["TotalSapce"] = totalspace
	--resp_str["CssCenter"]["Body"]["StorageName"] = storagename 
	if picstgname ~= ngx.null then 
		resp_str["CssCenter"]["Body"]["PicStorageName"] = picstgname
	end
	if videostgname ~= ngx.null then 
		resp_str["CssCenter"]["Body"]["VideoStorageName"] = picstgname
	end

	resp_str["CssCenter"]["Header"]["MessageType"] = "MSG_CSS_QUERY_RSP"
	resp_str["CssCenter"]["Header"]["Version"] = "v1.0"
	resp_str["CssCenter"]["Header"]["ErrorNum"] = "200"
	resp_str["CssCenter"]["Header"]["ErrorString"] = "Sucess OK"
	local resp_str = cjson.encode(resp_str)
	ngx.header.content_length = string.len(resp_str)
	ngx.say(resp_str)
	return true
end


function _M.handle_query_analysis(self,jreq)
	
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	local analysis_type = jreq["CssCenter"]["Body"]["AnalysisType"]
	local analysis_objtype = jreq["CssCenter"]["Body"]["ObjType"]
	
	--local ok,storage_bucket = css_base_iresty:check_analysis_flag(serinum,analysis_objtype)
	local ok,storage_bucket = css_base_iresty:check_abality(serinum,analysis_objtype,"AiStorage")
	if not ok and storage_bucket then 
		return false, storage_bucket
	end 
	
	if ok and not storage_bucket then 
		internal_send_resp_string(NOT_OPEN,"MSG_AI_ANALYSIS_QUERY_RSP","Dev Not Open Analysis Pic")
		return true
	end 
	
	if ok and storage_bucket then 
		local jrsp = {}
		jrsp["CssCenter"] = {}
		jrsp["CssCenter"]["Header"] = {}
		jrsp["CssCenter"]["Body"] = {}
		jrsp["CssCenter"]["Header"]["Version"] = "1.0"
		jrsp["CssCenter"]["Header"]["CSeq"] = "1"
		jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_AI_ANALYSIS_QUERY_RSP"
		jrsp["CssCenter"]["Header"]["ErrorNum"] = "200"
		jrsp["CssCenter"]["Header"]["ErrorString"] = "Success OK"
		--jrsp["CssCenter"]["Body"]["CloudDomain"] = "Just Test"
		local resp_str = cjson.encode(jrsp)
		ngx.header.content_length = string.len(resp_str)
		ngx.say(resp_str)	
	end 
	
	return true
end

return _M
