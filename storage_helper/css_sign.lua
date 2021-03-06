local css_base_iresty = require("storage_helper.css_base")
local cjson = require("cjson.safe")
local mysql_iresty = require("storage_helper.mysql_iresty")
local redis_iresty = require("common_lua.redis_iresty")

--[[
local mysql_ip = "117.78.36.130"
local mysql_port = 8635
--]]
--local mysql_ip = ngx.shared.shared_data:get("xmcloud_css_mysql_ip")
local mysql_port = 8635 
local mysql_user = "root"
local mysql_pwd = "123456@XiongMai"
local mysql_db = "xmcloud_css"

local redis_ip = ngx.shared.shared_data:get("xmcloud_css_redis_ip")
local redis_port = 5141

local _M = {}      
_M._VERSION = '1.0'

--[[
上传图片或者视频时的获取的签名
--]]

--[[
local redis_ip = ngx.shared.shared_data:get("xmcloud_css_redis_ip")
local redis_port = 5134
local reids_bucket_port = 5134
--]]
function _M.handle_upload_sign(self,jreq)
	if not jreq["CssCenter"]["Body"]["ObjType"] or not jreq["CssCenter"]["Body"]["ObjName"] then
		ngx.log(ngx.ERR, "do_alarm_notify invalid args")
		return false,"do_alarm_notify invalid args"
	end
	
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	local objtype = jreq["CssCenter"]["Body"]["ObjType"]
	local alarmid = jreq["CssCenter"]["Body"]["AlarmId"]
	local objname = jreq["CssCenter"]["Body"]["ObjName"]
	
	--local SignType = {AiStorage=1,CloudStorage=2,NorMalPushPic=3,VideoPic=4}
	local signType = "CloudStorage"
	if jreq["CssCenter"]["Body"]["SignType"] then 
		signType = jreq["CssCenter"]["Body"]["SignType"]
	end 
	
	--如果是图片,先检查一下是否有订阅
	if objtype == "PIC" and signType ~= "VideoPic" then 
		local res = css_base_iresty:get_subscribe_info(serinum)
		if not res then 	--设备没有订阅
			return false, "subscribe token not exists"
		end
	end 
	
	--检查是否开通了云存储功能
	local storage_bucket = nil 
	local res = nil
	--ngx.log(ngx.ERR,"======================>check abality:",serinum," objtype:",objtype," SignType:",signType)
	if not jreq["CssCenter"]["Body"]["StgBucket"] then
		res,storage_bucket = css_base_iresty:check_abality(serinum,objtype,signType)
		if not res and storage_bucket then
			ngx.log(ngx.ERR,"[UploadSign]:check abality failed err:",storage_bucket," Seri:",serinum," Type:",objtype," signTyep:",signType)
			return false,storage_bucket
		elseif res and not storage_bucket then
			ngx.log(ngx.ERR,"[UploadSign]:check abality not buy Seri:",serinum," Type:",objtype," signTyep:",signType)
			local jrsp = {}
			jrsp["CssCenter"] = {}
			jrsp["CssCenter"]["Header"] = {}
			jrsp["CssCenter"]["Body"] = {}
			jrsp["CssCenter"]["Header"]["Version"] = "1.0"
			jrsp["CssCenter"]["Header"]["CSeq"] = "1"
			jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_UPLOAD_SIGN_RSP"
			jrsp["CssCenter"]["Header"]["ErrorNum"] = string.format("%d",1002)
			jrsp["CssCenter"]["Header"]["ErrorString"] = "Dev Not Open Cloud Storage"
			jrsp["CssCenter"]["Body"]["ErrorString"] = "1002"
			local resp_str = cjson.encode(jrsp)
			ngx.header.content_length = string.len(resp_str)
			ngx.say(resp_str)
			return true
		end
	else
		storage_bucket = jreq["CssCenter"]["Body"]["StgBucket"]
	end

	--如果是视频签名请求就重写alarmid
	local format_day = nil
	if objtype == 'VIDEO' then
		local devid,serverid = string.match(alarmid,"(%w+)_(%w+)")
		if not devid or not serverid then
			local formatkey = serinum.."_"..alarmid
			local endname = string.match(objname,formatkey.."(.*)")
			serverid = ngx.time()
			alarmid = alarmid.."_"..serverid
			--ExpiredDay
			--objname = format_day.."_"..objname
			objname = serinum.."_"..alarmid..endname
			ngx.log(ngx.ERR,"[signvideo]:rewrite sign objname: ",objname)
			
			local ok, expirsday = css_base_iresty:get_storage_expirs_day(serinum,objtype)
			if not ok or not expirsday then 
				expirsday = 3
			end
			format_day = string.format("%03d",expirsday)
			objname = format_day.."_"..objname
		end
	elseif objtype == 'PIC' then
		--重写objname
		if not string.match(objname,"%w+_%w+_%w+_%w+-%w+.jpeg") then
			local ok, expirsday = css_base_iresty:get_storage_expirs_day(serinum,objtype)
			if not ok or not expirsday then 
				expirsday = 3
			end
			format_day = string.format("%03d",expirsday)
			objname = format_day.."_"..objname
		end 
	end

	local header = {} 
	local ostime = nil 
	local signature = nil
	local auth = nil
	local version4 = 0
	
	
	if storage_bucket == "S3_s3-eu-nor-01" or 
	   storage_bucket == "S3_s3-eu-pic-01" or 
	   storage_bucket == "S3_s3-vid-01" then
		ostime = os.date("%Y%m%dT%H%M%SZ")
        	header["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
	       	header["x-amz-date"] = ostime
		if storage_bucket == "S3_s3-vid-01" then 
			objname = "s3-vid-01/" .. objname
		end 	
		signature, auth = css_base_iresty:make_signature_aws_v4("PUT",header,objname,storage_bucket)
		version4 = 1
	else
		ostime = os.date("!%a, %d %b %Y %H:%M:%S GMT")
		header["Date"] = ostime
		signature = css_base_iresty:make_signature("PUT",header,objname,storage_bucket)
	end	
	
	if not signature then 
		ngx.log(ngx.ERR,"[UploadSign]:Get Sign failed bucket:",storage_bucket," Seri:",serinum," Type:",objtype," signTyep:",signType)
		return false,"get sign failed"
	end 


	local domain = ngx.shared.storage_key_data:get(storage_bucket.."_DM")
	local csskey, cssbucket = string.match(storage_bucket,"(%w+)_(.*)")
	--组织应答包
	local jrsp = {}
	jrsp["CssCenter"] = {}
	jrsp["CssCenter"]["Header"] = {}
	jrsp["CssCenter"]["Header"]["Version"] = "1.0"
	jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_UPLOAD_SIGN_RSP"
	jrsp["CssCenter"]["Header"]["ErrorNum"] = "200"
	jrsp["CssCenter"]["Header"]["ErrorString"] = "Sucess OK"
	jrsp["CssCenter"]["Body"] = {}
	jrsp["CssCenter"]["Body"]["Url"] = "/"..objname
	if objtype == 'VIDEO' then
		jrsp["CssCenter"]["Body"]["ReWriteAlarmId"] = alarmid
	end
	if format_day then
        	jrsp["CssCenter"]["Body"]["ExpiredDay"] = format_day
	end
	jrsp["CssCenter"]["Body"]["StorageName"] = csskey
	jrsp["CssCenter"]["Body"]["Method"] = "PUT"
	jrsp["CssCenter"]["Body"]["Host"] = domain
	jrsp["CssCenter"]["Body"]["Bucket"] = cssbucket
	jrsp["CssCenter"]["Body"]["RestFull"] = {}

	if version4 == 0 then
		jrsp["CssCenter"]["Body"]["RestFull"]["Date"] = header["Date"]
		jrsp["CssCenter"]["Body"]["RestFull"]["Authorization"] = signature
	else
		jrsp["CssCenter"]["Body"]["RestFull"]["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
        	jrsp["CssCenter"]["Body"]["RestFull"]["x-amz-date"] = ostime
	        jrsp["CssCenter"]["Body"]["RestFull"]["Authorization"] = auth
	end
	
	local resp_str = cjson.encode(jrsp)
	ngx.header.content_length = string.len(resp_str)
	ngx.say(resp_str)
	return true
end

function _M.handle_multi_ts_sign(self,jreq)	
	--检查参数有效性
	if not jreq["CssCenter"]["Body"]["AlarmId"] or 
	   not jreq["CssCenter"]["Body"]["AuthCode"] or 
	   not jreq["CssCenter"]["Body"]["ObjList"] or 
	   not jreq["CssCenter"]["Body"]["ObjType"] or 
	   not jreq["CssCenter"]["Body"]["SerialNumber"] or  
	   type(jreq["CssCenter"]["Body"]["ObjList"]) ~= "table" then
		return false, "Invalid Request"
	end
	
	local alarmid = jreq["CssCenter"]["Body"]["AlarmId"]
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	local offset = jreq["CssCenter"]["Body"]["OffsetNum"]
	local objtype = jreq["CssCenter"]["Body"]["ObjType"]
	
	local tempIndex = jreq["CssCenter"]["Body"]["ObjList"][4]["IndexNum"]
	local tempObjName = jreq["CssCenter"]["Body"]["ObjList"][1]["ObjName"]
	local tempser = string.match(tempObjName, "%w+_(%w+)_%w+_%w.*")
	if tempIndex > 100 and tempser == nil then
		local jrsp = {}
		jrsp["CssCenter"] = {}
		jrsp["CssCenter"]["Header"] = {}
		jrsp["CssCenter"]["Header"]["Version"] = "1.0"
		jrsp["CssCenter"]["Header"]["CSeq"] = "1"
		jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_MULTIUPLOAD_SIGN_RSP"
		jrsp["CssCenter"]["Header"]["ErrorNum"] = string.format("%d",1002)
		jrsp["CssCenter"]["Header"]["ErrorString"] = "Dev Not Open Cloud Storage"
		local resp_str = cjson.encode(jrsp)
		ngx.header.content_length = string.len(resp_str)
		ngx.say(resp_str)
		return true
	end
		
	--local res,storage_bucket = css_base_iresty:check_css_flag(serinum,objtype)
	--检查是支持视频云存储
	local res,storage_bucket = css_base_iresty:check_abality(serinum,objtype,"CloudStorage")
	if not res and storage_bucket then
		ngx.log(ngx.ERR,"[MultSign]:check abality failed err:",storage_bucket," SeriNum:",serinum)
		return false,storage_bucket
	elseif res and not storage_bucket then
		local jrsp = {}
		jrsp["CssCenter"] = {}
		jrsp["CssCenter"]["Header"] = {}
		jrsp["CssCenter"]["Header"]["Version"] = "1.0"
		jrsp["CssCenter"]["Header"]["CSeq"] = "1"
		jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_MULTIUPLOAD_SIGN_RSP"
		jrsp["CssCenter"]["Header"]["ErrorNum"] = string.format("%d",1002)
		jrsp["CssCenter"]["Header"]["ErrorString"] = "Dev Not Open Cloud Storage"
		local resp_str = cjson.encode(jrsp)
		ngx.header.content_length = string.len(resp_str)
		ngx.say(resp_str)
		ngx.log(ngx.ERR,"[MultSign]:Video Not Open SeriNum:",serinum)
		return true
	end
	
	--如果是视频签名请求就重写alarmid
	local rewrite_alarm_id = nil
	local format_day = nil
	if objtype == 'VIDEO' then
		local devid,serverid = string.match(alarmid,"(%w+)_(%w+)")
		if not devid or not serverid then
			serverid = ngx.time()
			rewrite_alarm_id = alarmid.."_"..serverid
		end
		local ok, expirsday = css_base_iresty:get_storage_expirs_day(serinum,objtype)
		if not ok or not expirsday then 
			expirsday = 3
		end
		format_day = string.format("%03d",expirsday)
	end
	
	--开始为每一个obj制作签名
	local header = {} 
	local ostime = nil
	local version4 = 0
	local signature = nil
	local auth = nil
	if storage_bucket == "S3_s3-eu-nor-01" or 
	   storage_bucket == "S3_s3-eu-pic-01" or 
	   storage_bucket == "S3_s3-vid-01" then
		ostime = os.date("%Y%m%dT%H%M%SZ")
        	header["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
	       	header["x-amz-date"] = ostime
		version4 = 1
	else
		ostime = os.date("!%a, %d %b %Y %H:%M:%S GMT")
		header["Date"] = ostime
	end	
		
	local rsp_sign_info = {}
	local objList = jreq["CssCenter"]["Body"]["ObjList"]
	for _,objinfo in pairs(objList) do 
		if objinfo and type(objinfo) == "table" then 
			local pos = objinfo["TsPos"]
			local objname = objinfo["ObjName"]
			if rewrite_alarm_id and format_day then 
				local formatkey = serinum.."_"..alarmid
				local endname = string.match(objname,formatkey.."(.*)")
				objname = format_day.."_"..serinum.."_"..rewrite_alarm_id..endname
			end
			
			local temp_sign_info = {}
			temp_sign_info["Url"] = "/"..objname
			temp_sign_info["IndexNum"] = objinfo["IndexNum"]
			temp_sign_info["RestFull"] = {}
			if version4 == 0 then 
				signature = css_base_iresty:make_signature("PUT",header,objname,storage_bucket)
				temp_sign_info["RestFull"]["Date"] = ostime
				temp_sign_info["RestFull"]["Authorization"] = signature
			else
				if storage_bucket == "S3_s3-vid-01"  then 
					objname = "s3-vid-01/" .. objname
				end
				signature, auth = css_base_iresty:make_signature_aws_v4("PUT",header,objname,storage_bucket)
				temp_sign_info["RestFull"]["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
        			temp_sign_info["RestFull"]["x-amz-date"] = ostime
	        		temp_sign_info["RestFull"]["Authorization"] = auth
			end 
			--rsp_sign_info[#rsp_sign_info + 1] = temp_sign_info
			table.insert(rsp_sign_info, temp_sign_info)
		end
	end
	
	local storagename,storagebucket = string.match(storage_bucket,"(%w+)_(.*)")
	local domain = ngx.shared.storage_key_data:get(storage_bucket.."_DM")

	local jrsp = {}
	jrsp["CssCenter"] = {}
	jrsp["CssCenter"]["Header"] = {}
	jrsp["CssCenter"]["Body"] = {}
	jrsp["CssCenter"]["Header"]["Version"] = "1.0"
	jrsp["CssCenter"]["Header"]["CSeq"] = "1"
	jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_MULTIUPLOAD_SIGN_RSP"
	jrsp["CssCenter"]["Header"]["ErrorNum"] = "200"
	jrsp["CssCenter"]["Header"]["ErrorString"] = "Success OK"
	jrsp["CssCenter"]["Body"]["Host"] = domain
	jrsp["CssCenter"]["Body"]["Bucket"] = storagebucket
	if rewrite_alarm_id then
		jrsp["CssCenter"]["Body"]["ReWriteAlarmId"] = rewrite_alarm_id
	end
	if format_day then 
		jrsp["CssCenter"]["Body"]["ExpiredDay"] = format_day
	end	
	jrsp["CssCenter"]["Body"]["StorageName"] = storagename
	jrsp["CssCenter"]["Body"]["Method"] = "PUT"
	jrsp["CssCenter"]["Body"]["SignList"] = rsp_sign_info
	
	local resp_str = cjson.encode(jrsp)
	--ngx.log(ngx.ERR,"send rsp: ",resp_str)
	ngx.header.content_length = string.len(resp_str)
	ngx.say(resp_str)
	return true
end

function _M.handle_download_sign(self,jreq)
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	if type(jreq["CssCenter"]["Body"]["ObjInfo"]) ~= "table" or not jreq["CssCenter"]["Body"]["ObjType"] then
		return false, "invalid request"	
	end
    
    	local objtype = jreq["CssCenter"]["Body"]["ObjType"]
	
	--开始构造签名 
	--是否还有存在的必要
	local res = nil 
	local storage_bucket = nil
	if objtype ~= "PIC" then 
		--res,storage_bucket = css_base_iresty:check_css_flag(serinum,objtype)
		res,storage_bucket = css_base_iresty:check_abality(serinum, objtype, "CloudStorage")
		if not res then
			return false,storage_bucket
		elseif res and not storage_bucket then
			local jrsp = {}
			jrsp["CssCenter"] = {}
			jrsp["CssCenter"]["Header"] = {}
			jrsp["CssCenter"]["Header"]["Version"] = "1.0"
			jrsp["CssCenter"]["Header"]["CSeq"] = "1"
			jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_DOWNLOAD_SIGN_RSP"
			jrsp["CssCenter"]["Header"]["ErrorNum"] = string.format("%d",1002)
			jrsp["CssCenter"]["Header"]["ErrorString"] = "Dev Not Open Cloud Storage"
			local resp_str = cjson.encode(jrsp)
			ngx.header.content_length = string.len(resp_str)
			ngx.say(resp_str)
			return true
		end	
	end 
	
	local all_map = {}
	local width = jreq["CssCenter"]["Body"]["Width"]
	local height = jreq["CssCenter"]["Body"]["Height"]
	local clienttype = jreq["CssCenter"]["Body"]["ClientType"] 
	
	for _,value in ipairs(jreq["CssCenter"]["Body"]["ObjInfo"]) do 
		local subrequest = nil
		if value.StorageBucket then 
			storage_bucket = value.StorageBucket
		end 
		
		local url = value.ObjName 		
		if clienttype and objtype == "PIC" then 
			local alarmid = string.match(value.ObjName,"%w+_(%w+).jpeg")
			if not alarmid then 
				return false, "invalid objname"
			end
			local mysql_ip = ngx.shared.shared_data:get("xmcloud_css_mysql_ip") 
			local opts = { ["mysql_ip"] = mysql_ip,["mysql_port"] = mysql_port,
					   ["mysql_user"] = mysql_user,["mysql_pwd"] = mysql_pwd,
					   ["mysql_db"] = mysql_db,["timeout"] = 3
					}
			
			local handledb,err = mysql_iresty:new(opts)
			if not handledb then
				return false,err
			end
			--从数据里面查询对应的图片名 (因为微信推送不会来主动获取图片名)
			local select_sql = "SELECT ObjName, StgFlag from alarm_msg_tb where SeriNum=\'"..serinum.."\' and AlarmId=\'"..alarmid.."\'".." limit 1"
			--ngx.log(ngx.ERR,"select sql:",select_sql)
			local res,err = handledb:update_sql(select_sql)
			if not res and err then
				ngx.log(ngx.ERR,"[DownLoadSign]:Select Sql failed err:",err," SeriNum:",serinum)
				return false,err
			end
			
			for _,v in pairs(res) do
				if not v.ObjName or not v.StgFlag then 
					return false, "alarm has no pic"
				else 
				    url = v.ObjName
					storage_bucket = v.StgFlag
				end
			end 
		end 
	
		if storage_bucket then 
			local csskey,cssbucket = string.match(storage_bucket,"(%w+)_(.*)")
			if width and height and objtype == "PIC" then
				if csskey == "OSS" then 
					subrequest = "?x-oss-process=image/resize,m_fixed,h_"..height..",w_"..width
				elseif csskey == "OBS" then 
					subrequest = "?x-image-process=image/resize,m_lfit,h_"..height..",w_"..width
				end 
			end
			
			if subrequest then
				url = url..subrequest
			end
			
			local signle_map = {}
			if clienttype then 
				local expires = {}
				--local year,month,day,hour,min,sec = string.match(ngx.utctime(),"(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)")
				--local utctime = os.time({day=day, month=month, year=year, hour=hour, min=min, sec=sec}) + 3600
				local utctime = ngx.time() + 3600
				expires["Date"] = utctime
				local _,signature = css_base_iresty:make_signature("GET",expires,url,storage_bucket)
				local accesskey = ngx.shared.storage_key_data:get(storage_bucket.."_AK")
				local accsignature = nil
				if csskey == "OBS" or csskey == "S3" then 
					accsignature = "AWSAccessKeyId="..accesskey.."&Expires="..utctime.."&Signature="..signature
				elseif csskey == "OSS" then 
					accsignature = "OSSAccessKeyId="..accesskey.."&Expires="..utctime.."&Signature="..signature
				end 

				if subrequest then
					url = url.."&"..accsignature
				else
					url = url.."?"..accsignature
				end
				
				signle_map["Host"] = ngx.shared.storage_key_data:get(storage_bucket.."_DM")	
				signle_map["URL"] = "/"..url
				all_map[#all_map + 1] = signle_map
			else
				local header = {}
				local ostime = nil
				local version4 = 0
				local host = ngx.shared.storage_key_data:get(storage_bucket.."_DM")
				if storage_bucket == "S3_s3-eu-nor-01" or
				   storage_bucket == "S3_s3-eu-pic-01" or 
				   storage_bucket == "S3_s3-vid-01" then
				        ostime = os.date("%Y%m%dT%H%M%SZ")
				        header["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
				   	header["x-amz-date"] = ostime
					header["host"] = host..":80"
			           	version4 = 1
				else
			           	ostime = os.date("!%a, %d %b %Y %H:%M:%S GMT")
			           	header["Date"] = ostime
				end
				
				if storage_bucket == "S3_s3-vid-01" then 	
					url = "s3-vid-01/" .. url						
				end

		        signle_map["Host"] = host
				signle_map["URL"] = "/"..url
				signle_map["ReqHeader"] = {}
				
				if version4 == 0 then 
					local signature = css_base_iresty:make_signature("GET",header,url,storage_bucket)
					signle_map["ReqHeader"]["Date"] = ostime
					signle_map["ReqHeader"]["Authorization"] = signature
				else 
					local signature, auth = css_base_iresty:make_signature_aws_v4("GET",header,url,storage_bucket)
					signle_map["ReqHeader"]["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
	        			signle_map["ReqHeader"]["x-amz-date"] = ostime
		        		signle_map["ReqHeader"]["Authorization"] = auth
				end 
				all_map[#all_map + 1] = signle_map
			end
		end
	end 
	
	local jrsp = {}
	jrsp["CssCenter"] = {}
	jrsp["CssCenter"]["Body"] = {}
	jrsp["CssCenter"]["Body"]["SerialNumber"] = serinum
	jrsp["CssCenter"]["Body"]["Method"] = "GET"
	jrsp["CssCenter"]["Body"]["ObjInfo"] = {}
	jrsp["CssCenter"]["Body"]["ObjInfo"] = all_map
	jrsp["CssCenter"]["Header"] = {}
	jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_DOWNLOAD_SIGN_RSP"
	jrsp["CssCenter"]["Header"]["Version"] = "1.0"
	jrsp["CssCenter"]["Header"]["ErrorString"] = "Success OK"
	jrsp["CssCenter"]["Header"]["ErrorNum"] = "200"
	
	local resp_str = cjson.encode(jrsp)
	ngx.header.content_length = string.len(resp_str)
	ngx.say(resp_str)
	return true
end

function _M.set_mysql_addr(self, css_mysql_ip) 
	if not css_mysql_ip then 
		return false
	end 
	mysql_ip = css_mysql_ip	
end

return _M
