local redis_iresty = require("common_lua.redis_iresty")

local _M = {}      
_M._VERSION = '1.0'

local redis_ip = "106.14.78.92"
local redis_port = 5128
local reids_bucket_port = 5128

function internal_reflush_SecretKey(stgname_bucket)
	local ak_key =  stgname_bucket.."_AK"
	local sk_key =  stgname_bucket.."_SK"
	local dm_key =  stgname_bucket.."_DM"
	local reflush_key = stgname_bucket.."_REFLUSH"
	local now_time = ngx.time()
	local reflush_time = ngx.shared.storage_key_data:get(reflush_key)
	
	if ngx.shared.storage_key_data:get(ak_key) == nil or
	   ngx.shared.storage_key_data:get(sk_key) == nil or
	   ngx.shared.storage_key_data:get(dm_key) == nil or 
	   reflush_time == nil or (now_time - reflush_time) > 600 then 
		local opts = {["redis_ip"]=redis_ip,["redis_port"]=reids_bucket_port,["timeout"]=3}
		local red_handler = redis_iresty:new(opts)
		if not red_handler then			
			return false,"redis_iresty:new failed"	
		end
		local redis_key = "<StorageKey>_"..stgname_bucket
		local res,err = red_handler:hmget(redis_key,"SecretKey","AccessKey","StorageDomain")
		if not res and err then 
			return false, err 
		end
		
		local SecretKey = res[1]
		local AccessKey = res[2]
		local StorageDomain = res[3]
		
		if SecretKey == ngx.null or AccessKey == ngx.null 
		   or StorageDomain == ngx.null then 
			ngx.log(ngx.ERR,"Has no key stgname_bucket:",stgname_bucket)
			return false, "There is no bucket"
		else
			ngx.shared.storage_key_data:set(ak_key,AccessKey)
			ngx.shared.storage_key_data:set(sk_key,SecretKey)
			ngx.shared.storage_key_data:set(dm_key,StorageDomain)
			ngx.shared.storage_key_data:set(reflush_key,now_time)
		end
	end	
	local domain = ngx.shared.storage_key_data:get(dm_key)
	return true,domain
end 

--[[
make_signature: 生成header里面的签名头
headers: 请求头里面带什么内容，一般是Date
objectKey：bucket的名字(test-pic-123)
csskey: 云存储名称+“_”+bucket名字的组合唯一标示（OSS_test-pic-123）
--]]
function _M.make_signature(self,method,headers,objectKey,csskey)
	local param = {}
	table.insert(param, string.upper(method))
	table.insert(param, headers['Content-MD5'] or '')
	table.insert(param, headers['Content-Type'] or '')
	table.insert(param, headers['Date'])
	local ak = ngx.shared.storage_key_data:get(csskey.."_AK")  --OSS_test-pic-123_AK
	local sk = ngx.shared.storage_key_data:get(csskey.."_SK")  --OSS_test-pic-123_SK
	if not ak or not sk then 
		local ok,_ = internal_reflush_SecretKey(csskey)
		if not ok then
			return false, csskey.."has not ak or sk"
		end 
	end 
	local stortype, cssbucket = string.match(csskey,"(%w+)_(.*)")
	local canonicalizedResource = '/'
	if cssbucket then
		canonicalizedResource = canonicalizedResource..cssbucket..'/'
	end
	
	if objectKey then
		canonicalizedResource = canonicalizedResource .. objectKey
	end
	
	table.insert(param,canonicalizedResource)
	local string2Sign = table.concat(param,'\n')
	local signature = ngx.encode_base64(ngx.hmac_sha1(sk,string2Sign))
	if stortype == "S3" or stortype == "OBS" then
		stortype = "AWS"
	end
	local auth = stortype.." "..ak..":"..signature
	return auth,signature
end

--[[
check_css_flag: 检查设备云存储（图片/视频）是否开通或者是否过期
serinum: 设备序列号(c142dd39f8222e1d)
objtype: 查询类型(PIC/VIDEO)
redis_ip: user css数据库ip
redis_port： user_css数据库端口
--]]
function _M.check_css_flag(self,serinum,objtype)
	local css_key = serinum.."_"..objtype		--PIC VIDEO
	local endtime_key = serinum.."_"..objtype.."_ENDTIME"
	local css_value = ngx.shared.css_share_data:get(css_key)
	local end_time =  ngx.shared.css_share_data:get(endtime_key)
	
	if not css_value or not end_time then
		local opts = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
		local red_handler = redis_iresty:new(opts)
		if not red_handler then			
			return false,"redis_iresty:new failed"	
		end
		
		local storage_info_key = "<CLOUD_STORAGE>_"..serinum
		local res = nil
		local err = nil
		if objtype == "PIC" then 
			res,err = red_handler:hmget(storage_info_key,"PicStgEndTime","PicStgType","PicStgBucket")
			if err then 
				return false, err 
			end
		elseif objtype == "VIDEO" then 
			res,err = red_handler:hmget(storage_info_key,"VideoStgEndTime","VideoStgType","VideoStgBucket")
			if err then 
				return false, err 
			end
		end 
		
		if not res then 
			return true, nil 
		end 
		
		local endtime = res[1]
		local stgtype = res[2]
		local stgbucket = res[3]
		
		if stgbucket == ngx.null  or 
		   stgtype == ngx.null or 
		   endtime == ngx.null then 
			return true, nil 
		end 
		
		local expairs_time = endtime - ngx.time()
		if expairs_time < 0 then
			return true, nil 
		end 
		
		
		ngx.shared.css_share_data:set(css_key,stgtype.."_"..stgbucket)
		ngx.shared.css_share_data:set(endtime_key,endtime)
		css_value = stgtype.."_"..stgbucket
		internal_reflush_SecretKey(css_value)
	end 
	
	return true,css_value
end

--[[
check_analysis_flag:是否开通了人形检测功能
serinum：设备序列号
objtype: 对象类型(PIC/VIDEO)
--]]

function _M.check_analysis_flag(self,serinum,objtype)
	local endtype = nil
	if objtype == "PIC" then 
		endtype = "PIC"
	elseif objtype == "VIDEO" then
		endtype = "VID"
	else
		return false,"invalid obj type"
	end
	
	local analyse_endtime_key = serinum.."_AIENDTIME_"..endtype
	local analyse_type_key = serinum.."_AITYPE_"..endtype
	local analyse_stgbuck_key = serinum.."_AISTGBUC_"..endtype
	
	local analyse_stgbuck = ngx.shared.css_share_data:get(analyse_stgbuck_key)
	local analyse_type = ngx.shared.css_share_data:get(analyse_type_key)
	local analyse_endtime = ngx.shared.css_share_data:get(analyse_endtime_key)
	
	if not analyse_endtime or not analyse_type or 
	   not analyse_stgbuck then
		local opt = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
		local red_handler = redis_iresty:new(opt)
		if not red_handler then
			return false,"redis_iresty:new failed"
		end
		local redis_key = "<AI_ANALYSIS>_"..serinum
		local res = nil 
		local err = nil
		if objtype == "PIC" then
			res, err = red_handler:hmget(redis_key,"AnalysisPicTime","AnalysisPicType","PicStgBuck")
		elseif objtype == "VIDEO" then 
			res, err = red_handler:hmget(redis_key,"AnalysisVidTime","AnalysisVidType","VidStgBuck")
		end
		
		if not res and err then
			return false,err
		end
		
		if not res and not err then
			return true,nil
		end	
		
		analyse_endtime = res[1]
		analyse_type = res[2]
		analyse_stgbuck = res[3]
	
		if analyse_endtime == ngx.null or 
		   analyse_type == ngx.null or 
		   analyse_stgbuck == ngx.null then
			return true,nil
		else
			ngx.shared.css_share_data:set(analyse_stgbuck_key,analyse_stgbuck)
			ngx.shared.css_share_data:set(analyse_type_key,analyse_type)
			ngx.shared.css_share_data:set(analyse_endtime_key,analyse_endtime)
		end
	end
	
	if analyse_endtime ~= ngx.null then
		local expairs_time = analyse_endtime - ngx.time()
		if expairs_time < 0 then
			return true, nil
		end
	end
	
	local res1, res2 = internal_reflush_SecretKey(analyse_stgbuck)
	if not res1 then 
		return true, nil
	end
	
	return true,analyse_stgbuck
end

function _M.check_abality(self,serinum,objtype,signtype)
	if signtype == "AiStorage" then 
		local res1,res2 = _M:check_analysis_flag(serinum,objtype)
		return res1,res2
	elseif signtype == "CloudStorage" then 
		local res1, res2 = _M:check_css_flag(serinum,objtype)
		return res1,res2
	else
		ngx.log(ngx.ERR,"=======================>type serinum:",serinum," singtype:",signtype) 
		return true,"Defalut"
	end
end 

--[[
查询云存储对象的回滚时间
--]]
function _M.get_storage_expirs_day(self,serinum,objtype)
	local redis_key = nil
	if objtype == "PIC" then
		redis_key = "PicStgTime"
	elseif objtype == "VIDEO" then
		redis_key = "VideoStgTime"
	else
		return false, "InValid Storage"
	end

	local opts = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
	local red_handler = redis_iresty:new(opts)
	if not red_handler then
		return true,10
	end

	local storage_key = "<CLOUD_STORAGE>_"..serinum
	local res, err = red_handler:hget(storage_key,redis_key)
	if not res and err then
		return true,10
	end

	if res == ngx.null and not err then
		--如果不存在默认回滚时间为10天
		return true,10
	end	
	return true,res
end

return _M
