local redis_iresty = require("common_lua.redis_iresty")
local restystr = require("resty.string")
local resty_sha265 = require("resty.sha256")
local resty_hmac = require("resty.hmac")

local _M = {}      
_M._VERSION = '1.0'

local redis_ip = ngx.shared.shared_data:get("xmcloud_css_redis_ip")
local redis_port = 5134
local reids_bucket_port = 5134
local reids_pms_port = 5131
local redis_cfg_port = 5141

function sha256(str)
    local sha256 = resty_sha265:new()
    sha256:update(str)
    local digest = sha256:final()
    return string.lower(restystr.to_hex(digest))
end

function hmac_sha256(sk, str)
    local hm, err = resty_hmac:new(sk)
    local signature, hmac_signature, hex_signature = hm:generate_signature("sha256",str)
    if not signature then
        ngx.log(ngx.ERR, "failed to sign message: ", hmac_signature)
        return false, err
    end
    return signature, hmac_signature, hex_signature
end

function internal_reflush_SecretKey(stgname_bucket)
	local ak_key =  stgname_bucket.."_AK"
	local sk_key =  stgname_bucket.."_SK"
	local dm_key =  stgname_bucket.."_DM"
	local rg_key = stgname_bucket.."_RG"
	local reflush_key = stgname_bucket.."_REFLUSH"
	--local year,month,day,hour,min,sec = string.match(ngx.utctime(),"(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)")
	--local now_time = os.time({day=day, month=month, year=year, hour=hour, min=min, sec=sec})
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
		local res,err = red_handler:hmget(redis_key,"SecretKey","AccessKey","StorageDomain","RegionName")
		if not res and err then 
			return false, err 
		end
		
		local SecretKey = res[1]
		local AccessKey = res[2]
		local StorageDomain = res[3]
		local RegioName = res[4]
		
		if SecretKey == ngx.null or AccessKey == ngx.null 
		   or StorageDomain == ngx.null then 
			ngx.log(ngx.ERR,"Has no key stgname_bucket:",stgname_bucket)
			--return false, "There is no bucket"
			return false, nil
		else
			ngx.shared.storage_key_data:set(ak_key,AccessKey)
			ngx.shared.storage_key_data:set(sk_key,SecretKey)
			ngx.shared.storage_key_data:set(dm_key,StorageDomain)
			ngx.shared.storage_key_data:set(rg_key,RegioName)
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
		internal_reflush_SecretKey(csskey)
		ak = ngx.shared.storage_key_data:get(csskey.."_AK")
		sk = ngx.shared.storage_key_data:get(csskey.."_SK") 
		if not sk or not ak then 
			ngx.log(ngx.ERR,"has not ak or sk==========>",csskey)
			return false, csskey.."has not ak or sk "
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
--AWS S3签名版本4（目前国内只支持签名版本4）
--method 类型 GET 或者 PUT
--headers 里面一般就是time之类的
--objname 上传或者下载的文件名
--csskey 云存储名称+“_”+bucket名字的组合唯一标示（S3_test-pic-123）
例如:
--产生签名
req_headers["host"] = "s3-cn-nor-01.s3.cn-north-1.amazonaws.com.cn"
req_headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
req_headers["x-amz-date"] =  20180731T085319Z
请求:
header["Host"] = storage_domain
header["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
header["x-amz-date"] = 20180731T085319Z
header["Authorization"] = sign
--]]
function _M.make_signature_aws_v4(self, method,headers,objname,csskey)
	local signed_headers = "host;x-amz-content-sha256;x-amz-date"
	local sign_type = "AWS4-HMAC-SHA256"
	local storage_name = "s3"
	
	--获取秘钥和bucket所在区域信息
	local ak = ngx.shared.storage_key_data:get(csskey.."_AK")  --Access Key
	local sk = ngx.shared.storage_key_data:get(csskey.."_SK")  --Secrity Key
	local regioninfo = ngx.shared.storage_key_data:get(csskey.."_RG") --Bucket Region Info
	if not ak or not sk or not regioninfo then 
		internal_reflush_SecretKey(csskey)
		ak = ngx.shared.storage_key_data:get(csskey.."_AK")
		sk = ngx.shared.storage_key_data:get(csskey.."_SK") 
		regioninfo = ngx.shared.storage_key_data:get(csskey.."_RG")
		if not ak or not sk or not regioninfo then
			return false, csskey.."has not ak or sk"
		end 
	end
	
	--规定死三个参数原因是省去了字典排序的麻烦但同时也带来了灵活性的损失
	if type(headers) ~= "table" or 
	   not headers["host"] or 
	   not headers["x-amz-content-sha256"] or 
	   not headers["x-amz-date"]  then 
		return false, "invaild headers key"
	end 
		
	local canonical_headers_table = {}
	table.insert(canonical_headers_table,"host:"..headers["host"])
	table.insert(canonical_headers_table,"x-amz-content-sha256:"..headers["x-amz-content-sha256"])
	table.insert(canonical_headers_table,"x-amz-date:"..headers["x-amz-date"])
	local canonical_headers = table.concat(canonical_headers_table,'\n').."\n"
	
	local querystring = headers["queryparam"] or ""
	local signed_headers = "host;x-amz-content-sha256;x-amz-date"
	local hash_payload = "UNSIGNED-PAYLOAD"	
	
	--开始制作签名
--=====================================阶段一============================================
	local canonitcal_request = {}
	table.insert(canonitcal_request,method)
	table.insert(canonitcal_request,"/"..objname)
	table.insert(canonitcal_request,querystring)
	table.insert(canonitcal_request,canonical_headers)
	table.insert(canonitcal_request,signed_headers)
	table.insert(canonitcal_request,hash_payload)
	local canonical_request_str = table.concat(canonitcal_request,'\n')
	
--=============================================阶段二==========================
	local req_date = headers["x-amz-date"]
	local socp_date = string.sub(req_date,1,8)
	local scop = socp_date.."/"..regioninfo.."/s3/aws4_request"
	local canonical_request_h256 = sha256(canonical_request_str)
	local stringtosign = {}
	table.insert(stringtosign,sign_type)
	table.insert(stringtosign,req_date)
	table.insert(stringtosign,scop)
	table.insert(stringtosign,canonical_request_h256)
	local stringtosign_str = table.concat(stringtosign,'\n')
	
--=============================================阶段三==========================	
	local datekey_sec = "AWS4"..sk
	local _, hmac_datekey, hex_dateregionkey = hmac_sha256(datekey_sec,socp_date)
	local _, hmac_dateregionkey, hex_dateregionkey = hmac_sha256(hmac_datekey,regioninfo)
	local _, hmac_dateregionservicekey,hex_dateregionservicekey = hmac_sha256(hmac_dateregionkey,storage_name)
	local _, hmac_signingkey,hex_signingkey = hmac_sha256(hmac_dateregionservicekey,"aws4_request")
	local _, hmac_sign, hex_signingkey = hmac_sha256(hmac_signingkey,stringtosign_str)
	
	local Auth = "AWS4-HMAC-SHA256 ".."Credential="..ak.."/"..scop
	local signheaders = " SignedHeaders="..signed_headers
	local sign = " Signature="..hex_signingkey
	local request_auth = Auth..","..signheaders..","..sign
	
	return Auth, request_auth
end

--[[
这个接口用来查询优惠策略
运作原理: 1.通过设备序列号到cfg数据库查到设备的型号
		  2.通过设备型号查或者OEM信息查到相关优惠策略
--]]
function _M.check_preferential_strategy(self, serinum)
	--检查内存中是否有优惠政策
	--优惠政策在共享内存中存储的格式为
	--flushTime:bucket_info:expirst_day
	--刷新时间:视频bucket_info名称:视频存储天数
	--key为:DSCT_serinum 
	local key = "DSCT_" .. serinum
	local value = ngx.shared.dev_product_data:get(key)
	local now_time = ngx.time()
	if value then 
		local lasttime, bucketinfo, expirseday = string.match(value,"(.*):(.*):(.*)")
		if not lasttime or not bucketinfo or not expirseday then 
			ngx.shared.dev_product_data:delete(key)
			ngx.log(ngx.ERR,"invalid dsct key:",value)
			return true, nil
		end
		
		local flushtime = now_time - lasttime
		if (tonumber(expirseday) > 0 and flushtime <= 600 ) then  
			ngx.log(ngx.ERR,"[dsctdebug] support normal video seri:", serinum," buckinfo:",bucketinfo," expirday:",expirseday)
			return true, bucketinfo, expirseday
		end

		if (tonumber(expirseday) <= 0 and flushtime < 180) then 
			ngx.log(ngx.ERR,"[dsctdebug] not support normal video serinumber:",serinum)
			return true, nil
		end
	end 
	
	--连接数据库 查询设备版本信息以及固件信息
	local opts = {["redis_ip"]=redis_ip,["redis_port"]=redis_cfg_port,["timeout"]=3}
	local red_handler = redis_iresty:new(opts)
	if not red_handler then	
		return false,"redis_iresty:new failed"	
	end
	
	--从cfg里面获取productid 和 版本信息
	local res, err = red_handler:hmget(serinum,"ProductID", "OtherInfo")
	if err then 
		ngx.log(ngx.ERR,"[dsctdebug]get nothing or get redis err seri:",serinum," err:",err)
		return false, err
	end 
	
	if not res or res == ngx.null or err then 
		return true, nil
	end
	
	--剥离出oem信息
	local ProductID = res[1]
	local OtherInfo = res[2]
	local VersionID = nil
	if OtherInfo and OtherInfo ~= ngx.null then 
		local _,position =  string.find(OtherInfo,'%w+.%w+.%w+.%w',0)
		if position ~= nil then 
			local TempID = string.sub(OtherInfo, position, position+2)
			if TempID ~= '000' then
				VersionID = TempID
			end
		end
	end 
	
	--使用pipe技术 获取到设备必要信息
	red_handler:init_pipeline()
	local key_serinum = nil 
	local key_product = nil
	local key_oem = nil 
	
	local chek_list = {}
	--获取video bucket信息
	if serinum then 
		key_serinum = "CFG::PMS:" .. serinum 
		red_handler:hget(key_serinum, "CssVideoBucket")
		table.insert(chek_list,key_serinum)
	end 
	
	if VersionID then 
		key_oem = "CFG::PMS:" .. VersionID  
		red_handler:hget(key_oem, "CssVideoBucket")
		table.insert(chek_list,key_oem)
	end
	
	if ProductID and ProductID ~= ngx.null then 
		key_product = "CFG::PMS:" .. ProductID  
		red_handler:hget(key_product, "CssVideoBucket")
		table.insert(chek_list,key_product)
	end

	local res_status,err = red_handler:commit_pipeline()
	if err then 
		return false, err 
	end 
	
	if not res_status or next(res_status)==nil then
		local preferentia_value = now_time .. ":0:0" 
		ngx.shared.dev_product_data:set(key, preferentia_value)
		ngx.log(ngx.ERR,"commit err seri:",serinum," err:",err)
		return true, nil
	end 	
	
	for k, v in ipairs(chek_list) do 
		if res_status[k] ~= nil and (type(res_status[k]) == 'string') then
			local bucketinfo = res_status[k]
			local expirstday = 3
			local preferentia_value = now_time .. ":".. bucketinfo ..":" .. expirstday 
			ngx.shared.dev_product_data:set(key, preferentia_value)
			return true, bucketinfo, expirstday
		end
	end
	
	return true, nil
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
	local reflush_key = serinum.."_CSS_REFLUSH"
	
	local css_value = ngx.shared.css_share_data:get(css_key)
	local end_time =  ngx.shared.css_share_data:get(endtime_key)
	local reflush_time = ngx.shared.css_share_data:get(endtime_key)
	
	local now_time = ngx.time()
	if not css_value or not end_time or now_time - reflush_time > 180 then
		local opts = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
		local red_handler = redis_iresty:new(opts)
		if not red_handler then	
			ngx.log(ngx.ERR,"[CssCheck]:new redis faild redis ip:",redis_ip," redis port:",redis_port," SeriNum:",serinum,"type:",objtype)		
			return false,"redis_iresty:new failed"	
		end
		local storage_info_key = "<CLOUD_STORAGE>_"..serinum
		local res = nil
		local err = nil
		if objtype == "PIC" then 
			res,err = red_handler:hmget(storage_info_key,"PicStgEndTime","PicStgType","PicStgBucket")
			if err then 
				ngx.log(ngx.ERR,"[CssCheck]:hmget PIC failed redis ip:",redis_ip," redis port:",redis_port," SeriNum:",serinum," err:",err)		
				return false, err 
			end
		elseif objtype == "VIDEO" then 
			res,err = red_handler:hmget(storage_info_key,"VideoStgEndTime","VideoStgType","VideoStgBucket")
			if err then
				ngx.log(ngx.ERR,"[CssCheck]:hmget VIDEO failed redis ip:",redis_ip," redis port:",redis_port," SeriNum:",serinum," err:",err)		 
				return false, err 
			end
		end 
		
		if not res then 
			ngx.log(ngx.ERR,"[CssCheck]:Not Open redis ip:",redis_ip," redis port:",redis_port," SeriNum:",serinum," type:",objtype)		
			return true, nil --not buy storage 
		end 
		
		local endtime = res[1]
		local stgtype = res[2]
		local stgbucket = res[3]
		
		if stgbucket == ngx.null  or 
		   stgtype == ngx.null or 
		   endtime == ngx.null then 
			ngx.log(ngx.ERR,"[CssCheck]:Not Open @@@ redis ip:",redis_ip," redis port:",redis_port," SeriNum:",serinum," type:",objtype)			
			return true, nil  --not buy storage
		end 
		
		--local year,month,day,hour,min,sec = string.match(ngx.utctime(),"(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)")
		--local now_time = os.time({day=day, month=month, year=year, hour=hour, min=min, sec=sec})
		ngx.shared.css_share_data:set(css_key,stgtype.."_"..stgbucket)
		ngx.shared.css_share_data:set(endtime_key,endtime)
		ngx.shared.css_share_data:set(reflush_key,now_time)
		
		end_time = endtime
		css_value = stgtype.."_"..stgbucket
	end 

	local res1, res2 = internal_reflush_SecretKey(css_value)
	if not res1 and not res2 then 
		ngx.log(ngx.ERR,"[CssCheck]:Css Invalid Bucket:",analyse_stgbuck," SeriNum:",serinum)
		return true, nil
	end
	
	if end_time - 0 > 0 then
		local expairs_time = end_time - now_time
		if expairs_time < 0 then
			ngx.log(ngx.ERR,"[CssCheck]:Css expirse SeriNum:",serinum," endtime:",end_time)
			return true, nil 
		end 
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
	
	local analyse_endtime_key = serinum.."_AIENDTIME_"..endtype  --人形检测过期时间
	local analyse_type_key = serinum.."_AITYPE_"..endtype		 --人形检测的类型
	local analyse_stgbuck_key = serinum.."_AISTGBUC_"..endtype	 --人形检测的bucket信息
	local reflush_key = serinum.."_CSS_REFLUSH"
	
	local analyse_stgbuck = ngx.shared.css_share_data:get(analyse_stgbuck_key)
	local analyse_type = ngx.shared.css_share_data:get(analyse_type_key)
	local analyse_endtime = ngx.shared.css_share_data:get(analyse_endtime_key)
	local relush_time = ngx.shared.css_share_data:get(reflush_key)
	local now_time = ngx.time()
	
	if not analyse_endtime or not analyse_type or 
	   not analyse_stgbuck or (relush_time and now_time - relush_time > 600) then 
		--定期刷新 从人形检测数据库里面获取 bucket endtime 和 pictype信息
		local opt = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
		local red_handler = redis_iresty:new(opt)
		if not red_handler then
			ngx.log(ngx.ERR,"[QueryAI]:New redis failed rdis ip:",redis_ip," redis_port:",redis_port," ser:",serinum)
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
			ngx.log(ngx.ERR,"[QueryAI]:Get AI Info failed:",redis_ip," redis_port:",redis_port," err:",err," ser:",serinum)
			return false,err
		end
		
		if not res and not err then
			ngx.log(ngx.ERR,"[QueryAI]:AI Not Open SeriNumber:",serinum)
			return true,nil
		end	
		
		analyse_endtime = res[1]
		analyse_type = res[2]
		analyse_stgbuck = res[3]
		
		--检测是否开通云存储 如果开通了云存储 就是用云存储的bucket信息
		local ok, css_stgbucket = _M:check_css_flag(serinum,objtype)
		if ok and css_stgbucket then 
			ngx.log(ngx.ERR,"[QueryAI]:Use Css Bucket Info SeriNum:",serinum," bucket:",css_stgbucket)
			analyse_stgbuck = css_stgbucket
		end 
	
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
	
	--analyse_endtime == -1 表示永久有效
	if analyse_endtime ~= ngx.null and (analyse_endtime - 0) > 0 then
		local expairs_time = analyse_endtime - now_time
		if expairs_time < 0 then
			ngx.log(ngx.ERR,"[QueryAI]:AI expirse SeriNum:",serinum," EndTime:",analyse_endtime)
			return true, nil
		end
	end
	
	--检查一下storage bucket有没有对应的秘钥
	local res1, res2 = internal_reflush_SecretKey(analyse_stgbuck)
	if not res1 and not res2 then 
		ngx.log(ngx.ERR,"[QueryAI]:AI Invalid Bucket:",analyse_stgbuck," SeriNum:",serinum)
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
		if res1 and not res2 and objtype == "VIDEO" then 
			res1, res2 = _M:check_preferential_strategy(serinum)
		end
		return res1,res2
	else
		ngx.log(ngx.ERR,"[CheckAbality]type serinum:",serinum," singtype:",signtype) 
		return true,"Defalut"
	end
end 

--[[
查询云存储对象的回滚时间
--]]
function _M.get_storage_expirs_day(self,serinum,objtype)
	local redis_key = nil
	local default_expires_day = 3
	if objtype == "PIC" then
		redis_key = "PicStgTime"
		default_expires_day = 3
	elseif objtype == "VIDEO" then
		redis_key = "VideoStgTime"
		default_expires_day = 30
	else
		return false, "InValid Storage"
	end

	local opts = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
	local red_handler = redis_iresty:new(opts)
	if not red_handler then
		return true,default_expires_day
	end

	local storage_key = "<CLOUD_STORAGE>_"..serinum
	local res, err = red_handler:hget(storage_key,redis_key)
	if not res and err then
		return true,default_expires_day
	end

	if res == ngx.null and not err then
		--如果不存在默认回滚时间
		return true,default_expires_day
	end	
	
	return true,res
end

--[[
查询设备是否被订阅
--]]
function _M.get_subscribe_info(self,serinum)
	if not serinum then 
		return false
	end
	
	--连接数据库
	local opts = {["redis_ip"] = redis_ip,["redis_port"] = reids_pms_port,["timeout"]=3}
	local red_handler = redis_iresty:new(opts)
	if not red_handler then
		return true
	end
	
	--检查是否订阅
	local sub_key = "<APP_TOKEN>_"..serinum
	local res, err = red_handler:exists(sub_key)
	if err or not res then 
		ngx.log(ngx.ERR,"[checksubscribe]: check subscribe failed err:",err)
		return true 
	end
	
	--是否订阅
	if tonumber(res) == 1 then 
		return true 
	else 
		return false
	end
end

return _M
