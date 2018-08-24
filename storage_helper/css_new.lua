local redis_iresty = require("common_lua.redis_iresty")
local cjson = require("cjson.safe")
local _M = {}      
_M._VERSION = '1.0'

local redis_ip = ngx.shared.shared_data:get("xmcloud_css_redis_ip")
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

        if SecretKey == ngx.null or 
		   AccessKey == ngx.null or 
		   StorageDomain == ngx.null then
            ngx.log(ngx.ERR,"Has no key stgname_bucket:",stgname_bucket)
            return false, nil
        else
            ngx.shared.storage_key_data:set(ak_key,AccessKey)
            ngx.shared.storage_key_data:set(sk_key,SecretKey)
            ngx.shared.storage_key_data:set(dm_key,StorageDomain)
            ngx.shared.storage_key_data:set(reflush_key,now_time)
            return true, "sucess ok"
        end
    end

    return true, "sucess ok" 
end

function _M.handle_new_css(self,jreq)
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	local auth = jreq["CssCenter"]["Body"]["AuthCode"]
	local storsize = jreq["CssCenter"]["Body"]["SpaceSize"]
	local stortime = jreq["CssCenter"]["Body"]["StorageTime"]
	local endtime = jreq["CssCenter"]["Body"]["EndTime"]
	local storbytes = (10*1024)*1024
	local stortype = jreq["CssCenter"]["Body"]["StorageInfo"]["Type"]
	local videobucket = jreq["CssCenter"]["Body"]["StorageInfo"]["VideoBucket"]
	local picbucket = jreq["CssCenter"]["Body"]["StorageInfo"]["PicBucket"]
	if (not videobucket and not picbucket)
	   or not stortime or type(stortime) ~= 'number' then
		return false, "invalid args"
	end
				
	if videobucket then
		local video_csskey = stortype.."_"..videobucket.."_SK"
		local video_value = ngx.shared.storage_key_data:get(video_csskey)
		if not video_value then
			return false, "Video Bucket has No Key"	
		end
	end

	if picbucket then
		local pic_csskey = stortype.."_"..picbucket.."_SK"
		local pic_value = ngx.shared.storage_key_data:get(pic_csskey)
		if not pic_value then
			return false, "Pic Bucket has No Key"
		end
	end
	
	local opts = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
	local red_handler = redis_iresty:new(opts)
	if not red_handler then			
		return false,"redis_iresty:new failed"	
	end
	
	local redis_key = "<CLOUD_STORAGE>_"..serinum
	red_handler:init_pipeline()
	if videobucket then
		red_handler:hmset(redis_key,"VideoStgTime",stortime,"VideoStgEndTime",endtime,
						 "VideoStgSize",storbytes,"VideoStgType",stortype,"VideoStgBucket",videobucket)
	end 
	
	if 	picbucket then
		red_handler:hmset(redis_key,"PicStgTime",stortime,"PicStgEndTime",endtime,
						 "PicStgSize",storbytes,"PicStgType",stortype,"PicStgBucket",picbucket)
	end 
	
	--把设备序列号写入redis队列以便同步到其他数据域
	local list_key = "<SYNC_CSTORAGE>_FLAG"
	red_handler:lpush(list_key,serinum)
	
	local res,err = red_handler:commit_pipeline()
	if not res and err then 
		ngx.log(ngx.ERR,"[NewCss] commit pipeline failed err:",err," SeriNum:",serinum," VideoBucket:",videobucket," PicBucket:",picbucket)
		return false,err
	end 
	
	ngx.log(ngx.ERR,"[NewCss]Add Sucess SeriNum:",serinum," VideoBucket:",videobucket,
				" PicBucket:",picbucket," StoreTime:",stortime," EndTime:",endtime)
			
	--update share data
	local timekey = serinum.."_ENDTIME"
	if picbucket then
		local pickey = serinum.."_PIC"
		local timekey = serinum.."_PIC_ENDTIME"
		local picvalue = stortype.."_"..picbucket
		ngx.shared.css_share_data:set(pickey,picvalue)
		ngx.shared.css_share_data:set(timekey,timevalue)
	end
	
	if videobucket then
		local videokey = serinum.."_VIDEO"
		local timekey = serinum.."_VIDEO_ENDTIME"
		local videovalue = stortype.."_"..videobucket
		ngx.shared.css_share_data:set(videokey,videovalue)
		ngx.shared.css_share_data:set(timekey,timevalue)
	end
		
	local resp_str = {}
	resp_str["CssCenter"] = {}
	resp_str["CssCenter"]["Header"] = {}
	resp_str["CssCenter"]["Header"]["ErrorString"] = "Success OK"
	resp_str["CssCenter"]["Header"]["Version"] = "1.0"
	resp_str["CssCenter"]["Header"]["MessageType"] = "MSG_CSS_NEW_RSP"
	resp_str["CssCenter"]["Header"]["ErrorNum"] = "200"
	local resp_str = cjson.encode(resp_str)
	ngx.header.content_length = string.len(resp_str)
	ngx.say(resp_str)
	return true
end

function _M.handle_new_analyespic(self,jreq)
	
	if type(jreq["CssCenter"]["Body"]["Enable"]) == "boolean" and 
		not jreq["CssCenter"]["Body"]["Enable"] and  
		jreq["CssCenter"]["Body"]["SerialNumber"] then 
		
		--关闭人形检测能力级
		local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
		local opt = {["redis_ip"] = redis_ip,["redis_port"] = redis_port,["timeout"] = 3}
		local red_handler = redis_iresty:new(opt)
		if not red_handler then
			return false,"redis_iresty:new failed"
		end
		
		local redis_key = "<AI_ANALYSIS>_"..serinum
		local ok, err = red_handler:hmset(redis_key,"Enable",0)
		if not ok then
			return false,err
		else
			local analyse_enable_key = serinum.."_AIENABLE_FLAG"
			ngx.shared.css_share_data:set(analyse_enable_key,0)
		end	
	elseif  not jreq["CssCenter"]["Body"]["StorageInfo"] or 
			not jreq["CssCenter"]["Body"]["EndTime"]  then
				return false, "invalid request"
	else
		local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
		local auth = jreq["CssCenter"]["Body"]["AuthCode"]
		local analyse_endtime = jreq["CssCenter"]["Body"]["EndTime"]
		local analyse_type = jreq["CssCenter"]["Body"]["DectionInfo"]["Type"]
		local storage_type = jreq["CssCenter"]["Body"]["StorageInfo"]["Type"]
		local storage_vbucket = jreq["CssCenter"]["Body"]["StorageInfo"]["VideoBucket"]
		local storage_pbucket = jreq["CssCenter"]["Body"]["StorageInfo"]["PicBucket"]
		
		if not analyse_type or not analyse_endtime or not storage_type then	
			return false, "invalid request"
		end
		
		if not storage_vbucket and not storage_pbucket then 
			return false, "invalid request no bucket"
		end
		
		--开通图片的AI检测
		if storage_pbucket then
			local stgname_bucket = storage_type.."_"..storage_pbucket
			local ok,err = internal_reflush_SecretKey(stgname_bucket)
			if not ok and not err then 
				ngx.log(ngx.ERR,"[NewAI]Add Pic Ai failed SeriNum:",serinum," Invalid bucket:",stgname_bucket)
				return false, "invalid pic storage info"
			end 
		end 
		
		--开通视频的AI检测	
		if storage_vbucket then
			local stgname_bucket = storage_type.."_"..storage_vbucket
			local ok,err = internal_reflush_SecretKey(stgname_bucket)
			if not ok and not err then 
				ngx.log(ngx.ERR,"[NewAI]Add Video Ai failed SeriNum:",serinum," Invalid bucket:",stgname_bucket)
				return false, "invalid video storage info"
			end 
		end
		
		--写入redis中
		local opt = {["redis_ip"]=redis_ip,["redis_port"]=redis_port,["timeout"]=3}
		local red_handler = redis_iresty:new(opt)
		if not red_handler then
			return false,"redis_iresty:new failed"
		end
		local redis_key = "<AI_ANALYSIS>_"..serinum
		red_handler:init_pipeline()
		if storage_pbucket then
			local storage_key = storage_type.."_"..storage_pbucket 
			red_handler:hmset(redis_key,"AnalysisPicTime",analyse_endtime,"PicStgBuck",storage_key,"AnalysisPicType",analyse_type)
		elseif storage_vbucket then 
			local storage_key = storage_type.."_"..storage_vbucket
			red_handler:hmset(redis_key,"AnalysisVidTime",analyse_endtime,"VidStgBuck",storage_key,"AnalysisVidType",analyse_type)
		end 

		red_handler:hmset(redis_key,"Enable",1) --人形检测开关置为1
		
		--写入list以便将人形检测能力同步到其他数据域
		local list_key = "<SYNC_ANALYSIS>_FLAG"
		red_handler:lpush(list_key,serinum)
		
		local res,err = red_handler:commit_pipeline()
		if not res and err then 
			ngx.log(ngx.ERR,"[NewAI]Add failed err:",err," SeriNum:",serinum," PicStgBuck:",storage_key,
					" AnalysisPicType:",analyse_type," AnalysisPicTime:",analyse_endtime)
			return false,err
		end 
					
		--更新图片检测内存
		if storage_pbucket then
			local analyse_endtime_key = serinum.."_AIENDTIME_PIC"
			local analyse_type_key = serinum.."_AITYPE_PIC"
			local analyse_stgbuc_key = serinum.."_AISTGBUC_PIC"
			ngx.shared.css_share_data:set(analyse_endtime_key,analyse_endtime)
			ngx.shared.css_share_data:set(analyse_type_key,analyse_type)
			ngx.shared.css_share_data:set(analyse_stgbuc_key,storage_type.."_"..storage_pbucket)
		end
		
		--更新视频检测内存
		if analyse_video_sdomain then
			local analyse_endtime_key = serinum.."_AIENDTIME_VID"
			local analyse_type_key = serinum.."_AITYPE_VID"
			local analyse_stgbuc_key = serinum.."_AISTGBUC_VID"
			ngx.shared.css_share_data:set(analyse_endtime_key,analyse_endtime)
			ngx.shared.css_share_data:set(analyse_type_key,analyse_type)
			ngx.shared.css_share_data:set(analyse_stgbuc_key,storage_type.."_"..storage_vbucket)
			
			--开关置为1
			local analyse_enable_key = serinum.."_AIENABLE_FLAG"
			ngx.shared.css_share_data:set(analyse_enable_key,1)
			ngx.log(ngx.ERR,"[NewAI]Add Sucess SeriNum:",serinum," PicStgBuck:",storage_key,
					" AnalysisPicType:",analyse_type," AnalysisPicTime:",analyse_endtime)
		end
	end
	
	local resp_str = {}
	resp_str["CssCenter"] = {}
	resp_str["CssCenter"]["Header"] = {}
	resp_str["CssCenter"]["Header"]["ErrorString"] = "Success OK"
	resp_str["CssCenter"]["Header"]["Version"] = "1.0"
	resp_str["CssCenter"]["Header"]["MessageType"] = "MSG_NEW_AI_ANALYSIS_RSP"
	resp_str["CssCenter"]["Header"]["ErrorNum"] = "200"
	local resp_str = cjson.encode(resp_str)
	ngx.header.content_length = string.len(resp_str)
	ngx.say(resp_str)
	return true
end

function _M.handle_del_css(self,jreq)
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	local storage_type = jreq["CssCenter"]["Body"]["StorageType"]
	local pickey = serinum.."_PIC"
	local videokey = serinum.."_VIDEO"
	if not storage_type then
		local videotime_key = serinum.."_VIDEO_ENDTIME"
		local pictime_key = serinum.."_PIC_ENDTIME"
		ngx.shared.css_share_data:delete(pickey)
		ngx.shared.css_share_data:delete(videokey)
		ngx.shared.css_share_data:delete(pictime_key)
		ngx.shared.css_share_data:delete(videotime_key)
	elseif storage_type == "PIC" then
		local pictime_key = serinum.."_PIC_ENDTIME"
		ngx.shared.css_share_data:delete(pickey)
		ngx.shared.css_share_data:delete(pictime_key)
	elseif storage_type == "VIDEO" then
		local videotime_key = serinum.."_VIDEO_ENDTIME"
		ngx.shared.css_share_data:delete(videokey)		
		ngx.shared.css_share_data:delete(videotime_key)
	end
	
	--删除云存储信息
	local opt = {["redis_ip"]=redis_port,["redis_port"]=redis_port,["timeout"]=3}
	local red_handler = redis_iresty:new(opt)
	if not red_handler then
		return false,"redis_iresty:new failed"
	end

	local redis_key = "<CLOUD_STORAGE>_"..serinum
	red_handler:init_pipeline()
	if storage_type ~= "PIC" then 
		red_handler:hdel(redis_key,"VideoStgTime")
		red_handler:hdel(redis_key,"VideoStgEndTime")
		red_handler:hdel(redis_key,"VideoStgSize")
		red_handler:hdel(redis_key,"VideoStgType")
		red_handler:hdel(redis_key,"VideoStgBucket")
	elseif 
		storage_type ~= "VIDEO" then 
		red_handler:hdel(redis_key,"PicStgTime")
		red_handler:hdel(redis_key,"PicStgEndTime")
		red_handler:hdel(redis_key,"PicStgSize")
		red_handler:hdel(redis_key,"PicStgType")
		red_handler:hdel(redis_key,"PicStgBucket")
	end 
	local res,err = red_handler:commit_pipeline()
	if not res and err then 
		return false,err
	end 
end

function _M.handle_add_bucket(self,jreq)
	--判断有效性
	if not jreq["CssCenter"]["Body"]["StorageName"] or not 
		   jreq["CssCenter"]["Body"]["DomainName"] or not 
		   jreq["CssCenter"]["Body"]["BucketName"] or not 
		   jreq["CssCenter"]["Body"]["SecretKey"] or not 
		   jreq["CssCenter"]["Body"]["AccessKey"] then
		return false,"Invaild Request"
	end
	
	local StorageName = jreq["CssCenter"]["Body"]["StorageName"]
	local DomainName = jreq["CssCenter"]["Body"]["DomainName"] 
	local BucketName = jreq["CssCenter"]["Body"]["BucketName"] 
	local SecretKey = jreq["CssCenter"]["Body"]["SecretKey"] 
	local AccessKey = jreq["CssCenter"]["Body"]["AccessKey"]
	local RegionName = jreq["CssCenter"]["Body"]["RegionName"]
	
--	local year,month,day,hour,min,sec = string.match(ngx.utctime(),"(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)")
--	local RefulushTime = os.time({day=day, month=month, year=year, hour=hour, min=min, sec=sec})
	local RefulushTime = ngx.time()	
	--刷新内存
	local ak_key =  StorageName.."_"..BucketName.."_AK"
	local sk_key =  StorageName.."_"..BucketName.."_SK"
	local dm_key =  StorageName.."_"..BucketName.."_DM"
	local rg_key = 	StorageName.."_"..BucketName.."_RG"
	local reflush_key = StorageName.."_"..BucketName.."_REFLUSH"
	
	if ngx.shared.storage_key_data:get(ak_key) ~= nil or
	   ngx.shared.storage_key_data:get(sk_key) ~= nil or
	   ngx.shared.storage_key_data:get(dm_key) ~= nil 
	then
		return false,"StorageName Or BucketName Conflict"
	end	
	
	local redis_key = "<StorageKey>_"..StorageName.."_"..BucketName
	local opt = {["redis_ip"]=redis_ip,["redis_port"]=reids_bucket_port,["timeout"]=3}
	local red_handler = redis_iresty:new(opt)
	if not red_handler then
		return false,"redis_iresty:new failed"
    end

	local res, err = red_handler:hmget(redis_key,"BucketName","StorageName")
	if not res and err then 
		ngx.log(ngx.ERR,"[AddBucket]Check Conflict redis failed err:",err)
		return false, err
	end	

	if type(res) == "table" and 
		res[1] ~= ngx.null  and 
		res[2] ~= ngx.null then 	
		return false, "StorageName Or BucketName Conflict Please Do Not Add"
	end

	if ngx.shared.storage_key_data:get(ak_key) ~= AccessKey or
	   ngx.shared.storage_key_data:get(sk_key) ~= SecretKey or 
	   ngx.shared.storage_key_data:get(dm_key) ~= DomainName 
	then
		if not RegionName then 
			RegionName = "DefaultRegion"
		end 
		ngx.shared.storage_key_data:set(ak_key,AccessKey)
		ngx.shared.storage_key_data:set(sk_key,SecretKey)
		ngx.shared.storage_key_data:set(dm_key,DomainName)
		ngx.shared.storage_key_data:set(rg_key,RegionName)
		ngx.shared.storage_key_data:set(reflush_key,RefulushTime)
		
		red_handler:init_pipeline()
		red_handler:hmset(redis_key,"BucketName",BucketName,"StorageDomain",DomainName,
											"SecretKey",SecretKey,"AccessKey",AccessKey,
											"StorageName",StorageName,"RegionName",RegionName)

		--将bucket信息写入队列方便后续同步到其他数据域
		local list_key = "<SYNC_BUCKETINFO>_FLAG"
		red_handler:lpush(list_key,StorageName.."_"..BucketName)
		
		local res,err = red_handler:commit_pipeline()
		if not res and err then 
			ngx.log(ngx.ERR,"[AddBucket]Add Bucket Failed err:",err," BucketName:",BucketName," StorageDomain:",
						DomainName," StorageName:",StorageName)
			return false, err
		end 
	end 
	
	ngx.log(ngx.ERR,"[AddBucket]Add Bucket Sucess BucketName:",BucketName," StorageDomain:",DomainName,
				" StorageName:",StorageName)
	local jrsp = {}
	jrsp["CssCenter"] = {}
	jrsp["CssCenter"]["Header"] = {}
	jrsp["CssCenter"]["Header"]["MessageType"] = "MSG_ADD_BUCKET_RSP"
	jrsp["CssCenter"]["Header"]["ErrorNum"] = 200
	jrsp["CssCenter"]["Header"]["ErrorString"] = "Sucess OK"
	jrsp["CssCenter"]["Header"]["Version"] = "v1.0"
	local resp_str = cjson.encode(jrsp)
	ngx.header.content_length = string.len(resp_str)
	ngx.say(resp_str)
	return true	
end 

return _M 
