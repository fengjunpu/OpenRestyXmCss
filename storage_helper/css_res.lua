local mysql_iresty = require("storage_helper.mysql_iresty")

local css_base_iresty = require("storage_helper.css_base")
local _M = {}      
_M._VERSION = '1.0'

local mysql_ip = ngx.shared.shared_data:get("xmcloud_css_mysql_ip")
local mysql_port = 8635 
local mysql_user = "root"
local mysql_pwd = "123456@XiongMai"
local mysql_db = "xmcloud_css"


function _M.handle_upload_pic_res(self,jreq)
	local flag = jreq["CssCenter"]["Body"]["UploadFlag"] or 0
	local upsize = jreq["CssCenter"]["Body"]["UploadSize"] or 0 
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	local alarmid = jreq["CssCenter"]["Body"]["AlarmId"]
	local picname = jreq["CssCenter"]["Body"]["PicName"] 
	local stgname = jreq["CssCenter"]["Body"]["StorageBucket"]
	
	local opts = { ["mysql_ip"] = mysql_ip,["mysql_port"] = mysql_port,
	   ["mysql_user"] = mysql_user,["mysql_pwd"] = mysql_pwd,
	   ["mysql_db"] = mysql_db,["timeout"] = 3
	}
	local handledb,err = mysql_iresty:new(opts)
	if not handledb then
		return false,err
	end
	
	if not picname then 
		picname = "Default"
		upsize = 0
	end	

	--local insert_sql = "update alarm_msg_tb set ObjName = \'"..picname.."\',StgFlag = \'"..stgname.."\',StorageFlag = "..flag..",PicSize = "..upsize.." where SeriNum = \""..serinum.."\" and AlarmId = \""..alarmid.."\"".." limit 1"
	local insert_sql = "update alarm_msg_tb set ObjName = \'"..picname.."\',StgFlag = \'"..stgname.."\',StorageFlag = "..flag..",PicSize = "..upsize.." where SeriNum = \""..serinum.."\" and AlarmId = \""..alarmid.."\"".." limit 1"
	local res,err = handledb:update_sql(insert_sql)
	if not res then
		return false,err
	end
	return true
end

function _M.handle_upload_video_res(self,jreq)
	--更新数据库
	local opts = { ["mysql_ip"] = mysql_ip,["mysql_port"] = mysql_port,
		       ["mysql_user"] = mysql_user,["mysql_pwd"] = mysql_pwd,
		       ["mysql_db"] = mysql_db,["timeout"] = 3
		     }
	local handledb,err = mysql_iresty:new(opts)
	if not handledb then
		return false,err
	end
	
	local serinum = jreq["CssCenter"]["Body"]["SerialNumber"]
	local starttime = jreq["CssCenter"]["Body"]["StartTime"]
	local stoptime = jreq["CssCenter"]["Body"]["StopTime"]
	local objsize = jreq["CssCenter"]["Body"]["ObjSize"]
	local indexname = jreq["CssCenter"]["Body"]["IndexName"]
	local picflag = jreq["CssCenter"]["Body"]["PicFlag"]	
	local channel = jreq["CssCenter"]["Body"]["Channel"]
	if not picflag then
		picflag = 0
	end
	
	local opts = { ["mysql_ip"] = mysql_ip,["mysql_port"] = mysql_port,
       		       ["mysql_user"] = mysql_user,["mysql_pwd"] = mysql_pwd,
					["mysql_db"] = mysql_db,["timeout"] = 3
     		    }
	local handledb,err = mysql_iresty:new(opts)
	if not handledb then
        	return false,err
	end
	
	--local year,month,day,hour,min,sec = string.match(ngx.utctime(),"(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)")
	--local utc_time = os.time({day=day, month=month, year=year, hour=hour, min=min, sec=sec})
	local utc_time = ngx.time()
	local ok, expirsday = css_base_iresty:get_storage_expirs_day(serinum,"VIDEO")
	if not ok or not expirsday then 
		expirsday = 3
	end 
	
	local expirstime = expirsday*24*3600 + utc_time
	local insert_sql = "insert into alarm_video_tb set SeriNum=\'"..serinum.."\',ChannelId="..channel..",ObjName=\'"..indexname.."\',StartTime=\'"..starttime.."\',StopTime=\'"..stoptime.."\',VideoSize="..objsize..",UtcTime="..utc_time..",PicFlag="..picflag..",ExpirseTime = "..expirstime
	if jreq["CssCenter"]["Body"]["StgFlag"] then 
		local StgName = jreq["CssCenter"]["Body"]["StgFlag"] 
		insert_sql = insert_sql..",StgFlag=\'"..StgName.."\'"
	else 
		local res,storage_bucket = css_base_iresty:check_abality(serinum,"VIDEO","CloudStorage")
		if res and storage_bucket then 
			insert_sql = insert_sql..",StgFlag=\'"..storage_bucket.."\'"
		end	
	end
	
	--开始时间向前移动10s  结束时间向后移动10s
	local start_sec = ngx.time(starttime) - 10
	local stop_sec = ngx.time(stoptime) + 5
	
	local update_sql = "update alarm_msg_tb set StorageFlag = 2 where SeriNum = \'"..serinum.."\' and UtcTime >= \'"..start_sec.."\' and UtcTime <= \'"..stop_sec.."\'"
	--ngx.log(ngx.ERR,"insert sql:",insert_sql)
	handledb:update_sql(insert_sql)
	handledb:update_sql(update_sql)
	return true
end

return _M
