local mysql_iresty = require("storage_helper.mysql_iresty")
local css_base_iresty = require("storage_helper.css_base")
local wanip_iresty = require ("common_lua.wanip_iresty")
local css_sign_iresty = require("storage_helper.css_sign")

local mysql_ip = ngx.shared.shared_data:get("xmcloud_css_mysql_ip")
local mysql_port = 8635
local mysql_user = "root"
local mysql_pwd = "123456@XiongMai"
local mysql_db = "xmcloud_css"

--删除消息 和 视频的间隔
local delete_msg_interval = 600
local delete_vid_interval = 600

--开始删除
local function delete_expirse_from_mysql(deltype) 
	if not deltype or (deltype ~= "VIDEO" and deltype ~= "MSG") then 
		ngx.log(ngx.ERR,"[delete]:Invalid delete type: ",deltype)
		return false
	end 
	
	local opts = { ["mysql_ip"] = mysql_ip,["mysql_port"] = mysql_port,
           ["mysql_user"] = mysql_user,["mysql_pwd"] = mysql_pwd,
           ["mysql_db"] = mysql_db,["timeout"] = 3
        }
	local handledb,err = mysql_iresty:new(opts)
	if not handledb then
		ngx.log(ngx.ERR,"[delete]: new msql failed err:",err)
		return false,err
	end

	local sql = nil 
	local nowTime = ngx.time()
	if deltype == "MSG" then 
		sql = "delete from alarm_msg_tb where ExpirseTime < "..nowTime.." limit 10000"
	elseif deltype == "VIDEO" then 
		sql = "delete from alarm_video_tb where ExpirseTime < "..nowTime.." limit 10000"
	end
	
	ngx.log(ngx.ERR,"[delete]: sql:",sql)
	local res,err = handledb:update_sql(sql)
	if not res then
		ngx.log(ngx.ERR,"[delete]: delete msql failed err:",err," sql:",sql)
		return false,err
	end
	
	return true
end 

--删除MSG
local msg_handler = nil
msg_handler = function ()
	--检查mysql_ip是否合法
	if mysql_ip ~= nil then
		local ip1,ip2,ip3,ip4 = string.match(mysql_ip,"(%w+).(%w+).(%w+).(%w+)")
		if not ip1 or not ip2 or not ip3 or not ip4 then 
			local ok, err = ngx.timer.at(1, msg_handler)
			if not ok then
				ngx.log(ngx.ERR, "failed to startup msg get ip heartbeat timer...", err)
			end
			ngx.log(ngx.ERR,"get msg invalid mysql ip")
			delete_msg_interval = 10
		else
			--开始删除过期消息
			local res, msg = delete_expirse_from_mysql("MSG") 
			if not res then 
				delete_msg_interval = 10
			else 
				delete_msg_interval = 180
			end 
		end 
	end
	--绑定定时器
	local ok, err = ngx.timer.at(delete_msg_interval, msg_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup msg heartbeat timer...", err)
	end
	
	return true
end

--删除VIDEO
local vid_handler = nil
vid_handler = function ()
	--检查mysql_ip是否合法
	if mysql_ip ~= nil then
		local ip1,ip2,ip3,ip4 = string.match(mysql_ip,"(%w+).(%w+).(%w+).(%w+)")
		if not ip1 or not ip2 or not ip3 or not ip4 then 
			local ok, err = ngx.timer.at(1, vid_handler)
			if not ok then
				ngx.log(ngx.ERR, "failed to startup vid get ip heartbeat timer...", err)
			end
			ngx.log(ngx.ERR,"get vid invalid mysql ip")
			delete_vid_interval = 10
		else
			--开始删除
			local res, msg = delete_expirse_from_mysql("VIDEO") 
			if not res then 
				delete_vid_interval = 10
			else 
				delete_vid_interval = 180
			end 
		end 
	end 
	--绑定定时器
	local ok, err = ngx.timer.at(delete_vid_interval, vid_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup vid heartbeat timer...", err)
	end
	
	return true
end

local update_mysqladdr_handler = nil
update_mysqladdr_handler = function() 
	local css_mysql_domain = ngx.shared.shared_data:get("xmcloud_css_mysql_domain")
	if css_mysql_domain then 
		local css_mysql_ip, _ = wanip_iresty.getdomainip(css_mysql_domain)
		if css_mysql_ip ~= nil then 
			ngx.shared.shared_data:set("xmcloud_css_mysql_ip", css_mysql_ip)	
			css_sign_iresty:set_mysql_addr(css_mysql_ip)
		end
	end
	local ok, err = ngx.timer.at(300, update_mysqladdr_handler) 
	if not ok and err then 
		ngx.log(ngx.ERR, "failed to start update mysql addr timer ...", err)
	end
	return true
end

--程序入口
--local pid = ngx.worker.pid()
local start_flag = "delete_expirse_init_flag"
local ok = ngx.shared.shared_data:add(start_flag,1)
if ok then
	--MSG
	local ok, err = ngx.timer.at(180, msg_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup 111 msg heartbeat timer...", err)
	end
	
	--VIDEO
	local ok, err = ngx.timer.at(180, vid_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup 111 video heartbeat timer...", err)
	end
	ngx.log(ngx.ERR,"start heartbeat helper ...")
	
	--MySQLAddr
	local ok, err = ngx.timer.at(0.1, update_mysqladdr_handler)
	if not ok then
		ngx.log(ngx.ERR, "failed to startup update_mysqladdr_handler...", err)
	end
	ngx.log(ngx.ERR,"start update_mysqladdr_handler ...")
end
