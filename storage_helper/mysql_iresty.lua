-- file name: resty/redis_iresty.lua
--redis接口的二次封装

local mysql_iresty = require "resty.mysql"

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

--_M 理解为一个table,预分配_M 的长度信息
local _M = new_tab(0, 155)
_M._VERSION = '0.01'

local mt = { __index = _M }

-- change connect address as you need
function _M.connect_mod( self, db )	
		db:set_timeout(self.timeout)
		local props = {
			host = self.mysql_ip,
			port = self.mysql_port,
			database = self.mysql_db,
			user = self.mysql_user,
			password = self.mysql_pwd
		}
		return db:connect(props)
end

function _M.set_keepalive_mod(self,db )
    return db:set_keepalive(60000, 1000)
end

--[[
db:update_sql(alarm_msg_tb,values,condition)
db:insert_sql(alarm_msg_tb,values,condition)
db:select_sql(alarm_msg_tb,values,condition)
db:delete_sql(alarm_msg_tb,values,condition)
--]]

function _M.insert_sql(self,sqlcmd)
	
	local db,err = mysql_iresty:new()
	if not db then
		return nil,err
	end
	
	local ok, err = self:connect_mod(db)
    if not ok or err then
        return nil, err
    end
	
	local res,err,errno,sqlstate = db:query(sqlcmd)
	if not res then
		ngx.log(ngx.ERR,"insert mysql failed err:",err," errno:",errno," sqlstate:",sqlstate)
		db:close()
		return false,err
	end
	
	self:set_keepalive_mod(db)
	return true,err
	
end

function _M.update_sql(self,sqlcmd)
	local db,err = mysql_iresty:new()
	if not db then
		return nil,err
	end
	
	local ok, err = self:connect_mod(db)
    if not ok or err then
        return nil, err
    end
	
	local res,err,errno,sqlstate = db:query(sqlcmd)
	if not res then
		ngx.log(ngx.ERR,"insert mysql failed err:",err," errno:",errno," sqlstate:",sqlstate)
		db:close()
		return false,err
	end
	self:set_keepalive_mod(db)
	return res
	
end

function _M.new(self,opts)
    --设定连接超时时间的选项
    opts = opts or {}
    local timeout = (opts.timeout and opts.timeout * 1000) or 1000
    local mysql_ip = opts.mysql_ip or "127.0.0.1"
    local mysql_port = opts.mysql_port or 3306
	local mysql_user = opts.mysql_user or 'root'
	local mysql_pwd = opts.mysql_pwd or 'root'
	local mysql_db = opts.mysql_db or 'css'

    return setmetatable({
            timeout = timeout,
			mysql_ip = mysql_ip,
			mysql_port = mysql_port,
			mysql_user = mysql_user,
			mysql_pwd = mysql_pwd,
			mysql_db = mysql_db,
            _reqs = nil }, mt)
end


return _M
