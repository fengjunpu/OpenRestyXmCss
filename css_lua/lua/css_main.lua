#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

-----------------代码规范说明-----------------
--[[
所有程序基本框架都是类似的
说明1>对错误应答的处理
	在processmsg函数中会调用各个处理分支，如果分支函数成功则其内部返回http应答
	如果返回失败，由processmsg判断返回值统一应答
说明2>对鉴权等常规共性的动作做好可以统一到脚本中去执行
说明3>HTTP应答头统一都是OK，这样便于查找是应用错误，还是系统错误
]]


--[设定搜索路径]
--将自定义包路径加入package的搜索路径中。也可以加到环境变量LUA_PATH中
--放到init_lus_path.lua中，不然的话，每一个请求处理的时候都会对全局变量
--package.path进行设置，导致

--[包含公共的模块]
local cjson = require("cjson.safe")
local wanip_iresty = require ("common_lua.wanip_iresty")
local authcode_iresty = require("common_lua.authcode_iresty")
local css_base_iresty = require("storage_helper.css_base")
local css_sign_iresty = require("storage_helper.css_sign")
local css_query_iresty = require("storage_helper.css_query")
local css_new_iresty = require("storage_helper.css_new")
local css_res_iresty = require("storage_helper.css_res")

--[基本变量参数]
--Redis服务器配置(会根据端口来进行修改)
local service_namespace = "css"			--->默认


--对输入的参数做有效性检查，返回解码后的消息体对象json对象
function get_request_param()
	local req_body, err = cjson.decode(ngx.var.request_body)
	if not req_body then
		ngx.log(ngx.ERR, "get_request_param:req body is not a json", ngx.var.request_body)
		return nil, "req body is not a json"
    end
    if not req_body["CssCenter"]
        or not req_body["CssCenter"]["Header"]
        or not req_body["CssCenter"]["Header"]["Version"]
        or not req_body["CssCenter"]["Header"]["MessageType"]
        or not req_body["CssCenter"]["Body"]
        or type(req_body["CssCenter"]["Header"]["Version"]) ~= "string"
        or type(req_body["CssCenter"]["Header"]["MessageType"]) ~= "string"
	then
        ngx.log(ngx.ERR, "invalid args")
        return nil, "invalid protocol format args"
    end
    return req_body, "success"
end

function send_resp_string(rspstatus,message_type,error_string)
	if not message_type or type(message_type) ~= "string" then
		ngx.log(ngx.ERR, "send_resp_string:type(message_type) ~= string", type(message_type))
		ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
	end
	if not error_string or type(error_string) ~= "string" then
		ngx.log(ngx.ERR, "send_resp_string:type(error_string) ~= string", type(error_string))
		ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
	end
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

--消息处理函数入库
function process_msg()
	--获取请求对象
	local jreq, err = get_request_param()
	if not jreq then
		send_resp_string(ngx.HTTP_BAD_REQUEST,"any",err);
	    return
	end

	--分命令处理
	if(jreq["CssCenter"]["Header"]["MessageType"] == "MSG_UPLOAD_SIGN_REQ") then
		local ok, err = css_sign_iresty:handle_upload_sign(jreq);
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_PIC_UPLOAD_SIGN_RSP",err);
		end
	elseif(jreq["CssCenter"]["Header"]["MessageType"] == "MSG_MULTIUPLOAD_SIGN_REQ") then
		local ok, err = css_sign_iresty:handle_multi_ts_sign(jreq);
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_MULTIUPLOAD_SIGN_RSP",err);
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_DOWNLOAD_SIGN_REQ") then
		local ok,err = css_sign_iresty:handle_download_sign(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_DOWNLOAD_SIGN_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_CSS_NEW_REQ") then
		local ok,err = css_new_iresty:handle_new_css(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_CSS_NEW_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_NEW_AI_ANALYSIS_REQ") then
		local ok,err = css_new_iresty:handle_new_analyespic(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_NEW_AI_ANALYSIS_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_CSS_SWITCH_REQ") then
		local ok,err = css_new_iresty:handle_css_switch(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_CSS_SWITCH_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_CSS_DELETE_REQ") then
		local ok,err = css_new_iresty:handle_del_css(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_CSS_DELETE_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_ADD_BUCKET_REQ") then
		local ok,err = css_new_iresty:handle_add_bucket(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_ADD_BUCKET_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_CSS_QUERY_REQ") then
		local ok,err = css_query_iresty:handle_query_css(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_CSS_QUERY_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_AI_ANALYSIS_QUERY_REQ") then
		local ok,err = css_query_iresty:handle_query_analysis(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_AI_ANALYSIS_QUERY_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_CSS_DEV_QUERY_REQ") then
		local ok,err = css_query_iresty:handle_dev_query_css(jreq)
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_CSS_DEV_QUERY_RSP",err)
		end
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_PIC_UPLOAD_RES_REQ") then
		local ok, err = css_res_iresty:handle_upload_pic_res(jreq);
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_PIC_UPLOAD_RES_RSP",err);
		end	
	elseif (jreq["CssCenter"]["Header"]["MessageType"] == "MSG_VIDEO_UPLOAD_RES_REQ") then
		local ok, err = css_res_iresty:handle_upload_video_res(jreq);
		if not ok then
			send_resp_string(ngx.HTTP_BAD_REQUEST,"MSG_VIDEO_UPLOAD_RES_RSP",err);
		end	
	else
		ngx.log(ngx.ERR, "invalid MessageType",jreq["CssCenter"]["Header"]["MessageType"])
		send_resp_string(ngx.HTTP_BAD_REQUEST,"any","Invalid MessageType");
	end
	return
end

--加载对应的名字空间的IP地址的配置
local function load_css_ip_addr()	
--加载redis的ip 和 端口
	local css_redis_ip = ngx.shared.shared_data:get("xmcloud_css_redis_ip")
	local css_mysql_domain = ngx.shared.shared_data:get("xmcloud_css_mysql_domain")
	if not css_redis_ip then 
		css_redis_ip = os.getenv("CssRedisIp")
		if css_redis_ip then 
			ngx.shared.shared_data:set("xmcloud_css_redis_ip",css_redis_ip)
		else 
			return false 
		end
	end

--<加载mysql的ip 和 端口>	
	if not css_mysql_domain then 
		css_mysql_domain = os.getenv("CssMysqlIp")
		if not css_mysql_domain then 
			ngx.shared.shared_data:set("xmcloud_css_mysql_domain", css_mysql_domain)
			local css_mysql_ip, _ = wanip_iresty.getdomainip(css_mysql_domain) 
			if css_mysql_ip == nil then 
				return false
			end 
			ngx.shared.shared_data:set("xmcloud_css_mysql_ip", css_mysql_ip)
		else
			return false
		end
	end 
	return true
end

--程序入口
if(ngx.var.server_port == "6614" or  ngx.var.server_port == "6615") then
	service_namespace = "css"
	local ok = load_css_ip_addr()
	if not ok then 
		return false
	end
else
	ngx.log(ngx.ERR,"invlaid ngx.var.server_port",ngx.var.server_port)
	return false
end

process_msg()
