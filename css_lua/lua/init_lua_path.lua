#!/usr/local/openresty/luajit/bin/luajit-2.1.0-alpha

--[�趨����·��]
--���Զ����·������package������·���С�Ҳ���Լӵ���������LUA_PATH��
--local p = "/root/OR/CSS/OpenRestyCssServer/"
local p = "/xm_workspace/xmcloud3.0/"
local m_package_path = package.path
package.path = string.format("%s;%s?.lua;%s?/init.lua",m_package_path, p, p)

