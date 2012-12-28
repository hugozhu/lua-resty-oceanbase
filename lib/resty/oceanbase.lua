-- Copyright (C) 2012 Hugo Zhu (hugozhu)

local bit = require "bit"
local sub = string.sub
local tcp = ngx.socket.tcp
local insert = table.insert
local strlen = string.len
local strbyte = string.byte
local strchar = string.char
local strfind = string.find
local strrep = string.rep
local null = ngx.null
local band = bit.band
local bxor = bit.bxor
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local tohex = bit.tohex
local concat = table.concat
local unpack = unpack
local setmetatable = setmetatable
local error = error
local tonumber = tonumber
local print = ngx.say
local pairs = pairs

module(...)

_VERSION = '0.1'

-- constants

local mt = { __index = _M }

local function _get_int16(data,i)
    local a, b = strbyte(data, i, i + 1)
    return bor(lshift(a,8), b), i + 2
end

local function _get_int32(data,i)
    local a, b, c, d = strbyte(data, i, i + 3)    
    return bor(lshift(a,24), lshift(b,16), lshift(c,8), d), i+4
end

local function _set_int16(n)
    return strchar(band(rshift(n, 8), 0xff), band(n, 0xff))
end

local function _set_int32(n)
    return strchar(band(rshift(n, 24), 0xff), 
        band(rshift(n, 16), 0xff), 
        band(rshift(n, 8), 0xff),
        band(n, 0xff)
        )
end

local function _get_cstring(data, i)
    local last = strfind(data, "\0", i, true)
    if not last then
        return nil, nil
    end

    return sub(data, i, last - 1), last + 1
end

local function _dump(data)
    local bytes = {}
    for i = 1, #data do
        insert(bytes, strbyte(data, i, i))
    end
    return concat(bytes, " ")
end


local function _dumphex(data)
    local bytes = {}
    for i = 1, #data do
        insert(bytes, tohex(strbyte(data, i), 2))
    end
    return concat(bytes, " ")
end

function _send_packet(self, msgType, msg)
    local sock = self.sock

    local length = msg:len() + 4 + 1

    local packet = {
        msgType, 
        _set_int32(length), 
        msg, 
        "\0"
    }

    --print("sending packet...")

    return sock:send(packet)
end

local function _recv_packet(self)
    local sock = self.sock
    local msgType, msgLen, msg
    local data, err = sock:receive(1)
    msgType = data
    data, err = sock:receive(4)
    msgLen = _get_int32(data,1)
    if msgLen - 4 > 0 then
        data, err = sock:receive(msgLen - 4)
        msg = data
    end
    return msgType, msg
end

function new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end

function connect(self, opts)
    local ok, err, data, length
    local host = opts.host
    local port = opts.port or 5432

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    ok, err = sock:connect(host, port)
    if not ok then
        return nil, 'failed to connect: ' .. err
    end

    local params = {} -- oceanbase doesn't support authentication for now

    length = 4 + 4
    for k,v in pairs(params) do
        length = length + k:len() + 1 + v:len() + 1
    end
    length = length + 1

    sock:send({_set_int32(length), _set_int16(3), _set_int16(0)})
    for k,v in pairs(params) do
        sock:send({k,"\0",v,"\0"})
    end
    sock:send({"\0"})

    local typ, msg = _recv_packet(self)

    repeat
        local msgType, _ = _recv_packet(self)
    until msgType == 'Z'  -- ready for query now

    return typ == "R" and _get_int32(msg,1) == 0 
end

function query(self, sql, callback)
    local sock = self.sock
    local length = sql:len() + 4 + 1
    sock:send({ "Q", _set_int32(length), sql, "\0"})
    local columns, pos, msgType, row, data
    repeat
        msgType, data = _recv_packet(self)

        if msgType == 'T' then
            columns, pos = _parse_row_description(data)
            callback(columns)
        end

        if msgType == 'D' then
            row, pos = _parse_row_data(data)
            callback(row)
        end
    until msgType == 'C'
end

function _parse_row_data(data)
    local num_of_field, pos = _get_int16(data, 1)
    local columns = {}
    local msgLen
    for i=1, num_of_field do
        msgLen, pos = _get_int32(data, pos)
        local bytes = sub(data, pos, pos + msgLen - 1)
        insert(columns, bytes)
        pos = pos + msgLen
    end
    return columns, pos
end

function _parse_row_description(data)
    local num_of_field, pos = _get_int16(data, 1)
    local columns = {}
    local column
    for i=1, num_of_field do
        column, pos = _get_cstring(data, pos)
        pos = pos + ( 4 + 2 ) * 3
        insert(columns, column)        
    end
    return columns, pos
end

function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    self.state = nil

    return sock:close()
end

local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}

setmetatable(_M, class_mt)

