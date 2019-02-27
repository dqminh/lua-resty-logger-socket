-- Copyright (C) 2013-2014 Jiale Zhi (calio), CloudFlare Inc.
--require "luacov"

local ffi = require "ffi"
local base = require "resty.core.base"

local ffi_new = ffi.new
local ffi_str = ffi.string
local ffi_fill = ffi.fill
local C = ffi.C
local string_format = string.format
local error = error
local errmsg = base.get_errmsg_ptr()
local FFI_OK = base.FFI_OK

local concat                = table.concat
local tcp                   = ngx.socket.tcp
local timer_at              = ngx.timer.at
local ngx_log               = ngx.log
local ngx_sleep             = ngx.sleep
local type                  = type
local pairs                 = pairs
local tostring              = tostring
local debug                 = ngx.config.debug

local DEBUG                 = ngx.DEBUG
local NOTICE                = ngx.NOTICE
local WARN                  = ngx.WARN
local ERR                   = ngx.ERR
local CRIT                  = ngx.CRIT

ffi.cdef[[
int ngx_http_lua_socket_tcp_ffi_set_buf(ngx_http_request_t *r, void *u, void *cdata_ctx, size_t len, char **err);
]]


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 5)

local is_exiting

if not ngx.config or not ngx.config.ngx_lua_version
    or ngx.config.ngx_lua_version < 9003 then

    is_exiting = function() return false end

    ngx_log(CRIT, "We strongly recommend you to update your ngx_lua module to "
            .. "0.9.3 or above. lua-resty-logger-socket will lose some log "
            .. "messages when Nginx reloads if it works with ngx_lua module "
            .. "below 0.9.3")
else
    is_exiting = ngx.worker.exiting
end


_M._VERSION = '0.03'

-- user config
local flush_limit           = 4096         -- 4KB
local drop_limit            = 1048576      -- 1MB
local timeout               = 1000         -- 1 sec
local host
local port
local path
local max_buffer_reuse      = 10000        -- reuse buffer for at most 10000
                                           -- times
local periodic_flush        = nil
local need_periodic_flush   = nil

-- internal variables
local buffer_size           = 0
-- 2nd level buffer as ffi char array, it stores logs ready to be sent out.
local send_buffer
-- Current length of 2nd level buffer
local send_buffer_len       = 0
-- 1st level buffer, it stores incoming logs
local log_buffer_data       = new_tab(20000, 0)
-- number of log lines in current 1st level buffer, starts from 0
local log_buffer_index      = 0

local last_error

local connecting
local connected
local exiting
local retry_connect         = 0
local retry_send            = 0
local max_retry_times       = 3
local retry_interval        = 100         -- 0.1s
local pool_size             = 10
local flushing
local logger_initted
local logger_enable_ffi
local counter               = 0

local function check_tcp(tcp)
    if not tcp or type(tcp) ~= "table" then
        return error("bad \"tcp\" argument")
    end

    tcp = tcp[1]
    if type(tcp) ~= "userdata" then
        return error("bad \"tcp\" argument")
    end

    return tcp
end

local function ffi_set_buffer(tcp, buffer, len)
    tcp = check_tcp(tcp)
    local r = base.get_request()
    if not r then
        return error("no request found")
    end
    local rc = C.ngx_http_lua_socket_tcp_ffi_set_buf(r, tcp, buffer, len, errmsg)
    if rc ~= FFI_OK then
        return nil, ffi_str(errmsg[0])
    end
    return true
end


local function _write_error(msg)
    last_error = msg
end

local function _do_connect()
    local ok, err, sock

    if not connected then
        sock, err = tcp()
        if not sock then
            _write_error(err)
            return nil, err
        end

        sock:settimeout(timeout)
    end

    -- "host"/"port" and "path" have already been checked in init()
    if host and port then
        ok, err =  sock:connect(host, port)
    elseif path then
        ok, err =  sock:connect("unix:" .. path)
    end

    if not ok then
        return nil, err
    end

    return sock
end

local function _connect()
    local err, sock

    if connecting then
        if debug then
            ngx_log(DEBUG, "previous connection not finished")
        end
        return nil, "previous connection not finished"
    end

    connected = false
    connecting = true

    retry_connect = 0

    while retry_connect <= max_retry_times do
        sock, err = _do_connect()

        if sock then
            connected = true
            break
        end

        if debug then
            ngx_log(DEBUG, "reconnect to the log server: ", err)
        end

        -- ngx.sleep time is in seconds
        if not exiting then
            ngx_sleep(retry_interval / 1000)
        end

        retry_connect = retry_connect + 1
    end

    connecting = false
    if not connected then
        return nil, "try to connect to the log server failed after "
                    .. max_retry_times .. " retries: " .. err
    end

    return sock
end

local function _prepare_stream_buffer()
    local idx = 0

    for i = 1, log_buffer_index do
        local log_line = log_buffer_data[i]
        local msg, msg_len
        if type(log_line) == "table" then
            msg, msg_len = log_line[1], log_line[2]
            ffi.copy(send_buffer + idx, msg, msg_len)
        else
            msg, msg_len = log_line, #log_line
            ffi.copy(send_buffer + idx, ffi_str(msg), msg_len)
        end
        idx = idx + msg_len
        send_buffer_len = send_buffer_len + msg_len
    end

    log_buffer_index = 0
    counter = counter + 1
    if counter > max_buffer_reuse then
        log_buffer_data = new_tab(20000, 0)
        counter = 0
        if debug then
            ngx_log(DEBUG, "log buffer reuse limit (" .. max_buffer_reuse
                    .. ") reached, create a new \"log_buffer_data\"")
        end
    end
end

local function _do_flush()
    local sock, err = _connect()
    if not sock then
        return nil, err
    end

    local bytes
    local err
    if not logger_enable_ffi then
        bytes, err = sock:send(ffi.string(send_buffer, send_buffer_len))
        if not bytes then
            -- "sock:send" always closes current connection on error
            return nil, err
        end
    else
        if not ffi_set_buffer(sock, send_buffer, send_buffer_len) then
            return nil, "failed to set buffer using ffi"
        end
        bytes, err = sock:ffi_send()
        if not bytes then
            return nil, err
        end
    end

    if debug then
        ngx.update_time()
        ngx_log(DEBUG, ngx.now(), ":log flush:" .. bytes)
    end

    local ok, err = sock:setkeepalive(0, pool_size)
    if not ok then
        return nil, err
    end

    return bytes
end

local function _need_flush()
    if buffer_size > 0 then
        return true
    end

    return false
end

local function _flush_lock()
    if not flushing then
        if debug then
            ngx_log(DEBUG, "flush lock acquired")
        end
        flushing = true
        return true
    end
    return false
end

local function _flush_unlock()
    if debug then
        ngx_log(DEBUG, "flush lock released")
    end
    flushing = false
end

local function _flush()
    local ok, err

    -- pre check
    if not _flush_lock() then
        if debug then
            ngx_log(DEBUG, "previous flush not finished")
        end
        -- do this later
        return true
    end

    if not _need_flush() then
        if debug then
            ngx_log(DEBUG, "no need to flush:", log_buffer_index)
        end
        _flush_unlock()
        return true
    end

    -- start flushing
    retry_send = 0
    if debug then
        ngx_log(DEBUG, "start flushing")
    end

    local bytes
    while retry_send <= max_retry_times do
        if log_buffer_index > 0 then
            _prepare_stream_buffer()
        end

        bytes, err = _do_flush()

        if bytes then
            break
        end

        if debug then
            ngx_log(DEBUG, "resend log messages to the log server: ", err)
        end

        -- ngx.sleep time is in seconds
        if not exiting then
            ngx_sleep(retry_interval / 1000)
        end

        retry_send = retry_send + 1
    end

    _flush_unlock()

    if not bytes then
        local err_msg = "try to send log messages to the log server "
                        .. "failed after " .. max_retry_times .. " retries: "
                        .. err
        _write_error(err_msg)
        return nil, err_msg
    else
        if debug then
            ngx_log(DEBUG, "send " .. bytes .. " bytes")
        end
    end

    buffer_size = buffer_size - send_buffer_len
    ffi.fill(send_buffer, send_buffer_len)
    send_buffer_len = 0

    return bytes
end

local function _periodic_flush()
    if need_periodic_flush then
        -- no regular flush happened after periodic flush timer had been set
        if debug then
            ngx_log(DEBUG, "performing periodic flush")
        end
        _flush()
    else
        if debug then
            ngx_log(DEBUG, "no need to perform periodic flush: regular flush "
                    .. "happened before")
        end
        need_periodic_flush = true
    end

    timer_at(periodic_flush, _periodic_flush)
end

local function _flush_buffer()
    local ok, err = timer_at(0, _flush)

    need_periodic_flush = false

    if not ok then
        _write_error(err)
        return nil, err
    end
end

local function _write_buffer_cdata(msg, msg_len)
    log_buffer_index = log_buffer_index + 1
    local buf = ffi_new("char[?]", msg_len)
    ffi.copy(buf, msg, msg_len)
    log_buffer_data[log_buffer_index] = {buf, msg_len}
    buffer_size = buffer_size + msg_len
    return buffer_size
end

local function _write_buffer(msg)
    log_buffer_index = log_buffer_index + 1
    log_buffer_data[log_buffer_index] = msg

    buffer_size = buffer_size + #msg


    return buffer_size
end

function _M.init(user_config)
    if (type(user_config) ~= "table") then
        return nil, "user_config must be a table"
    end

    for k, v in pairs(user_config) do
        if k == "host" then
            host = v
        elseif k == "port" then
            port = v
        elseif k == "path" then
            path = v
        elseif k == "flush_limit" then
            flush_limit = v
        elseif k == "drop_limit" then
            drop_limit = v
        elseif k == "timeout" then
            timeout = v
        elseif k == "max_retry_times" then
            max_retry_times = v
        elseif k == "retry_interval" then
            -- ngx.sleep time is in seconds
            retry_interval = v
        elseif k == "pool_size" then
            pool_size = v
        elseif k == "max_buffer_reuse" then
            max_buffer_reuse = v
        elseif k == "periodic_flush" then
            periodic_flush = v
        elseif k == "enable_ffi" then
            logger_enable_ffi = v
        end
    end

    if not (host and port) and not path then
        return nil, "no logging server configured. \"host\"/\"port\" or "
                .. "\"path\" is required."
    end


    if (flush_limit >= drop_limit) then
        return nil, "\"flush_limit\" should be < \"drop_limit\""
    end

    flushing = false
    exiting = false
    connecting = false

    connected = false
    retry_connect = 0
    retry_send = 0
    send_buffer = ffi.new("char[?]", drop_limit)
    ffi.fill(send_buffer, drop_limit)

    logger_initted = true

    if periodic_flush then
        if debug then
            ngx_log(DEBUG, "periodic flush enabled for every "
                    .. periodic_flush .. " milliseconds")
        end
        need_periodic_flush = true
        timer_at(periodic_flush, _periodic_flush)
    end

    return logger_initted
end

function _M.log_cdata(msg, msg_len)
    if not logger_initted then
        return nil, "not initialized"
    end

    if not logger_enable_ffi then
      return _M.log(ffi_str(msg, msg_len))
    end

    local bytes

    if (debug) then
        ngx.update_time()
        ngx_log(DEBUG, ngx.now(), ":log message length: " .. msg_len)
    end

    -- response of "_flush_buffer" is not checked, because it writes
    -- error buffer
    if (is_exiting()) then
        exiting = true
        _write_buffer_cdata(msg, msg_len)
        _flush_buffer()
        if (debug) then
            ngx_log(DEBUG, "Nginx worker is exiting")
        end
        bytes = 0
    elseif (msg_len + buffer_size < flush_limit) then
        _write_buffer_cdata(msg, msg_len)
        bytes = msg_len
    elseif (msg_len + buffer_size <= drop_limit) then
        _write_buffer_cdata(msg, msg_len)
        _flush_buffer()
        bytes = msg_len
    else
        _flush_buffer()
        if (debug) then
            ngx_log(DEBUG, "logger buffer is full, this log message will be "
                    .. "dropped")
        end
        bytes = 0
        --- this log message doesn't fit in buffer, drop it
    end

    if last_error then
        local err = last_error
        last_error = nil
        return bytes, err
    end

    return bytes
end

function _M.log(msg)
    if not logger_initted then
        return nil, "not initialized"
    end

    local bytes

    if type(msg) ~= "string" then
        msg = tostring(msg)
    end

    if (debug) then
        ngx.update_time()
        ngx_log(DEBUG, ngx.now(), ":log message length: " .. #msg)
    end

    local msg_len = #msg

    -- response of "_flush_buffer" is not checked, because it writes
    -- error buffer
    if (is_exiting()) then
        exiting = true
        _write_buffer(msg)
        _flush_buffer()
        if (debug) then
            ngx_log(DEBUG, "Nginx worker is exiting")
        end
        bytes = 0
    elseif (msg_len + buffer_size < flush_limit) then
        _write_buffer(msg)
        bytes = msg_len
    elseif (msg_len + buffer_size <= drop_limit) then
        _write_buffer(msg)
        _flush_buffer()
        bytes = msg_len
    else
        _flush_buffer()
        if (debug) then
            ngx_log(DEBUG, "logger buffer is full, this log message will be "
                    .. "dropped")
        end
        bytes = 0
        --- this log message doesn't fit in buffer, drop it
    end

    if last_error then
        local err = last_error
        last_error = nil
        return bytes, err
    end

    return bytes
end

function _M.initted()
    return logger_initted
end

_M.flush = _flush

return _M

