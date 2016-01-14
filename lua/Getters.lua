--[[
   These functions are used from a thread pool and
   can not rely on anything outside their function
   scope. Put any external requirements in the function
   or else it will not be accessible.
--]]

local function getFile(url, offset, length)
   local f = io.open(url, 'r')
   if f ~= nil then
      local contents
      if offset ~= nil and length ~= nil then
         if offset >= 0 and length ~= 0 then
            f:seek("set", offset)
            contents = f:read(math.abs(length))
         end
      else
         contents = f:read('*all')
      end
      f:close()
      return contents
   end
end

local function getHTTP(url, offset, length)
   local sys = require 'sys'
   local http = require 'socket.http'
   local function _download()
      local t1 = sys.clock() + 30
      while sys.clock() < t1 do
         local body,code = http.request(url, { bindBeforeConnect = true, timeout = 10 })
         if code == 200 then
            return body
         elseif code == 404 then
            print('ERROR: failed to download: ' .. url .. ' [error code = ' .. code .. ']')
            return nil
         else
            print('WARNING: failed to download: ' .. url .. ' [error code = ' .. (code or 'nil') .. '], retrying...')
         end
      end
   end
   local contents = _download()
   if contents then
      if offset ~= nil and length ~= nil then
         -- Negative length indicates chunked dataset, first 8 bytes is true length
         if length < 0 then
            length = tonumber(contents:sub(offset + 1, offset + 8), 16)
            return contents:sub(offset + 9, offset + 8 + length)
         else
            return contents:sub(offset + 1, offset + length)
         end
      else
         return contents
      end
   end
end

local function getTensor(url, offset, length)
   return url
end

local function getters(url, indexType)
   if url and string.sub(url, 1, 4) == 'http' then
      return getHTTP
   elseif indexType and indexType == 'Tensor' then
      return getTensor
   else
      return getFile
   end
end

return getters
