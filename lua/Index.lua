local http = require 'socket.http'
local paths = require 'paths'
local lfs = require 'lfs'
local Cache = require 'dataset.Cache'
local IndexCSV = require 'dataset.IndexCSV'
local IndexDirectory = require 'dataset.IndexDirectory'
local IndexSlowFS = require 'dataset.IndexSlowFS'
local IndexTensor = require 'dataset.IndexTensor'
local SlowFS = require 'dataset.SlowFS'

local function Index(url, partition, partitions, opt)

   local partition = opt.partition or 1
   local partitions = opt.partitions or 1
   opt = opt or { }
   local cache = Cache(opt)

   local function startsWith(s, p)
      return string.sub(s, 1, string.len(p)) == p
   end

   local function hasPrefix(url)
      return #url:split("://") > 1
   end

   local function isDirectory(fn)
      local ok1,ok2 = pcall(function()
         return lfs.attributes(fn).mode == 'directory'
      end)
      return (ok1 and ok2)
   end

   local function fetchIndexFiles(url, ext, hasMeta)
      if startsWith(url, 'http') then
         local function download(url, fn)
            local body = http.request(url)
            if body then
               local f = io.open(fn, 'w')
               f:write(body)
               f:close()
            end
         end
         local fn = cache.get(url, nil, download)
         if hasMeta then
            return fn, cache.get(url:sub(1, #url-4)..'-meta.'..ext, nil, download)
         end
         return fn
      elseif hasPrefix(url) then
         local slowFS = SlowFS.find(url)(cache, opt)
         local fn = slowFS.get(url)
         if hasMeta then
            return fn, slowFS.get(url:sub(1, #url-4)..'-meta.'..ext)
         end
         return fn
      end
      return url
   end

   if torch.isTensor(url) or type(url) == 'table' then
      return IndexTensor(url, partition, partitions, opt)
   elseif type(url) == 'string' then
      local ext = paths.extname(url)
      if ext == 't7' or ext == 'th' then
         return IndexTensor(torch.load(fetchIndexFiles(url, ext)), partition, partitions, opt)
      elseif ext == 'csv' then
         local fn, mfn = fetchIndexFiles(url, ext, true)
         opt.metaURL = mfn
         return IndexCSV(fn, partition, partitions, opt)
      elseif hasPrefix(url) then
         return IndexSlowFS(url, partition, partitions, opt)
      elseif isDirectory(url) then
         return IndexDirectory(url, partition, partitions, opt)
      end
   end
   error('Index unsupported url: '..url)
end

return Index
