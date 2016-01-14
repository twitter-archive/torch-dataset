-- test utils

local localHdfsPath = 'viewfs://' .. paths.dirname(paths.thisfile()) .. '/hdfs/files'

local testFiles = {'part-r-00000', 'part-r-00001', 'part-r-00002', 'part-r-00003'}

local testFileSizes = { }
testFileSizes["part-r-00000"] = 21
testFileSizes["part-r-00001"] = 24
testFileSizes["part-r-00002"] = 17
testFileSizes["part-r-00003"] = 16

local function removePrefix(url)
   return url:split("://")[2]
end

-- table of lines from all the files
-- can be sorted to assert presense of all lines in the output
local allLines = {}

for i, fn in ipairs(testFiles) do
   local f = io.open(removePrefix(localHdfsPath .. '/' .. fn))
   while true do
        line = f:read()
        if line == nil then break end
        table.insert(allLines, line)
   end
end

local function _listCopy(t)
   local r = { }
   for _,v in ipairs(t) do
      table.insert(r, v)
   end
   return r
end

local function _noDupes(t)
   local r = { }
   for _,v in ipairs(t) do
      if r[v] ~= nil then
         return false
      else
         r[v] = true
      end
   end
   return true
end

local function _listEquals(a, b)
   for i,v in ipairs(a) do
      if b[i] ~= v then
         return false
      end
   end
   return true
end

local function MockHDFS(cache, opt)
   local opt = opt or { }
   local verbose = opt.verbose or false

   local function removePrefix(url)
      local _, e = string.find(url, "://")
      return string.sub(url, e + 1)
   end

   return {
      parts = function(root)
         local ret = {}
         for l in lfs.dir(removePrefix(root)) do
            if l ~= '.' and l ~= '..' then
               table.insert(ret, root .. '/' .. l)
            end
         end
         table.sort(ret)
         return ret
      end,

      get = function(remotePath)
         local mmh3 = require 'murmurhash3'
         local localPath = '/tmp/' .. mmh3.hash32(remotePath)
         if verbose then
            print("[INFO:] Local HDFS - importing " .. remotePath)
         end
         os.execute('cp ' .. removePrefix(remotePath) .. ' ' .. localPath)
         return localPath
      end,

      evict = function(remotePath)
         local mmh3 = require 'murmurhash3'
         local localPath = '/tmp/' .. mmh3.hash32(remotePath)
         os.remove(localPath)
      end,
   }
end

local SlowFS = require 'dataset.SlowFS'
SlowFS.register('viewfs://', MockHDFS)

return {
   localHdfsPath = localHdfsPath,
   testFileSizes = testFileSizes,
   testFileContent = testFileContent,
   allLines = allLines,
   listEquals = _listEquals,
   listCopy = _listCopy,
   noDupes = _noDupes,
}

