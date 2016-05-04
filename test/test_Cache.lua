local test = require 'regress'
local paths = require 'paths'
local dataset = require 'libdataset'
local Cache = require 'dataset.Cache'
local ipc = require 'libipc'
local sys = require 'sys'

test {
--[[
   testLockRemoveUnlockLockAndEvict = function()
      local cache = Cache({ forceEvict = true})
      local url = tostring(math.random())
      local cachePath, unlock = cache.lock(url)
      test.mustBeTrue(type(cachePath) == 'string', 'expected to get a cachePath')
      test.mustBeTrue(type(unlock) == 'function', 'expected to own the lock')
      sys.execute('touch '..cachePath)
      os.remove(cachePath)
      unlock()
      cachePath, unlock = cache.lock(url)
      test.mustBeTrue(type(cachePath) == 'string', 'expected to get a cachePath')
      test.mustBeTrue(type(unlock) == 'function', 'expected to own the lock')
      cache.evict(url)
      test.mustBeTrue(paths.filep(cachePath) == false, 'expected cache file to not exist')
      local url2 = tostring(math.random())
      local cachePath2, unlock2 = cache.lock(url2)
      test.mustBeTrue(cachePath ~= cachePath2, 'expected different paths')
      unlock2()
   end,

   testLockAndEvict = function()
      local cache = Cache({ forceEvict = true, clean = true })
      local url = tostring(math.random())
      local cachePath, unlock = cache.lock(url)
      test.mustBeTrue(type(cachePath) == 'string', 'expected to get a cachePath')
      test.mustBeTrue(type(unlock) == 'function', 'expected to own the lock')
      sys.execute('touch '..cachePath)
      unlock()
      cache.evict(url)
      test.mustBeTrue(paths.filep(cachePath) == false, 'expected cache file to not exist')
      local url2 = tostring(math.random())
      local cachePath2, unlock2 = cache.lock(url2)
      test.mustBeTrue(cachePath ~= cachePath2, 'expected different paths')
      unlock2()
   end,

   testLockAndEvictWithLocalPath = function()
      local cache = Cache({ forceEvict = true, clean = true })
      local url = tostring(math.random())
      local fn = '/tmp/'..math.random()
      os.remove(fn)
      test.mustBeTrue(paths.filep(fn) == false, 'expected local file to not exist')
      local cachePath, unlock = cache.lock(url, fn)
      test.mustBeTrue(paths.filep(fn) == false, 'expected local file to not exist')
      test.mustBeTrue(type(cachePath) == 'string', 'expected to get a cachePath')
      test.mustBeTrue(type(unlock) == 'function', 'expected to own the lock')
      local f = io.open(cachePath, 'w')
      f:write('hi')
      f:close()
      test.mustBeTrue(paths.filep(fn) == false, 'expected local file to not exist')
      unlock()
      test.mustBeTrue(paths.filep(fn), 'expected local file to exist')
      local f = io.open(fn)
      local e = f:read('*all')
      f:close()
      test.mustBeTrue(e == 'hi', 'expected file contents to be the same')
      cache.evict(url, fn)
      test.mustBeTrue(paths.filep(fn) == false, 'expected local file to not exist')
      test.mustBeTrue(paths.filep(cachePath) == false, 'expected cache file to not exist')
   end,

   testMultipleLockers = function()
      local cache = Cache({ forceEvict = true, clean = true })
      for _ = 1,10 do
         local url = tostring(math.random())
         cache.evict(url)
         local ret = { ipc.map(math.random(1, 100), function(url, mapid)
            local sys = require 'sys'
            local ipc = require 'libipc'
            local Cache = require 'dataset.Cache'
            local cache = Cache()
            local cachePath, unlock = cache.lock(url)
            local id = tostring(ipc.gettid())
            if unlock then
               sys.sleep(math.random(1, 100) / 100) -- pretend to be slow
               local f = io.open(cachePath, 'w')
               f:write(id)
               f:close()
               unlock()
               return id, id
            else
               local f = io.open(cachePath)
               local fid = f:read('*all')
               f:close()
               return id, fid
            end
         end, url):join() }
         local winner
         for i = 1,#ret,2 do
            if ret[i] == ret[i + 1] then
               test.mustBeTrue(winner == nil, 'expected only one winner')
               winner = ret[i]
            end
            for j = i + 2,#ret,2 do
               test.mustBeTrue(ret[i] ~= ret[j], 'expected everyone to be unique')
            end
         end
         if not winner then
            print(ret)
         end
         test.mustBeTrue(winner ~= nil, 'expected one winner')
         for i = 2,#ret,2 do
            test.mustBeTrue(ret[i] == winner, 'expected all reflect the winner')
         end
         cache.evict(url)
      end
   end,

   testCacheSize = function()
      local limit = 500 * 1024
      local cache = Cache({ maxCacheSize = limit, clean = true })
      local q = ipc.workqueue('cache')
      local m = ipc.map(7, function(limit)
         local ipc = require 'libipc'
         local Cache = require 'dataset.Cache'
         local cache = Cache({ maxCacheSize = limit })
         local q = ipc.workqueue('cache')
         while true do
            local url = q:read()
            if url then
               local cachePath, unlock = cache.lock(url)
               if unlock then
                  local f = io.open(cachePath, 'w+')
                  for _ = 1,math.random(1024) do
                     f:write(math.random(9))
                  end
                  f:close()
                  unlock()
               end
            else
               break
            end
         end
      end, limit)
      local cfn = os.getenv("HOME")..'/.torch/dataset/'
      for _ = 1,1000 do
         q:write(tostring(math.random()))
      end
      for _ = 1,7 do
         q:write(nil)
      end
      m:join()
      local size = dataset.dirsize(cfn)
      assert(size <= limit, 'size to stay under '..limit..', saw '..size)
   end,

   testSimultaneousDownloads = function()
      local cache = Cache({ forceEvict = true, clean = true })
      local url1 = tostring(math.random())
      local cachePath1, unlock1 = cache.lock(url1)
      assert(type(cachePath1) == 'string')
      assert(type(unlock1) == 'function')
      local url2 = tostring(math.random())
      local cachePath2, unlock2 = cache.lock(url2)
      assert(type(cachePath2) == 'string')
      assert(type(unlock2) == 'function')
      unlock1()
      unlock2()
   end,
--]]
   testDirsize = function()
      local n = os.tmpname()
      os.execute('rm '..n)
      os.execute('mkdir '..n)
      kb = ''
      for _ = 1,1024 do
         kb = kb..'x'
      end
      local f = io.open(n..'/f', 'w')
      for _ = 1,1024*1024 do
         f:write(kb)
      end
      f:close()
      local s = dataset.dirsize(n)
      os.execute('ls -al '..n)
      assert(s == 1073741824, 'expected 1073741824 saw '..s)
   end,
}
