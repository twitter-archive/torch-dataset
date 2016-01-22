local test = require 'regress'
local paths = require 'paths'
local Cache = require 'dataset.Cache'
local ipc = require 'libipc'

test {
   testLockAndEvict = function()
      local cache = Cache({ evictDisabled = false, clean = true })
      local url = math.random()
      local cachePath, unlock = cache.lock(url)
      test.mustBeTrue(type(cachePath) == 'string', 'expected to get a cachePath')
      test.mustBeTrue(type(unlock) == 'function', 'expected to own the lock')
      cache.evict(url)
      test.mustBeTrue(paths.filep(cachePath) == false, 'expected cache file to not exist')
      local url2 = math.random()
      local cachePath2, unlock2 = cache.lock(url2)
      test.mustBeTrue(cachePath ~= cachePath2, 'expected different paths')
      unlock2()
   end,

   testLockAndEvictWithLocalPath = function()
      local cache = Cache({ evictDisabled = false, clean = true })
      local url = math.random()
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
      local cache = Cache({ evictDisabled = false, clean = true })
      for _ = 1,10 do
         local url = tostring(math.random())
         cache.evict(url)
         local ret = { ipc.map(math.random(1, 100), function(url)
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
}
