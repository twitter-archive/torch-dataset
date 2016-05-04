
local function Cache(opt)
   -- Cache interfaces are instantiated in threads, so keep requires scoped
   local sys = require 'sys'
   local paths = require 'paths'
   local lfs = require 'lfs'
   local ipc = require 'libipc'
   local dataset = require 'libdataset'
   local mmh3 = require 'murmurhash3'

   opt = opt or { }
   local root = opt.cacheDir or os.getenv("HOME")..'/.torch/dataset/'
   local GB = 1024 * 1024 * 1024
   local maxCacheSize = opt.maxCacheSize or (mesos and ((mesos.disk * GB) - (4 * GB))) or (64 * GB)
   local forceEvict = opt.forceEvict or false

   -- Start clean?
   if opt.clean == true then
      sys.execute('rm -rf '..root)
   end

   -- Make sure root exists
   local function mkdir(path)
      if lfs.attributes(path) == nil then
         mkdir(paths.dirname(path))
         lfs.mkdir(path)
      end
   end
   mkdir(root)

   local function loadLRU()
      local lfn = paths.concat(root, '.lock')
      local flock = ipc.flock(lfn)
      local fn = paths.concat(root, '.lru')
      local lru = { }
      local f = io.open(fn, 'r')
      if f then
         for line in f:lines() do
            table.insert(lru, line)
         end
         f:close()
      end
      return lru, flock
   end

   local function saveLRU(lru, lruflock)
      local fn = paths.concat(root, '.lru')
      local f = io.open(fn, 'w+')
      for _,entry in ipairs(lru) do
         f:write(entry..'\n')
      end
      f:close()
      lruflock:close()
   end

   local function getCachePath(remotePath)
      return paths.concat(root, mmh3.hash32(remotePath, 1))
   end

   local function exists(path, isDir)
      return (isDir and paths.dirp(path)) or paths.filep(path)
   end

   local function lock(remotePath, localPath, isDir)
      -- Hash the path
      local cachePath = getCachePath(remotePath)
      local sfn = cachePath..'.lock'
      -- This will become our file lock
      local flock, lru, lruflock
      -- Create an unlock closure
      local function unlock()
         -- Make sure the cachePath exists
         -- The calling function could have failed to populate the file
         if exists(cachePath, isDir) then
            if localPath then
               -- Make sure the localPath parent directory is created
               mkdir(paths.dirname(localPath))
               -- Try and create the link
               local ret = dataset.symlink(cachePath, localPath)
               if ret ~= 0 then
                  error('failed ('..ret..') to create symlink '..cachePath..' <- '..localPath)
               end
            end
            if not lruflock then
               -- Lock and load the cache
               lru, lruflock = loadLRU()
               for i,entry in ipairs(lru) do
                  if entry == cachePath then
                     -- If its already there remove it
                     table.remove(lru, i)
                     break
                  end
               end
            end
            -- Add it to the LRU and unlock it
            table.insert(lru, 1, cachePath)
            saveLRU(lru, lruflock)
         end
         -- Unlock and remove the file lock
         if flock then
            os.remove(sfn)
            flock:close()
         end
      end
      -- Lock and load the cache
      lru, lruflock = loadLRU()
      -- If the file exists in the cache, move it up in the LRU, unlock the cache and return it
      for i,entry in ipairs(lru) do
         if entry == cachePath then
            -- Check if the file actually exists (it could have been manually deleted)
            if paths.filep(cachePath) then
               table.remove(lru, i)
               unlock()
               return cachePath
            else
               table.remove(lru, i)
               break
            end
         end
      end
      -- It needs to be downloaded, make sure there is room in the cache
      while dataset.dirsize(root) > (80 * maxCacheSize / 100) and #lru > 0 do
         local ok,err = os.remove(table.remove(lru))
         if ok == nil then
            error(err)
         end
      end
      -- Flush any changes
      saveLRU(lru, lruflock)
      lruflock = nil
      -- Switch the lock away from the LRU and to the file itself
      -- If any other process gets here they will block
      flock = ipc.flock(sfn)
      -- If it exists, then we don't need to re-download it
      if exists(cachePath, isDir) then
         unlock()
         unlock = nil
      end
      -- Finally return path and a function to unlock
      return cachePath, unlock
   end

   local function get(remotePath, localPath, download, isDir)
      local cachePath, unlock = lock(remotePath, localPath, isDir)
      if unlock then
         if not pcall(download, remotePath, cachePath, isDir) then
            os.remove(cachePath)
         end
         unlock()
      end
      return cachePath
   end

   local function evict(remotePath, localPath)
      if localPath then
         os.remove(localPath)
      end
      if forceEvict then
         local cachePath = getCachePath(remotePath)
         local lru, flock = loadLRU()
         for i,entry in ipairs(lru) do
            if entry == cachePath then
               os.remove(entry)
               table.remove(lru, i)
               saveLRU(lru, flock)
               return
            end
         end
         flock:close()
      end
   end

   return {
      lock = lock,
      get = get,
      evict = evict,
   }
end

return Cache
