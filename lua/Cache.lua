
local function Cache(opt)
   -- Cache interfaces are instantiated in threads, so keep requires scoped
   local sys = require 'sys'
   local paths = require 'paths'
   local lfs = require 'lfs'
   local parallel = require 'libparallel'
   local mmh3 = require 'murmurhash3'

   opt = opt or { }
   local root = opt.cacheDir or os.getenv("HOME")..'/.torch/dataset/'
   local evictDisabled = true
   if opt.evictDisabled ~= nil then
      evictDisabled = opt.evictDisabled
   end

   -- Start with a clean root?
   if opt.clean then
      os.execute('rm -rf '..root)
   end

   -- Make sure root exists
   local function mkdir(path)
      if lfs.attributes(path) == nil then
         mkdir(paths.dirname(path))
         lfs.mkdir(path)
      end
   end
   mkdir(root)

   local function getCachePath(remotePath)
      return paths.concat(root, mmh3.hash32(remotePath, 1))
   end

   local function exists(path, isDir)
      return (isDir and paths.dirp(path)) or paths.filep(path)
   end

   -- Use atomic file operations (link) to guard against
   -- multiple processes importing the same file at the same time
   local function lock(remotePath, localPath, isDir, checkStale)
      local cachePath = getCachePath(remotePath)
      -- Make a junk temp file we can link to for locking
      local tmpFn = os.tmpname()
      local f = io.open(tmpFn, 'w')
      f:write(cachePath)
      f:close()
      -- Try and create the lock
      -- Put the mesos.runid or pid in the lock path so that
      -- crashed processes can not abandon locks
      local id = (mesos and mesos.runid) or tostring(parallel.getpid())
      local lockFn = cachePath..'.'..id..'.lock'
      local ret = parallel.link(tmpFn, lockFn)
      if ret == 0 then
         -- We got the lock!
         -- On OSX we need to check for file staleness since our cache is very long lived
         if checkStale and exists(cachePath, isDir) and checkStale(cachePath) then
            os.remove(cachePath)
         end
         local function unlock()
            -- Make sure the cachePath exists
            -- The calling function could have failed to populate the file
            -- so don't create the link unless its there
            if localPath and exists(cachePath, isDir) then
               -- Make sure the localPath parent directory is created
               mkdir(paths.dirname(localPath))
               -- Try and create the link
               local ret = parallel.symlink(cachePath, localPath)
               if ret ~= 0 then
                  error('failed ('..ret..') to create symlink '..cachePath..' <- '..localPath)
               end
            end
            -- Remove the lock (really only need to remove one of these)
            os.remove(lockFn)
            os.remove(tmpFn)
         end
         -- If the file exists then we do not need to keep the lock
         if exists(cachePath, isDir) then
            unlock()
            return cachePath
         else
            -- Return path and a function to unlock
            return cachePath, unlock
         end
      end
      -- Failed to get the lock, spin wait until the other process finishes importing
      os.remove(tmpFn)
      while paths.filep(lockFn) do
         sys.sleep(0)
      end
      -- Return path
      return cachePath
   end

   local function evict(remotePath, localPath)
      -- TODO: Skipping the multiple users case!
      if not evictDisabled then
         -- TODO: run du and use a size estimate to determine if we really should evict
         local cachePath = getCachePath(remotePath)
         if localPath then
            os.remove(localPath)
         end
         os.remove(cachePath)
         return true
      end
   end

   local function get(remotePath, localPath, download, isDir, checkStale)
      local cachePath, unlock = lock(remotePath, localPath, isDir, checkStale)
      if unlock then
         download(remotePath, cachePath, isDir)
         unlock()
      end
      return cachePath
   end

   return {
      lock = lock,
      evict = evict,
      get = get,
   }
end

return Cache
