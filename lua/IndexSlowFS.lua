local parallel = require 'libparallel'
local Cache = require 'dataset.Cache'
local IndexUtils = require 'dataset.IndexUtils'
local SlowFS = require 'dataset.SlowFS'

local function IndexSlowFS(url, partition, partitions, opt)

   local partition = partition or 1
   assert(partition >= 1)
   local partitions = partitions or 1
   assert(partitions >= 1)
   assert(partition <= partitions)
   local opt = opt or { }
   local verbose = opt.verbose or false
   local cache = Cache(opt)
   local slowFS = SlowFS.find(url)(cache, opt)
   local indexUtils = IndexUtils(opt)

   local totalItems = 0  --total item counter, updated as we parse part files

   -- keep track of the permutation of part files
   -- there can be repeats in this queue
   local queue = { }

   -- handler on the background file importer
   local importer

   -- what part files are currently resident (by part index)
   local resident = { }

   -- files in this partition
   local partitionFiles

   ------------ Index construction --------------

   local function addFile(files, file)
      table.insert(files, file)
   end

   local function loadSlowFSFiles(root)
      local files = { }  -- all part files in this partition
      local parts = slowFS.parts(root)

      for _,part in ipairs(parts) do
         addFile(files, part:sub(#root + 2))
      end

      return files
   end

   local function downsample(t, partition, partitions)
      assert(partition >= 1)
      assert(partitions >= 1)
      assert(partition <= partitions)
      assert(#t >= partitions, 'number of part files ('..#t..') must be >= number of partitions ('..partitions..')')

      if partitions == 1 then
         return t
      else
         local ret = { }
         for i,item in ipairs(t) do
            if (i % partitions) == partition then
               table.insert(ret, item)
            elseif (i % partitions) == 0 and partition == partitions then
               -- last items goes to the last partition
               table.insert(ret, item)
            end
         end
         return ret
      end
   end

   local function makeFileIndex(fileName, url, opt, idx, SlowFS)
      local Cache = require 'dataset.Cache'
      local cache = Cache(opt)
      local slowFS = SlowFS(cache)

      local partIndex = { }
      local numItems = 0

      local fileURL = url .. '/' .. fileName
      local fpath = slowFS.get(fileURL)
      local ipath = fpath..'.i'

      local f = io.open(fpath, 'r')
      local fi = io.open(ipath, 'w')
      fi:write(string.format("%10X", f:seek()))
      local lines = f:lines()
      for _ in lines do
         numItems = numItems + 1 -- count total number of items
         local str = string.format("%10X", f:seek())
         assert(string.len(str) == 10, "must have at 10 hex values for file position, instead was "..string.len(str))
         fi:write(str)
      end
      f:close()
      fi:close()

      local partIndex = { url = url,
                          fileName = fileName,
                          indexPath = ipath,
                          filePath = fpath,
                          itemCount = numItems,
                          idx = idx,
                        }
      return partIndex
   end

   ------------ Index access --------------

   -- item at the actual part index
   -- return filePath, offset, length of the item
   local function innerItemAt(ip, fp, fileName, url)
      return function(i)
         -- Open index, seek to item,
         local fi = io.open(ip, 'r')
         assert(fi, 'failed to open "'..ip..'" at: '..debug.traceback())
         fi:seek("set", 10 * (i - 1))
         local s = fi:read(20)
         assert(type(s) == 'string' and string.len(s) == 20, 'index "'..ip..'" item '..i..' expected a string of length 20, got: "'..tostring(s)..'"')
         fi:close()

         local offset = tonumber(s:sub(1, 10), 16)
         local length = tonumber(s:sub(11, 20), 16) - offset - 1

         local fptry = io.open(fp, 'r')
         assert(fptry, 'File must be present on local disk.')
         fptry:close()

         return fp, offset, length
      end
   end

   ------------ File import and cache  --------------

   local function startNextImport()
      assert(#queue > 0, "must have at least one file to import")
      local idx = table.remove(queue, 1)
      if verbose then
         print("[INFO:] Starting import of file " .. url..'/'..partitionFiles[idx])
      end
      importer = parallel.map(1, makeFileIndex, partitionFiles[idx], url, opt, idx, SlowFS.find(url))
   end

   local function addPartIndex(partIndex)
      assert(partIndex <= #partitionFiles and partIndex > 0, "index of part file must be valid")
      table.insert(queue, partIndex)
      if not importer then
         startNextImport()
      end
   end

   local function finishFileImport()
      local partIndex = importer:join()
      partIndex.itemAt = innerItemAt(partIndex.indexPath,
                                     partIndex.filePath,
                                     partIndex.fileName,
                                     partIndex.url)
      return partIndex
   end

   --------------- MAIN -----------------

   local allFiles = loadSlowFSFiles(url)
   partitionFiles = downsample(allFiles, partition, partitions)
   local urlPrefix = url

   local numFiles = #partitionFiles

   -- initial estimate of num items: at least one record/file
   totalItems = #partitionFiles

   -- start by loading one part file
   assert(#partitionFiles > 0, "Must have at least 1 part files per partition...")

   --------------- Index functions -------------

   local function itemAt(partIndex, index)
      -- it better be resident, itemsInPart called first!
      assert(resident[partIndex])
      -- get the item at index out of the part
      assert(index >= 1 and index <= resident[partIndex].itemCount, 'itemAt('..partIndex..', '..index..') out of range: [1,'..resident[partIndex].itemCount..']: '..debug.traceback())
      return resident[partIndex].itemAt(index)
   end

   local function doneWithPart(partIndex)
      assert(resident[partIndex], "part must be present to remove it")
      if #partitionFiles > 1 then
         if verbose then
            print('[INFO:] Evicting ' .. url..'/'..resident[partIndex].fileName .. ' from cache.')
         end
         if slowFS.evict(url .. '/' .. resident[partIndex].fileName) then
            os.remove(resident[partIndex].indexPath)
         end
      end
      resident[partIndex] = nil
   end

   local function reset()
      if importer then
         local part = importer:join()
         resident[part.idx] = part
         importer = nil
      end
      totalItems = #partitionFiles
      -- evict all the resident files
      for i,v in pairs(resident) do
         doneWithPart(i)
      end
   end

   local function itemsInPart(partIndex)
      if not resident[partIndex] then
         assert(importer, "importer must be initialized")
         -- finish the import
         local part = finishFileImport()
         if verbose then
            print('[INFO:] Finishing import of ' .. url..'/'..part.fileName .. ' into cache.')
         end
         assert(partIndex == part.idx)
         resident[partIndex] = part
         -- update the total items we know about so far
         totalItems = totalItems - 1 + resident[partIndex].itemCount
         -- start the next import if more parts in queue
         if #queue > 0 then
            startNextImport()
         else -- we're done, set importer to nil
            importer = nil
         end
      end
      return resident[partIndex].itemCount
   end

   local function itemCount()
      return totalItems
   end

   -- needed for tests...
   local function partFileName(partIndex)
      assert(resident[partIndex], "part index needs to be resident")
      return resident[partIndex].fileName
   end

   return {
      itemCount = itemCount,
      itemAt = itemAt,
      itemsInPart = itemsInPart,
      partFileName = partFileName,
      labelIndex = { },
      numFiles = numFiles,
      reset = reset,
      doneWithPart = doneWithPart,
      addPartIndex = addPartIndex,
      indexType = 'SlowFS'
   }
end

return IndexSlowFS
