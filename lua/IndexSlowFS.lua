local ipc = require 'libipc'
local Cache = require 'dataset.Cache'
local SlowFS = require 'dataset.SlowFS'

local function IndexSlowFS(url, partition, partitions, opt)

   local partition = partition or 1
   assert(partition >= 1)
   local partitions = partitions or 1
   assert(partitions >= 1)
   assert(partition <= partitions)
   local opt = opt or { }
   local maxParts = opt.maxParts
   local verbose = opt.verbose or false
   local cache = Cache(opt)
   local slowFS = SlowFS.find(url)(cache, opt)

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

      for i,part in ipairs(parts) do
         if maxParts and i > maxParts then break end
         addFile(files, part:sub(#root + 2))
      end

      return files
   end

   local function downsample(t, partition, partitions)
      assert(partition >= 1)
      assert(partitions >= 1)
      assert(partition <= partitions)
      assert(#t >= partitions, 'number of part files ('..#t..') must be >= number of partitions ('..partitions..'): '..url)

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
      local slowFS = SlowFS(cache, opt)
      local fileURL = url .. '/' .. fileName
      local fpath = slowFS.get(fileURL)
      local dataset = require 'libdataset'
      local offsets = dataset.offsets(fpath)
      return {
         url = url,
         fileName = fileName,
         filePath = fpath,
         itemCount = offsets:size(1) - 1,
         offsets = offsets,
         idx = idx,
      }
   end

   ------------ Index access --------------

   -- item at the actual part index
   -- return filePath, offset, length of the item
   local function innerItemAt(offsets, fp, fileName, url)
      return function(i)
         local offset = offsets[i]
         local length = offsets[i + 1] - offset
         return fp, offset, length
      end
   end

   ------------ File import and cache  --------------

   local function startNextImport()
      assert(#queue > 0, "must have at least one file to import")
      local idx = table.remove(queue, 1)
      if verbose then
         io.stderr:write("[INFO:] Starting import of file " .. url..'/'..partitionFiles[idx]..'\n')
      end
      importer = ipc.map(1, makeFileIndex, partitionFiles[idx], url, opt, idx, SlowFS.find(url))
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
      partIndex.itemAt = innerItemAt(partIndex.offsets,
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
            io.stderr:write('[INFO:] Evicting ' .. url..'/'..resident[partIndex].fileName .. ' from cache.\n')
         end
         slowFS.evict(url .. '/' .. resident[partIndex].fileName)
      end
      resident[partIndex] = nil
   end

   local function reset()
      if importer then
         local part = finishFileImport()
         resident[part.idx] = part
         importer = nil
      end
      totalItems = #partitionFiles
      -- evict all the resident files
      local keys = { }
      for i,v in pairs(resident) do
         table.insert(keys, i)
      end
      for _,i in ipairs(keys) do
         doneWithPart(i)
      end
   end

   local function itemsInPart(partIndex)
      if not resident[partIndex] then
         assert(importer, "importer must be initialized")
         -- finish the import
         local part = finishFileImport()
         if verbose then
            io.stderr:write('[INFO:] Finishing import of ' .. url..'/'..part.fileName .. ' into cache.\n')
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
