local Cache = require 'dataset.Cache'
local SlowFS = require 'dataset.SlowFS'
local SlowFSStream = require 'dataset.SlowFSStream'

local function StreamedDataset(indexURL, opt)

   opt = opt or { }
   local partition = opt.partition or 1
   local partitions = opt.partitions or 1
   local cache = Cache(opt)
   local slowFS = SlowFS.find(indexURL)(cache, opt)

   local function downsample(t, partition, partitions)
      assert(partition >= 1)
      assert(partitions >= 1)
      assert(partition <= partitions)
      assert(#t >= partitions, 'number of part files ('..#t..') must be >= number of partitions ('..partitions..'): '..indexURL)

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

   local parts = opt.parts or slowFS.parts(indexURL)
   local partitionParts = downsample(parts, partition, partitions)
   while opt.maxParts and #partitionParts > opt.maxParts do
      table.remove(partitionParts)
   end

   local function sampledBatcher(opt)
      opt = opt or { }
      assert(opt.samplerKind == 'part-linear', 'streaming only supports part-linear sampling')
      local batchSize = opt.batchSize or 1
      local get = opt.get or error('a get function is required')
      local processor = opt.processor or error('a processor function is required')
      local processorOpt = opt.processorOpt or { }
      local slowFSStream = SlowFSStream(slowFS, partitionParts, opt)
      local hasMore

      local reuse = { }
      for i = 1,batchSize do
         reuse[i] = {
            keys = torch.LongTensor(1),
            values = torch.FloatTensor(1),
            codes = torch.ByteTensor(1),
            labels = torch.LongTensor(1),
            weights = torch.FloatTensor(1),
         }
      end

      local function getBatch()
         assert(hasMore ~= nil, 'you must call numBatches before calling getBatch')
         local batch = {
            keys = { },
            values = { },
            labels = { },
            weights = { },
         }
         local i = 1
         local j = 1
         while j <= batchSize and hasMore == true do
            local ok1, ok2, res = pcall(get, slowFSStream)
            if not ok1 then
               io.stderr:write('StreamedDataset.get failed: '..tostring(ok2)..'\n')
            elseif ok2 then
               if reuse[i].keys:nDimension() == 0 then
                  reuse[i].keys:resize(1)
                  reuse[i].values:resize(1)
                  reuse[i].codes:resize(1)
                  reuse[i].labels:resize(1)
                  reuse[i].weights:resize(1)
               end
               local ok1, ok2 = pcall(processor, res, reuse[i], processorOpt)
               if not ok1 then
                  io.stderr:write('StreamedDataset.processor failed: '..tostring(ok2)..'\n')
               elseif ok2 then
                  batch.keys[i] = reuse[i].keys
                  batch.values[i] = reuse[i].values
                  batch.labels[i] = reuse[i].labels
                  batch.weights[i] = reuse[i].weights
                  i = i + 1
               end
            end
            hasMore = slowFSStream.next()
            j = j + 1
         end
         batch.batchSize = i - 1
         return batch
      end

      local function numBatches()
         if hasMore == nil then
            hasMore = slowFSStream.next()
         end
         return (hasMore and math.huge) or 0
      end

      local function reset()
         slowFSStream.reset("TERM")
         hasMore = true
      end

      return getBatch, numBatches, reset
   end

   return {
      sampledBatcher = sampledBatcher,
   }
end

return StreamedDataset
