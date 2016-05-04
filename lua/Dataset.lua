local Batch = require 'dataset.Batch'
local Index = require 'dataset.Index'
local Reader = require 'dataset.Reader'
local Getters = require 'dataset.Getters'
local Sampler = require 'dataset.Sampler'
local SlowFS = require 'dataset.SlowFS'
local StreamedDataset = require 'dataset.StreamedDataset'

local function Dataset(indexURL, opt)
   opt = opt or { }
   if type(indexURL) == 'string' and SlowFS.find(indexURL) ~= nil and opt.streaming == true then
      return StreamedDataset(indexURL, opt)
   end
   local partition = opt.partition or 1
   local partitions = opt.partitions or 1
   -- See if this is a table of urls and not a table of tensors
   if type(indexURL) ~= 'table' or torch.typename(indexURL.x) ~= nil then
      indexURL = { indexURL }
   end
   local index = { }
   for i,url in ipairs(indexURL) do
      table.insert(index, Index(url, partition, partitions, opt))
   end

   local function sampledBatcher(opt)
      opt = opt or { }
      local numBuffers = opt.numBuffers or 2
      local buffers = { }
      for _ = 1,numBuffers do
         table.insert(buffers, { batch = Batch(opt) })
      end
      local getBatchDims = opt.getBatchDims

      local function _tableize(t, force)
         if t then
            if type(t) ~= 'table' or force then
               return { t }
            end
            return t
         end
      end

      local function _nitem(item, n)
         local ret = { }
         for i = 1,n do
            ret[i] = item
         end
         return ret
      end

      local samplerKind = _tableize(opt.samplerKind) or _nitem('uniform', #indexURL)
      assert(#samplerKind == #indexURL, 'expected one samplerKind for every indexURL')
      local samplerLabel = _tableize(opt.samplerLabel)
      local samplerOptions = _tableize(opt.samplerOptions, type(opt.samplerOptions) == 'table' and #opt.samplerOptions == 0)
      local sampler = { }
      local resetSampler = { }
      local get = _tableize(opt.get)
      if get then
         assert(#get == #indexURL, 'expected one get function for every indexURL')
      else
         get = { }
      end
      for i = 1,#indexURL do
         local sampleri,resetSampleri = Sampler(samplerKind[i], index[i], samplerLabel and samplerLabel[i], samplerOptions and samplerOptions[i])
         sampler[i] = sampleri
         resetSampler[i] = resetSampleri
         if not get[i] then
            get[i] = Getters(index[i].urlPrefix, index[i].indexType)
         end
      end
      local reader = Reader(sampler, {
         get = get,
         processor = opt.processor,
         processorOpt = opt.processorOpt,
         verbose = opt.verbose,
         poolSize = opt.poolSize or (numBuffers * opt.batchSize),
      })

      local batchSize = buffers[1].batch.batchSize
      local currentIdx = 0
      local bufferInUse

      local function numBatches()
         return math.ceil(index[1].itemCount(samplerLabel and samplerLabel[1]) / batchSize)
      end

      local function getBuffer(idx)
         return buffers[(idx % numBuffers) + 1]
      end

      local function startBatch(idx)
         local buffer = getBuffer(idx)
         assert(buffer.running == nil)
         buffer.batch.reset(getBatchDims and getBatchDims(idx))
         buffer.running = reader.startBatch(buffer.batch.batchSize, buffer.batch.readerInput)
         assert(buffer.running ~= nil)
      end

      local function startBatches(idx)
         while idx <= numBatches() do
            local buffer = getBuffer(idx)
            if buffer.running == nil then
               startBatch(idx)
            else
               break
            end
            idx = idx + 1
         end
      end

      local function finishBatch(idx, doNotBlock)
         local buffer = getBuffer(idx)
         assert(buffer.running ~= nil)
         local items = reader.finishBatch(buffer.running, doNotBlock)
         buffer.batch.finish(#items)
         buffer.batch.item = items
         for i,item in ipairs(items) do
            buffer.batch.index[i] = ((idx - 1) * batchSize) + item.index
            buffer.batch.class[i] = item.label
            buffer.batch.target[i] = index[1].labelIndex[item.label] or 0  -- unlabeled datasets?
         end
         bufferInUse = buffer
         return buffer.batch
      end

      local function getBatch(notUsed, doNotBlock)
         -- last buffer is now free
         if bufferInUse then
            assert(bufferInUse.running ~= nil)
            bufferInUse.running = nil
         end
         -- first round pays the full price
         currentIdx = currentIdx + 1
         if currentIdx == 1 and getBuffer(currentIdx).running == nil then
            startBatches(currentIdx)
         end
         local ret = finishBatch(currentIdx, doNotBlock)
         if currentIdx == numBatches() then
            -- we just got the last batch, tell the sampler to reset for next time
            for _,rs in pairs(resetSampler) do
               rs()
            end
            currentIdx = 0
         end
         -- all subsequent rounds should be free
         startBatches(currentIdx + 1)
         return ret
      end

      local function reset()
         -- block on and reset all prefetched batches
         reader.reset()
         for i = 1,numBuffers do
            buffers[i].running = nil
         end
         bufferInUse = nil
         currentIdx = 0
         for _,rs in pairs(resetSampler) do
            rs()
         end
      end

      return getBatch, numBatches, reset
   end

   return {
      sampledBatcher = sampledBatcher,
      index = (#index == 1 and index[1]) or index,
   }
end

return Dataset
