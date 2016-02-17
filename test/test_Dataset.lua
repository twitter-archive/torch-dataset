local test = require 'regress'
local Dataset = require 'dataset.Dataset'
local paths = require 'paths'
local TestUtils = require './TestUtils'

local function getNumBatches(getBatch, numBatches)
   local x = 0
   local b = 1
   while b <= numBatches() do
      local batch = getBatch()
      test.mustBeTrue(batch.batchSize == 1 and batch.batchSize == batch.input:size(1), 'batch.batchSize='..batch.batchSize..' and batch.input:size(1)='..batch.input:size(1))
      for i = 1,batch.batchSize do
         x = x + 1
      end
      b = b + 1
   end
   return x
end

local function verifyNumItemsInDataset(partition, partitions, batchSize)
   local dataset = Dataset(TestUtils.localHdfsPath, {
      partition = partition,
      partitions = partitions })
   local getBatch, numBatches = dataset.sampledBatcher({
      batchSize = batchSize,
      inputDims = { 1 },
      samplerKind = 'part-linear',
      samplerLabel = '*',
      verbose =true
   })
   local x = getNumBatches(getBatch, numBatches)

   local expectedItems = 0
   for i = 1, dataset.index.numFiles do
      expectedItems = expectedItems + dataset.index.itemsInPart(i)
   end

   test.mustBeTrue(x == expectedItems, 'must see '..expectedItems..' items not '..x)
end

test {
   testEmptyBatches = function()
      local dataset = Dataset(paths.concat(paths.dirname(paths.thisfile()), 'index4.csv'))
      local function get(url, offset, length)
         return url
      end
      local function pass(n)
         return n ~= 3 and n ~= 4 and n ~= 7 and n ~= 8
      end
      local function processor(res, processorOpt, input)
         local n = tonumber(res)
         if processorOpt.pass(n) then
            input:fill(n)
            return true
         end
      end
      local getBatch, numBatches = dataset.sampledBatcher({
         batchSize = 2,
         inputDims = { 1 },
         samplerKind = 'linear',
         get = get,
         processor = processor,
         processorOpt = {
            pass = pass,
         },
      })
      local x = 1
      for b = 1,numBatches() do
         local batch = getBatch()
         test.mustBeTrue((batch.batchSize == 0 and batch.input:nDimension() == 0) or batch.batchSize == batch.input:size(1), 'Invalid batchSize or batch.input.size()')
         local y = x
         for i = 1,batch.batchSize do
            local input = batch.input[i]
            test.mustBeTrue(input:size(1) == 1, 'input:size(1)='..input:size(1)..' not 1')
            for j = 1,input:size(1) do
               test.mustBeTrue(input[j] == y, 'input[j]='..input[j]..' not '..y)
               test.mustBeTrue(pass(y), 'should always see passing numbers')
            end
            y = y + 1
         end
         x = x + 2
      end
   end,


   testFrequentFailures = function()
      local dataset = Dataset(paths.concat(paths.dirname(paths.thisfile()), 'index3.csv'))
      local function get(url, offset, length)
         return url
      end
      local function pass(n)
         return (n - 1) % 2 == 0
      end
      local function processor(res, processorOpt, input)
         local n = tonumber(res)
         if processorOpt.pass(n) then
            input:fill(n)
            return true
         end
      end
      local indexForValue = { }
      for i = 1,dataset.index.itemCount() do
         local y = dataset.index.itemAt(i)
         local x = tonumber(y)
         if pass(x) == true then
            indexForValue[x] = i
         end
      end
      local getBatch, numBatches = dataset.sampledBatcher({
         batchSize = 3,
         inputDims = { 1 },
         samplerKind = 'linear',
         get = get,
         processor = processor,
         processorOpt = {
            pass = pass,
         },
      })
      for b = 1,numBatches() do
         local batch = getBatch()
         if batch.batchSize > 0 then
            test.mustBeTrue(batch.batchSize == batch.input:size(1), 'Invalid batchSize and batch.input.size()')
            test.mustBeTrue(batch.batchSize == batch.target:size(1), 'Invalid batchSize and batch.target.size()')
            test.mustBeTrue(batch.batchSize == batch.index:size(1), 'Invalid batchSize and batch.index.size()')
            test.mustBeTrue(batch.batchSize == #batch.item, 'Invalid batchSize and #batch.item')
            test.mustBeTrue(batch.batchSize == #batch.class, 'Invalid batchSize and #batch.class = '..#batch.class)
            for i = 1,batch.batchSize do
               local x = batch.input[i][1]
               test.mustBeTrue(pass(x), 'should always see passing numbers')
               test.mustBeTrue(batch.item[i].label == batch.class[i], 'item.label and batch.class must match')
               test.mustBeTrue(dataset.index.labelIndex[batch.item[i].label] == batch.target[i], 'item.label index and batch.target must match')
               test.mustBeTrue(batch.index[i] >= indexForValue[x], 'batch.index is wrong batch.index[i]='..batch.index[i]..' expected='..indexForValue[x])
            end
         end
      end
   end,

   testVaryingBatchDims = function()
      local dataset = Dataset(paths.concat(paths.dirname(paths.thisfile()), 'index4.csv'))
      local function get(url, offset, length)
         return url
      end
      local function processor(res, processorOpt, input)
         input:fill(tonumber(res))
         return true
      end
      local function getBatchDims(idx)
         return { ((idx - 1) % 2) + 1 }
      end
      local getBatch, numBatches = dataset.sampledBatcher({
         batchSize = 4,
         inputDims = { 1 },
         samplerKind = 'linear',
         get = get,
         processor = processor,
         getBatchDims = getBatchDims,
      })
      local x = 1
      for b = 1,numBatches() do
         local batch = getBatch()
         test.mustBeTrue(batch.batchSize > 0 and batch.batchSize == batch.input:size(1), 'batch.batchSize='..batch.batchSize..' and batch.input:size(1)='..batch.input:size(1))
         for i = 1,batch.batchSize do
            local input = batch.input[i]
            local e = (((b - 1) % 2) + 1)
            test.mustBeTrue(input:size(1) == e, 'input:size(1)='..input:size(1)..' not '..e)
            for j = 1,input:size(1) do
               test.mustBeTrue(input[j] == x, 'input[j]='..input[j]..' not '..x)
            end
            x = x + 1
         end
      end
   end,

   testVaryingProcessorDims = function()
      local dataset = Dataset(paths.concat(paths.dirname(paths.thisfile()), 'index4.csv'))
      local function get(url, offset, length)
         return url
      end
      local function processor(res, processorOpt, input)
         local n = tonumber(res)
         input:resize(n)
         input:fill(n)
         return true
      end
      local getBatch, numBatches = dataset.sampledBatcher({
         batchSize = 1,
         inputDims = { 1 },
         samplerKind = 'linear',
         get = get,
         processor = processor,
      })
      for epoch = 1,3 do
         local x = 1
         for b = 1,numBatches() do
            local batch = getBatch()
            test.mustBeTrue(batch.batchSize > 0 and batch.batchSize == batch.input:size(1), 'batch.batchSize='..batch.batchSize..' and batch.input:size(1)='..batch.input:size(1))
            for i = 1,batch.batchSize do
               local input = batch.input[i]
               test.mustBeTrue(input:size(1) == x, 'input:size(1)='..input:size(1)..' not '..x)
               for j = 1,input:size(1) do
                  local y = input[j]
                  test.mustBeTrue(torch.isTensor(y) == false, 'messed up at epoch '..epoch)
                  test.mustBeTrue(input[j] == x, 'input[j]='..input[j]..' not '..x)
               end
               x = x + 1
            end
         end
      end
   end,

   testMultiFileDataset = function()
      local dataset = Dataset(paths.concat(paths.dirname(paths.thisfile()), 'index-files.csv'))
      local function textProcessor(res, processorOpt, input)
         local tensor = torch.FloatTensor(input:size(1))
         tensor:fill(0)
         for i = 1,#res do
            tensor[i] = string.byte(string.sub(res, i, i))
         end
         input:copy(tensor)
         return true
      end
      local getBatch, numBatches = dataset.sampledBatcher({
         batchSize = 4,
         inputDims = { 4 },
         samplerKind = 'linear',
         processor = textProcessor
      })
      local expected = {
         { label = 'A', tensor = { 65, 66, 0, 0 } },
         { label = 'A', tensor = { 69, 70, 0, 0 } },
         { label = 'A', tensor = { 83, 84, 0, 0 } },
         { label = 'B', tensor = { 76, 77, 78, 0 } },
         { label = 'B', tensor = { 81, 82, 83, 0 } },
         { label = 'C', tensor = { 73, 74, 75, 76 } },
      }
      local m = 0
      for n = 1,5 do
         local batch = getBatch()
         test.mustBeTrue(batch.input:size(1) == batch.batchSize, 'batchSize must be ' .. batch.batchSize)
         for i = 1,batch.batchSize do
            local j = (m % #expected) + 1
            m = m + 1
            test.mustBeTrue(batch.class[i] == expected[j].label, 'labels do not match, saw "' .. batch.class[i] .. '" expected "' .. expected[j].label)
            for k = 1,4 do
               local x = batch.input[i][k]
               local y = expected[j].tensor[k]
               test.mustBeTrue(x == y, 'tensors do not match at position ' .. k .. ' saw ' .. x .. ' expected ' .. y)
            end
         end
      end
   end,

   testMoreThanTwoBuffers = function()
      local dataset = Dataset(paths.concat(paths.dirname(paths.thisfile()), 'index4.csv'))
      local function get(url, offset, length)
         return url
      end
      local function processor(res, processorOpt, input)
         input:fill(tonumber(res))
         return true
      end
      local function getBatchDims(idx)
         return { ((idx - 1) % 2) + 1 }
      end
      local getBatch, numBatches = dataset.sampledBatcher({
         numBuffers = 3,
         batchSize = 4,
         inputDims = { 1 },
         samplerKind = 'linear',
         get = get,
         processor = processor,
         getBatchDims = getBatchDims,
      })
      local x = 1
      for b = 1,numBatches() do
         local batch = getBatch()
         test.mustBeTrue(batch.batchSize > 0 and batch.batchSize == batch.input:size(1), 'batch.batchSize='..batch.batchSize..' and batch.input:size(1)='..batch.input:size(1))
         for i = 1,batch.batchSize do
            local input = batch.input[i]
            local e = (((b - 1) % 2) + 1)
            test.mustBeTrue(input:size(1) == e, 'input:size(1)='..input:size(1)..' not '..e)
            for j = 1,input:size(1) do
               test.mustBeTrue(input[j] == x, 'input[j]='..input[j]..' not '..x)
            end
            x = x + 1
         end
      end
   end,

   testLoopingAndManualResets = function()
      local dataset = Dataset(paths.concat(paths.dirname(paths.thisfile()), 'index4.csv'))
      local function get(url, offset, length)
         return url
      end
      local function processor(res, processorOpt, input)
         input:fill(tonumber(res))
         return true
      end
      local getBatch, numBatches, reset = dataset.sampledBatcher({
         batchSize = 1,
         inputDims = { 1 },
         samplerKind = 'linear',
         get = get,
         processor = processor,
      })
      test.mustBeTrue(42 > numBatches(), '42 must be greater than batchSize')
      for b = 1,10*numBatches() do
         local batch = getBatch()
         if b % 42 == 0 then
            reset()
         end
         test.mustBeTrue(batch.batchSize == 1, 'batch.batchSize must be 1')
         test.mustBeTrue(batch.input:size(1) == 1, 'batch.input:size(1) must be 1')
         for i = 1,batch.batchSize do
            local input = batch.input[i]
            for j = 1,input:size(1) do
               local y = input[j]
               test.mustBeTrue(torch.isTensor(y) == false, 'messed up at epoch '..(b/numBatches()))
            end
         end
      end
   end,

   testMultipleIndexes = function()
      local dataset = Dataset({
         paths.concat(paths.dirname(paths.thisfile()), 'index1.csv'),
         paths.concat(paths.dirname(paths.thisfile()), 'index1.csv'),
         paths.concat(paths.dirname(paths.thisfile()), 'index4.csv')
      })
      local function get(url, offset, length)
         return url
      end
      local function processor(res1, res2, res3, processorOpt, input)
         input[1] = tonumber(res1)
         input[2] = tonumber(res2)
         input[3] = tonumber(res3)
         return true
      end
      local getBatch, numBatches, reset = dataset.sampledBatcher({
         batchSize = 1,
         inputDims = { 3 },
         samplerKind = { 'linear', 'linear', 'uniform' },
         get = { get, get, get },
         processor = processor,
      })
      for b = 1,10*numBatches() do
         local batch = getBatch()
         test.mustBeTrue(batch.batchSize == 1, 'batch.batchSize must be 1 not '..batch.batchSize)
         test.mustBeTrue(batch.input:size(1) == 1, 'batch.input:size(1) must be 1')
         for i = 1,batch.batchSize do
            local input = batch.input[i]
            test.mustBeTrue(input[1] >= 1 or input[1] <= 20, 'expected [1..20] got '..input[1])
            test.mustBeTrue(input[2] >= 1 or input[2] <= 20, 'expected [1..20] got '..input[2])
            test.mustBeTrue(input[3] >= 1 or input[3] <= 9, 'expected [1..9] got '..input[3])
         end
         if b % 42 == 0 then
            reset()
         end
      end
   end,

   testExtraItem = function()
      local dataset = Dataset(paths.concat(paths.dirname(paths.thisfile()), 'index1.csv'))
      local function get(url, offset, length)
         return url
      end
      local function processor(res, processorOpt, input)
         input[1] = tonumber(res)
         local extra = torch.FloatTensor(3, 3)
         extra:fill(tonumber(res))
         return true, extra
      end
      local getBatch, numBatches, reset = dataset.sampledBatcher({
         batchSize = 1,
         inputDims = { 1 },
         samplerKind = 'linear',
         get = get,
         processor = processor,
      })
      for b = 1,10*numBatches() do
         local batch = getBatch()
         test.mustBeTrue(batch.batchSize == 1, 'batch.batchSize must be 1 not '..batch.batchSize)
         test.mustBeTrue(batch.input:size(1) == 1, 'batch.input:size(1) must be 1')
         for i = 1,batch.batchSize do
            local input = batch.input[i]
            test.mustBeTrue(input[1] >= 1 or input[1] <= 20, 'expected [1..20] got '..input[1])
            test.mustBeTrue(batch.item[1].extra:sum() == 9*input[1])
         end
         if b % 42 == 0 then
            reset()
         end
      end
   end,

   testHdfsDatasetOnePartition = function()
      verifyNumItemsInDataset(1, 1, 1)
   end,

   testHdfsDatasetOnePartPerPartition = function()
      verifyNumItemsInDataset(1, 4, 1)
      verifyNumItemsInDataset(2, 4, 1)
      verifyNumItemsInDataset(3, 4, 1)
      verifyNumItemsInDataset(4, 4, 1)
   end,

   testHdfsDatasetTwoPartPerPartition = function()
      verifyNumItemsInDataset(1, 2, 1)
      verifyNumItemsInDataset(2, 2, 1)
   end,

   testHdfsDatasetOddPartition = function()
      verifyNumItemsInDataset(1, 3, 1)
      verifyNumItemsInDataset(2, 3, 1)
      verifyNumItemsInDataset(3, 3, 1)
   end,

   testHdfsDatasetMultiEpoch = function()
      local dataset = Dataset(TestUtils.localHdfsPath)
      local getBatch, numBatches = dataset.sampledBatcher({
         batchSize = 1,
         inputDims = { 1 },
         samplerKind = 'part-linear',
         samplerLabel = '*',
         verbose =true
      })
      local x = 0
      local numEpochs = 3
      for e = 1, numEpochs do
         local b = 1
         while b <= numBatches() do
            local batch = getBatch()
            test.mustBeTrue(batch.batchSize == 1 and batch.batchSize == batch.input:size(1), 'batch.batchSize='..batch.batchSize..' and batch.input:size(1)='..batch.input:size(1))
            for i = 1,batch.batchSize do
               x = x + 1
            end
            b = b + 1
         end
      end
      test.mustBeTrue(x == numEpochs * 78, 'must see '.. numEpochs * 78 .. ' items not '..x)
   end,

   testHdfsDatasetMultiEpochBatch6 = function()
      local batchSize = 6
      local dataset = Dataset(TestUtils.localHdfsPath)
      local getBatch, numBatches = dataset.sampledBatcher({
         batchSize = batchSize,
         inputDims = { 1 },
         samplerKind = 'part-linear',
         samplerLabel = '*',
         verbose =true
      })
      local x = 0
      local numEpochs = 10
      for e = 1, numEpochs do
         local b = 1
         while b <= numBatches() do
            local batch = getBatch()
            test.mustBeTrue(batch.batchSize == batchSize and batch.batchSize == batch.input:size(1), 'batch.batchSize='..batch.batchSize..' and batch.input:size(1)='..batch.input:size(1))
            for i = 1,batch.batchSize do
               x = x + 1
            end
            b = b + 1
         end
      end
      test.mustBeTrue(x == numEpochs * 78, 'must see '.. numEpochs * 78 .. ' items not '..x)
   end,

   testHdfsDatasetMultiEpochItemContent = function()
      local dataset = Dataset(TestUtils.localHdfsPath)
      local getBatch, numBatches, reset, dumpStats = dataset.sampledBatcher({
         batchSize = 1,
         inputDims = { 1 },
         samplerKind = 'part-linear',
         samplerLabel = '*',
         verbose = true,
         processor = function(res, opt, input)
                        local buffer = torch.ByteTensor(torch.ByteStorage():string(res))
                        input:resize(#res):copy(buffer)
                        return true
                     end
      })

      local expectedLines = TestUtils.allLines
      local resultLines = { }

      local x = 0
      local numEpochs = 3
      for e = 1, numEpochs do
         local b = 1
         while b <= numBatches() do
            local batch = getBatch()
            local input = batch.input[1]

            local s = input:byte():storage():string()
            table.insert(resultLines, s)

            b = b + 1
         end

         table.sort(expectedLines)
         table.sort(resultLines)
         test.mustBeTrue(TestUtils.listEquals(resultLines, expectedLines) == true, 'the lists must have the same sorted orders')

         --reset
         resultLines = { }
      end
   end,

   testHdfsDatasetMultiEpochOneFileOddSize = function()
      local dataset = Dataset(TestUtils.localHdfsPath)
      local getBatch, numBatches, reset, dumpStats = dataset.sampledBatcher({
         batchSize = 5,
         inputDims = { 1 },
         samplerKind = 'part-linear',
         samplerLabel = '*',
         verbose = true
      })

      local resultLines = { }

      local x = 0
      local numEpochs = 3
      for e = 1, numEpochs do
         local b = 1
         while b <= numBatches() do
            local batch = getBatch()
            local input = batch.input[1]

            x = x + 1
            b = b + 1
         end
         test.mustBeTrue(x == 16, "number of items must be 16, but was " .. x)

         --reset
         x = 0
         resultLines = { }
      end
   end,

   testHdfsDatasetMultiEpochPartPermutationItemContent = function()
      local dataset = Dataset(TestUtils.localHdfsPath)
      local getBatch, numBatches, reset, dumpStats = dataset.sampledBatcher({
         batchSize = 1,
         inputDims = { 1 },
         samplerKind = 'part-linear-permutation',
         samplerLabel = '*',
         verbose = true,
         processor = function(res, opt, input)
                        local buffer = torch.ByteTensor(torch.ByteStorage():string(res))
                        input:resize(#res):copy(buffer)
                        return true
                     end
      })

      local expectedLines = TestUtils.allLines
      local prev
      local seen = { }

      local x = 0
      local numEpochs = 5
      for e = 1, numEpochs do
         local b = 1
         while b <= numBatches() do
            local batch = getBatch()
            local input = batch.input[1]
            local s = input:byte():storage():string()
            table.insert(seen, s)

            x = x + 1
            b = b + 1
         end

         test.mustBeTrue(x == 78, 'must see 78 items, instead saw ' .. x)
         if prev ~= nil then
            test.mustBeTrue(TestUtils.listEquals(seen, prev) == false, 'the lists must have different orders')

            local sortedSeen = TestUtils.listCopy(seen)
            table.sort(sortedSeen)
            local sortedPrev = TestUtils.listCopy(prev)
            table.sort(sortedPrev)
            test.mustBeTrue(TestUtils.listEquals(sortedSeen, sortedPrev) == true, 'the lists must have the same sorted orders')
         end
         -- reset
         prev = seen
         seen = {}
         x = 0
      end
   end,

   testHdfsDatasetMultiEpochPartPermutationPermutationItemContent = function()
      local dataset = Dataset(TestUtils.localHdfsPath)
      local getBatch, numBatches, reset, dumpStats = dataset.sampledBatcher({
         batchSize = 1,
         inputDims = { 1 },
         samplerKind = 'part-permutation-permutation',
         samplerLabel = '*',
         verbose = true,
         processor = function(res, opt, input)
                        local buffer = torch.ByteTensor(torch.ByteStorage():string(res))
                        input:resize(#res):copy(buffer)
                        return true
                     end
      })

      local expectedLines = TestUtils.allLines
      local seen = { }

      local x = 0
      local numEpochs = 10
      for e = 1, numEpochs do
         local b = 1
         while b <= numBatches() do
            local batch = getBatch()
            local input = batch.input[1]
            local s = input:byte():storage():string()
            table.insert(seen, s)

            x = x + 1
            b = b + 1
         end

         test.mustBeTrue(x == 78, 'must see 78 items, instead saw ' .. x)
         if prev ~= nil then
            test.mustBeTrue(TestUtils.listEquals(seen, prev) == false, 'the lists must have different orders')

            local sortedSeen = TestUtils.listCopy(seen)
            table.sort(sortedSeen)
            local sortedPrev = TestUtils.listCopy(prev)
            table.sort(sortedPrev)
            test.mustBeTrue(TestUtils.listEquals(sortedSeen, sortedPrev) == true, 'the lists must have the same sorted orders')
         end
         -- reset
         prev = seen
         seen = {}
         x = 0
      end
   end,
}
