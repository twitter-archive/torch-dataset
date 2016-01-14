local test = require 'regress'
local IndexCSV = require 'dataset.IndexCSV'
local IndexDirectory = require 'dataset.IndexDirectory'
local IndexSlowFS = require 'dataset.IndexSlowFS'
local IndexTensor = require 'dataset.IndexTensor'
local paths = require 'paths'
local TestUtils = require './TestUtils'
local Getters = require 'dataset.Getters'
local SlowFS = require 'dataset.SlowFS'

local function verify(index, items, label)
   local printLabel = label or 'nil'
   local c = index.itemCount(label)
   test.mustBeTrue(c == #items, 'class '..printLabel..' must have '..#items..' saw '..c)
   table.sort(items)
   for i,v in ipairs(items) do
      local x,l = index.itemAt(i, label)
      test.mustBeTrue(x == v, 'class '..printLabel..' must have '..v..' at '..i..' saw '..x)
      test.mustBeTrue(l == label or (label == nil and l == ''), 'class '..printLabel..' must see returned label of '..printLabel..' saw '..(l or 'nil'))
   end
end

local function mustFail(f, m)
   ok = pcall(f)
   test.mustBeTrue(ok == false, m)
end

test {
   testLoad = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'))
      test.mustBeTrue(#index.labels == 3, '#labels must be 3')
      test.mustBeTrue(index.labelIndex['A'] == 1, 'label index of A must be 1')
      test.mustBeTrue(index.labelIndex['B'] == 2, 'label index of B must be 2')
      test.mustBeTrue(index.labelIndex['C'] == 3, 'label index of C must be 3')
      test.mustBeTrue(index.itemCount() == 25, '#items must be 25')
      verify(index, { '1', '2', '3', '5', '6', '9', '10', '11', '12', '13', '14', '16', '18', '19', '20' }, 'A')
      verify(index, { '3', '4', '7', '8', '9', '13', '14', '15', '17' }, 'B')
      verify(index, { '9' }, 'C')
      mustFail(function() index.itemAt(-13) end, 'negative indexes should be illegal')
      mustFail(function() index.itemAt(0) end, 'zero index should be illegal')
      mustFail(function() index.itemAt(26) end, 'too large indexes should be illegal')
      mustFail(function() index.itemAt(-13, 'B') end, 'negative indexes should be illegal for classes')
      mustFail(function() index.itemAt(0, 'B') end, 'zero index should be illegal for classes')
      mustFail(function() index.itemAt(10, 'B') end, 'too large indexes should be illegal for classes')
   end,


   testLoadPartion = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'), 1, 5)
      test.mustBeTrue(#index.labels == 3, '#labels must be 3')
      test.mustBeTrue(index.labelIndex['A'] == 1, 'label index of A must be 1')
      test.mustBeTrue(index.labelIndex['B'] == 2, 'label index of B must be 2')
      test.mustBeTrue(index.labelIndex['C'] == 3, 'label index of C must be 3')
      test.mustBeTrue(index.itemCount() == 4, '#items must be 4 not '..index.itemCount())
      verify(index, { '13', '2', '9' }, 'A')
      verify(index, { '3' }, 'B')
      verify(index, { }, 'C')
      mustFail(function() index.itemAt(-13) end, 'negative indexes should be illegal')
      mustFail(function() index.itemAt(0) end, 'zero index should be illegal')
      mustFail(function() index.itemAt(7) end, 'too large indexes should be illegal')
      mustFail(function() index.itemAt(-13, 'B') end, 'negative indexes should be illegal for classes')
      mustFail(function() index.itemAt(0, 'B') end, 'zero index should be illegal for classes')
      mustFail(function() index.itemAt(3, 'B') end, 'too large indexes should be illegal for classes')
   end,

   testAllPartions = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'))
      local c = 0
      for p = 1,3 do
         local subIndex = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'), p, 3)
         c = c + subIndex.itemCount()
      end
      test.mustBeTrue(index.itemCount() == c, 'total partitioned #items must be '..index.itemCount()..' not '..c)
   end,

   testLoadNoLabels = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'nolabels.csv'))
      test.mustBeTrue(#index.labels == 0, '#labels must be 0')
      test.mustBeTrue(index.labelIndex['*'] == nil, 'label index of * must be nil')
      test.mustBeTrue(index.itemCount() == 8, '#items must be 8 not '..index.itemCount())
      verify(index, { '1', '2', '3', '4', '5', '6', '7', '8' })
      mustFail(function() index.itemAt(-13) end, 'negative indexes should be illegal')
      mustFail(function() index.itemAt(0) end, 'zero index should be illegal')
      mustFail(function() index.itemAt(9) end, 'too large indexes should be illegal')
   end,

   testLoadMeta = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index2.csv'))
      test.mustBeTrue(#index.labels == 2, '#labels must be 2')
      test.mustBeTrue(index.itemCount() == 2, '#items must be 2 not '..index.itemCount())
      test.mustBeTrue(index.urlPrefix == 'http://prefix.com/go/here', 'wrong url prefix')
      test.mustBeTrue(index.itemAt(1) == 'http://prefix.com/go/here/1', 'did not properly concat url prefix to item')
   end,

   testUrlParams = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index2.csv'), nil, nil, { urlParams = { a = 1, b = 2, c = 'three', d = true } })
      test.mustBeTrue(#index.labels == 2, '#labels must be 2')
      test.mustBeTrue(index.itemCount() == 2, '#items must be 2 not '..index.itemCount())
      test.mustBeTrue(index.urlPrefix == 'http://prefix.com/go/here', 'wrong url prefix')
      test.mustBeTrue(index.itemAt(1) == 'http://prefix.com/go/here/1?a=1&d=true&c=three&b=2', 'did not properly concat url suffix to item')
   end,

   testDirectory = function()
      local p = paths.concat(paths.dirname(paths.thisfile()), 'files/local')
      local index = IndexDirectory(p)
      test.mustBeTrue(#index.labels == 2, '#labels must be 2')
      test.mustBeTrue(index.itemCount() == 5, '#items must be 2 not '..index.itemCount())
      verify(index, { p..'/cats/cat1.txt', p..'/cats/cat2.txt' }, 'cats')
      verify(index, { p..'/dogs/dog1.txt' }, 'dogs')
      test.mustBeTrue(index.urlPrefix == p, 'wrong url prefix')
   end,

   testSlowFSInitialLoad = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)
      test.mustBeTrue(index.itemCount() == 4, '#items must be 4 not '..index.itemCount())
      test.mustBeTrue(index.numFiles == 4, '#part files must be 4 not'..index.numFiles)
   end,

   testSlowFSAccessAll = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)
      -- force parsing of all files
      for i = 1,4 do
         index.addPartIndex(i)
      end
      for i = 1,4 do
         index.itemsInPart(i)
      end
      test.mustBeTrue(index.itemCount() == 78, 'total partitioned #items must be 78 not '..index.itemCount())
   end,

   testSlowFSAllPartitions = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)
      local c = 0
      local numPartitions = 2
      for i = 1,numPartitions do
         local subIndex = IndexSlowFS(TestUtils.localHdfsPath, i, numPartitions)
         c = c + subIndex.itemCount()
      end
      test.mustBeTrue(c == index.itemCount(), 'total partitioned #items must be '..index.itemCount()..' not '..c)
   end,

   testTensor = function()
      local x = torch.randn(100,3,3)
      local index = IndexTensor(x)
      test.mustBeTrue(index.itemCount() == 100, 'expected 100, saw '..index.itemCount())
      for i = 1,100 do
         local e = (x[i] - index.itemAt(i)):abs():sum()
         test.mustBeTrue(e == 0.0, 'nope at '..i..' error = '..e)
      end
      local y = torch.randn(100):apply(function(yy)
         if yy < 0 then
            return 1
         else
            return 2
         end
      end)
      local x = torch.randn(100,2,2)
      local index = IndexTensor({ x = x, y = y })
      test.mustBeTrue(index.itemCount() == 100, 'expected 100, saw '..index.itemCount())
      test.mustBeTrue(#index.labels == 2, 'expected 2, saw '..#index.labels)
      local c = index.itemCount(1)+index.itemCount(2)
      test.mustBeTrue(c == 100, 'expected 100, saw '..c)
      local j = 1
      local k = 1
      for i = 1,100 do
         if y[i] == 1 then
            local it,lb = index.itemAt(j, 1)
            local e = (x[i] - it):abs():sum()
            test.mustBeTrue(e == 0.0, 'nope at '..i..' error = '..e)
            test.mustBeTrue(lb == y[i], 'label mismatch at '..i)
            j = j + 1
         else
            local it,lb = index.itemAt(k, 2)
            local e = (x[i] - it):abs():sum()
            test.mustBeTrue(e == 0.0, 'nope at '..i..' error = '..e)
            test.mustBeTrue(lb == y[i], 'label mismatch at '..i)
            k = k + 1
         end
      end
   end,

   testTensorPartition = function()
      local x = torch.randn(100,3,3)
      local index = IndexTensor(x, 2, 10)
      test.mustBeTrue(index.itemCount() == 10, 'expected 10, saw '..index.itemCount())
      for i = 1,10 do
         local j = ((i-1)*10)+2
         local e = (x[j] - index.itemAt(i)):abs():sum()
         test.mustBeTrue(e == 0.0, 'nope at '..i..' error = '..e)
      end
      local y = torch.randn(100):apply(function(yy)
         if yy < 0 then
            return 1
         else
            return 2
         end
      end)
      local x = torch.randn(100,2,2)
      local index = IndexTensor({ x = x, y = y }, 1, 7)
      test.mustBeTrue(index.itemCount() == 15, 'expected 15, saw '..index.itemCount())
      test.mustBeTrue(#index.labels == 2, 'expected 2, saw '..#index.labels)
      local c = index.itemCount(1)+index.itemCount(2)
      test.mustBeTrue(c == 15, 'expected 15, saw '..c)
   end,

   testSlowFSPartIndexCount = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)
      local initialNumItems = index.itemCount()

      test.mustBeTrue(initialNumItems == 4, '#items for new index must be 4, instead got '..initialNumItems)

      for i = 1,4 do
         index.addPartIndex(i)
      end

      local itemsInPart1 = index.itemsInPart(1)
      local expectedItems1 = TestUtils.testFileSizes[index.partFileName(1)]

      test.mustBeTrue(itemsInPart1 == expectedItems1, '#items for new index must be '..expectedItems1..', instead got '..itemsInPart1)

      local itemsInPart2 = index.itemsInPart(2)
      local expectedItems2 = TestUtils.testFileSizes[index.partFileName(2)]

      test.mustBeTrue(itemsInPart2 == expectedItems2, '#items for new index must be '..expectedItems2..', instead got '..itemsInPart2)
   end,

   testSlowFSPartIndexItems = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)

      for i = 1,4 do
         index.addPartIndex(i)
      end

      index.itemsInPart(1)

      local expectedOffset1 = 0
      local expectedLength1 = 175
      local resPath1, resOffset1, resLength1 = index.itemAt(1, 1)

      test.mustBeTrue(expectedOffset1 == resOffset1, 'offset to first item in first part file didnt match:'..resOffset1)
      test.mustBeTrue(expectedLength1 == resLength1, 'length to first item in first part file didnt match:'..resLength1)

      local expectedOffset2 = 176
      local expectedLength2 = 105
      local resPath2, resOffset2, resLength2 = index.itemAt(1, 2)

      test.mustBeTrue(expectedOffset2 == resOffset2, 'offset to second item in first part file didnt match:'..resOffset2)
      test.mustBeTrue(expectedLength2 == resLength2, 'length to second item in first part file didnt match:'..resLength2)
   end,
}
