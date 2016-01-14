local test = require 'regress'
local IndexCSV = require 'dataset.IndexCSV'
local IndexSlowFS = require 'dataset.IndexSlowFS'
local Sampler = require 'dataset.Sampler'
local paths = require 'paths'
local TestUtils = require './TestUtils'

test {
   testPermutationSampler = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'))
      local sampler,resetSampler = Sampler('permutation', index)
      local seen = { }
      local labelCounts = { }
      local prev
      for i = 1,75 do
         local s,label = sampler()
         table.insert(seen, s)
         if labelCounts[label] == nil then
            labelCounts[label] = 1
         else
            labelCounts[label] = labelCounts[label] + 1
         end
         if i % 25 == 0 then
            test.mustBeTrue(sampler() == nil, 'going past the end must return nil')
            resetSampler()
            if prev ~= nil then
               test.mustBeTrue(#seen == #prev, 'need to see the same amount sampled each loop')
               test.mustBeTrue(TestUtils.listEquals(seen, prev) == false, 'the lists must have different orders')
               local sortedSeen = TestUtils.listCopy(seen)
               table.sort(sortedSeen)
               local sortedPrev = TestUtils.listCopy(prev)
               table.sort(sortedPrev)
               test.mustBeTrue(TestUtils.listEquals(sortedSeen, sortedPrev) == true, 'the lists must have the same sorted orders')
               for _,label in ipairs(index.labels) do
                  local x = index.itemCount(label)
                  local y = labelCounts[label]
                  test.mustBeTrue(x == y, 'must have the same distribution of '..label..' saw '..y..' expected '..x)
               end
            end
            prev = seen
            seen = { }
            labelCounts = { }
         end
      end
   end,

   testLabelPermutationSampler = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'))
      local sampler = Sampler('label-permutation', index)
      local seen = { }
      local seenClasses = { }
      local labelCounts = { }
      local labelCountsClasses = {}
      local prev, prevClasses
      local fullLoop = 135

      for i = 1,3*fullLoop do
         local s,label = sampler()
         table.insert(seen, s)
         table.insert(seenClasses, label)
         labelCounts[label] = labelCounts[label] or 0
         labelCounts[label] = labelCounts[label] + 1
         labelCountsClasses[label] = labelCountsClasses[label] or 0
         labelCountsClasses[label] = labelCountsClasses[label] + 1

         if i % 3 == 0 then
            if prevClasses ~= nil then
               test.mustBeTrue(#seenClasses == #prevClasses, 'need to see the same amount sampled each loop')
               test.mustBeTrue(TestUtils.listEquals(seenClasses, prevClasses) == false, 'the lists must have different orders')
               local sortedSeen = TestUtils.listCopy(seenClasses)
               table.sort(sortedSeen)
               local sortedPrev = TestUtils.listCopy(prevClasses)
               table.sort(sortedPrev)
               test.mustBeTrue(TestUtils.listEquals(sortedSeen, sortedPrev) == true, 'the lists must have the same sorted orders')

               for _,label in ipairs(index.labels) do
                  local x = 1
                  local y = labelCountsClasses[label]
                  test.mustBeTrue(x == y, 'wrong distribution saw '..y..' expected '..x)
               end
            end
            prevClasses = seenClasses
            seenClasses = { }
            labelCountsClasses = { }
         end

         if i % fullLoop == 0 then
            if prev ~= nil then
               test.mustBeTrue(#seen == #prev, 'need to see the same amount sampled each loop')
               test.mustBeTrue(TestUtils.listEquals(seen, prev) == false, 'the lists must have different orders')
               local sortedSeen = TestUtils.listCopy(seen)
               table.sort(sortedSeen)
               local sortedPrev = TestUtils.listCopy(prev)
               table.sort(sortedPrev)
               test.mustBeTrue(TestUtils.listEquals(sortedSeen, sortedPrev) == true, 'the lists must have the same sorted orders')
               for _,label in ipairs(index.labels) do
                  local x = fullLoop / #index.labels
                  local y = labelCounts[label]
                  test.mustBeTrue(x == y, 'must have the same distribution of '..label..' saw '..y..' expected '..x)
               end
            end
            prev = seen
            seen = { }
            labelCounts = { }
         end
      end
   end,

   testPermutationSamplerWithLabel = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'))
      local sampler,resetSampler = Sampler('permutation', index, 'B')
      local seen = { }
      local prev
      for i = 1,27 do
         local s,label = sampler()
         test.mustBeTrue(label == 'B', 'label must always be B')
         table.insert(seen, s)
         if i % 9 == 0 then
            test.mustBeTrue(sampler() == nil, 'going past the end must return nil')
            resetSampler()
            if prev ~= nil then
               test.mustBeTrue(#seen == #prev, 'need to see the same amount sampled each loop')
               test.mustBeTrue(TestUtils.noDupes(seen) == true, 'should not see any dupes')
               test.mustBeTrue(TestUtils.listEquals(seen, prev) == false, 'the lists must have different orders')
               local sortedSeen = TestUtils.listCopy(seen)
               table.sort(sortedSeen)
               local sortedPrev = TestUtils.listCopy(prev)
               table.sort(sortedPrev)
               test.mustBeTrue(TestUtils.listEquals(sortedSeen, sortedPrev) == true, 'the lists must have the same sorted orders')
            end
            prev = seen
            seen = { }
         end
      end
   end,

   testLinearSampler = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'))
      local sampler,resetSampler = Sampler('linear', index, 'B')
      for j = 1,3 do
         for i = 1,index.itemCount('B') do
            local s,label = sampler()
            test.mustBeTrue(label == 'B', 'label must always be B')
            test.mustBeTrue(s == index.itemAt(i, 'B'), 'must sample in index order')
         end
         test.mustBeTrue(sampler() == nil, 'going past the end must return nil')
         resetSampler()
      end
   end,

   testLabelUniformSampler = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index.csv'))
      local sample = Sampler('label-uniform', index)
      local hist = {}
      for i = 1,1e6 do
         local s,label = sample()
         hist[label] = (hist[label] or 0) + 1
      end
      local ratioA = math.abs((3 / (1e6/hist.A)) - 1)
      test.mustBeTrue(ratioA < .1, 'ratios of labels A must be 1/3')
      local ratioB = math.abs((3 / (1e6/hist.A)) - 1)
      test.mustBeTrue(ratioB < .1, 'ratios of labels B must be 1/3')
   end,

   testUniformSampler = function()
      local index = IndexCSV(paths.concat(paths.dirname(paths.thisfile()), 'index3.csv'))
      local sample = Sampler('uniform', index)
      local hist = {}
      for i = 1,1e6 do
         local s,label = sample()
         hist[tonumber(s)] = (hist[tonumber(s)] or 0) + 1
      end
      hist = torch.Tensor(hist):div(1e6/index.itemCount()):add(-1):abs()
      local err = hist:max()
      test.mustBeTrue(err < .1, 'ratios of samples must be uniform')
   end,

   testPartLinearSamplerSlowFS = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)
      local sampler,resetSampler = Sampler('part-linear', index)

      local prev
      local seen = {}

      for j = 1,4 do
         for i = 1, 78 do
            local s = sampler()
            table.insert(seen, s)
         end
         if prev ~= nil then
            test.mustBeTrue(#seen == #prev, 'need to see the same amount sampled each loop')
            test.mustBeTrue(TestUtils.listEquals(seen, prev), 'the lists must have the same order')
         end
         prev = seen
         seen = {}
         resetSampler()
      end
   end,

   testPartPermutationSamplerSlowFS = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)
      local sampler,resetSampler = Sampler('part-linear-permutation', index)

      local prev
      local seen = {}

      for j = 1,3 do
         for i = 1,78 do
            local s = sampler()
            table.insert(seen, s)
         end
         if prev ~= nil then
            test.mustBeTrue(#seen == #prev, 'need to see the same amount sampled each loop')

            local sortedSeen = TestUtils.listCopy(seen)
            table.sort(sortedSeen)
            local sortedPrev = TestUtils.listCopy(prev)
            table.sort(sortedPrev)
            test.mustBeTrue(TestUtils.listEquals(sortedSeen, sortedPrev) == true, 'the lists must have the same sorted orders')
         end
         prev = seen
         seen = {}
         resetSampler()
      end
   end,

   testPartPermutationPermutationSamplerSlowFS = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)
      local sampler,resetSampler = Sampler('part-permutation-permutation', index)

      local prev
      local seen = {}

      for j = 1,3 do
         for i = 1,78 do
            local s = sampler()
            table.insert(seen, s)
         end

         if prev ~= nil then
            test.mustBeTrue(#seen == #prev, 'need to see the same amount sampled each loop')

            local sortedSeen = TestUtils.listCopy(seen)
            table.sort(sortedSeen)
            local sortedPrev = TestUtils.listCopy(prev)
            table.sort(sortedPrev)
            test.mustBeTrue(TestUtils.listEquals(sortedSeen, sortedPrev) == true, 'the lists must have the same sorted orders')
         end
         prev = seen
         seen = {}
         resetSampler()
      end
   end,

   testUnsupportedSamplersSlowFS = function()
      local index = IndexSlowFS(TestUtils.localHdfsPath)
      -- expect it to fail
      test.mustBeFalse(pcall(function() Sampler('linear', index) end))
      test.mustBeFalse(pcall(function() Sampler('uniform', index) end))
      test.mustBeFalse(pcall(function() Sampler('label-uniform', index) end))
      test.mustBeFalse(pcall(function() Sampler('permutation', index) end))
      test.mustBeFalse(pcall(function() Sampler('label-permutation', index) end))
      test.mustBeFalse(pcall(function() Sampler('label-distribution', index) end))
   end,
}
