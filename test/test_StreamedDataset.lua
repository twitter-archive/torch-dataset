local test = require 'regress'
local Dataset = require 'dataset.Dataset'
local paths = require 'paths'
local TestUtils = require './TestUtils'

test {
   testSimple = function()
      local get = function(stream)
         return true, stream:record()
      end
      local processor = function(res, input)
         return true
      end
      local dataset = Dataset(TestUtils.localHdfsPath, {
         streaming = true
      })
      local getBatch, numBatches, reset = dataset.sampledBatcher({
         samplerKind = 'part-linear',
         batchSize = 4,
         get = get,
         processor = processor,
      })
      local b = 1
      while b <= numBatches() do
         local batch = getBatch()
         b = b + 1
      end
      local e = 0
      for _,n in pairs(TestUtils.testFileSizes) do
         e = e + n
      end
      local x = math.ceil(e/4)
      assert(b-1 == x, 'should be '..x..' not '..(b-1))
   end,
}
