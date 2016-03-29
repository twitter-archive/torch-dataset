--[[
In this example the index is built from a Torch serialized
file that has been placed on S3. The file contains a Lua
table with 2 fields x and y. The field x is a ByteTensor
of dimensions 50000 x 3 x 32 x 32, each item in the first
dimension is 3 channel 32 by 32 image. The field y is a
ByteTensor of size 50000 that contains a label number per
image in the x Tensor.

When your data size is small enough to fit in memory this
can be a very convenient format.
--]]
local opt = lapp[[
Use Dataset to sample images from MNIST:

(options)
   --partition     (default 1)     which partition should we sample from?
   --partitions    (default 1)     how many partitions to divide the dataset into?
   --batchSize     (default 16)    how many images in a mini-batch?
   --cuda                          should we return a CUDA tensor mini-batch from the sampler?
]]

-- Requires
local Dataset = require 'dataset.Dataset'
local sys = require 'sys'

-- Load the index
local trainingDataset = Dataset('http://d3jod65ytittfm.cloudfront.net/dataset/mnist/train.t7', {
   partition = opt.partition,
   partitions = opt.partitions,
})

-- Create a batched uniform sampler
local getTrainingBatch, numTrainingBatches = trainingDataset.sampledBatcher({
   samplerKind = 'uniform',
   cuda = opt.cuda,
   batchSize = opt.batchSize,
   inputDims = { 1, 32, 32 },
   verbose = true,
   processor = function(res, processorOpt, input)
      -- The data is already a 32x32 tensor, we can just copy it into the mini-batch
      input:copy(res)
      return true
   end,
})

-- Walk through the training images once, uniformly sampling them
local t0 = sys.clock()
local c = 0
local b = 1
while b <= numTrainingBatches() do
   local batch = getTrainingBatch()
   c = c + batch.batchSize
   if c % 1000 == 0 then
      local dt = (sys.clock() - t0)
      local ips = c / dt
      print('Sampled '..c..' images in '..dt..' seconds ('..ips..' images per second)')
   end
   b = b + 1
end
