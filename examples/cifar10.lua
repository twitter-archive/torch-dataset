--[[
In this example the index is built from a CSV file
that has been uploaded to S3. The index CSV contains
file names that will be appended to the index URL path
that correspond to PNGs in the CIFAR10 data set. The
index CSV also contains labels for the images.

When your data size is much larger than your memory
this is a simple format to work with. It also has
the nice property of requiring very little startup
copying and loading before sampling begins.
--]]
local opt = lapp[[
Use Dataset to sample images from CIFAR10:

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
local trainingDataset = Dataset('https://d3jod65ytittfm.cloudfront.net/dataset/cifar10/training.csv', {
   partition = opt.partition,
   partitions = opt.partitions,
})

-- Create a batched permutation sampler
local getTrainingBatch, numTrainingBatches = trainingDataset.sampledBatcher({
   samplerKind = 'permutation',
   cuda = opt.cuda,
   batchSize = opt.batchSize,
   inputDims = { 3, 32, 32 },
   verbose = true,
   processor = function(res, processorOpt, input)
      -- This function is not a closure, it is run in a clean Lua environment
      local image = require 'image'
      -- Turn the res string into a ByteTensor (containing the PNG file's contents)
      local bytes = torch.ByteTensor(#res)
      bytes:storage():string(res)
      -- Decompress the PNG bytes into a Tensor
      local pixels = image.decompressPNG(bytes)
      -- Copy the pixels tensor into the mini-batch
      input:copy(pixels)
      return true
   end,
})

-- Walk through the training images once in a random order
local t0 = sys.clock()
local c = 0
local b = 1
while b <= numTrainingBatches() do
   local batch = getTrainingBatch()
   c = c + batch.batchSize
   if c % 10 == 0 then
      local dt = (sys.clock() - t0)
      local ips = c / dt
      print('Sampled '..c..' images in '..dt..' seconds ('..ips..' images per second)')
   end
end
