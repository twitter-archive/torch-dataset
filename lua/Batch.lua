local function Batch(opt)

   opt = opt or { }
   local batchSize = opt.batchSize or 32
   assert(batchSize > 0)

   local function applyInputDims(dims)
      assert(dims and dims[1])
      local ret = { batchSize }
      for _,d in ipairs(dims) do
         assert(d > 0)
         table.insert(ret, d)
      end
      return torch.LongStorage(ret)
   end

   local inputTensorType = opt.inputTensorType or torch.FloatTensor

   local inputDims = applyInputDims(opt.inputDims)
   local input = inputTensorType(inputDims)
   local target = torch.FloatTensor(inputDims[1])
   local index = torch.LongTensor(inputDims[1])

   -- always keep the reader/processor threads off of cuda for now
   local readerInput
   if opt.cuda then
      input = input:cuda()
      target = target:cuda()
      readerInput = inputTensorType(inputDims)
   else
      readerInput = input
   end

   local batch = {
      batchSize = inputDims[1],
      inputDims = torch.LongStorage(opt.inputDims), -- does not contain batchSize
      input = input,
      target = target,
      index = index,
      class = { },
      item = { },
      readerInput = readerInput
   }

   batch.reset = function(newInputDims)
      if newInputDims then
         inputDims = applyInputDims(newInputDims or opt.inputDims)
      end
      input:resize(inputDims)
      if opt.cuda then
         readerInput:resize(inputDims)
      end
      batch.input = input
      target:resize(inputDims[1])
      batch.target = target
      index:resize(inputDims[1])
      batch.index = index
      batch.readerInput = readerInput
      batch.batchSize = batchSize
      batch.class = { }
      batch.item = { }
   end

   batch.finish = function(n)
      assert(n >= 0 and n <= batchSize)
      if n == 0 then
         batch.input:resize(0)
         batch.target:resize(0)
         batch.index:resize(0)
         batch.readerInput:resize(0)
      elseif n == 1 and batchSize == 1 then
         inputDims = batch.readerInput:size()
         local c = batch.readerInput:nDimension() - 1
         batch.inputDims = torch.LongStorage(c)
         for i = 1,c do
            batch.inputDims[i] = inputDims[i + 1]
         end
      elseif n < batchSize then
         batch.input = input[{ { 1, n } }]
         batch.target = target[{ { 1, n } }]
         batch.index = index[{ { 1, n } }]
         batch.readerInput = readerInput[{ { 1, n } }]
      end
      batch.batchSize = n
      if opt.cuda then
         input:resize(readerInput:size()):copy(readerInput)
      end
   end

   return batch
end

return Batch
