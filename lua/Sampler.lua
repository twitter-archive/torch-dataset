local AliasMethod = require 'distribution.AliasMethod'

local function LinearSampler(index, label)
   assert(index.indexType ~= 'SlowFS', "LinearSampler is not supported for SlowFS. Use PartLinearSampler (part-linear).")

   local i = 0
   local n = index.itemCount(label)
   return function()
      i = i + 1
      if i <= n then
         return index.itemAt(i, label)
      end
   end, function()
      i = 0
   end
end

local function tableCopy(t)
   local r = { }
   for _,p in ipairs(t) do
       table.insert(r, p)
   end
   return r
end

local function PartLinearSampler(index)
   assert(index.indexType == 'SlowFS', "PartLinearSampler is only supported for SlowFS. Type of index passed: " .. index.indexType)
   assert(index.numFiles > 0, "Must have at least one part file in SlowFS index.")

   local fileCounter = 1
   local itemCounter = 0
   local numFiles = index.numFiles
   local itemsInCurrentPart

   local function reset()
      -- clear the index
      index.reset()

      -- tell the index to start prefetching
      for partIndex = 1, numFiles do
         index.addPartIndex(partIndex)
      end

      -- reset the counters
      fileCounter = 1
      itemCounter = 0
   end

   local function next()
      itemCounter = itemCounter + 1

      if fileCounter <= numFiles then
         -- first time file access, trigger finish import and get num items
         if itemCounter == 1 then
            itemsInCurrentPart = index.itemsInPart(fileCounter)
         end

         if fileCounter and itemCounter <= itemsInCurrentPart then
            local url, offset, length = index.itemAt(fileCounter, itemCounter)
            return url, '', offset, length
         end
         -- ran out of items in this file
         -- start next file
         if itemCounter > itemsInCurrentPart then
            -- remove second to last file since the reader may
            -- not be done reading fromt the current file
            -- (all the files are cleared in reset)
            if fileCounter > 2 then
               index.doneWithPart(fileCounter - 1)
            end

            fileCounter = fileCounter + 1
            itemCounter = 0
            -- try again
            return next()
         end
         return nil
      end
   end
   -- start the sampler
   reset()

   return next, reset
end

local function UniformSampler(index, label)
   assert(index.indexType ~= 'SlowFS', "UniformSampler is not supported for SlowFS.")

   return function()
      local id = torch.random(1, index.itemCount(label))
      return index.itemAt(id, label)
   end, function()
      -- nothing to do on reset
   end
end

local function getNewPerm(size, oldPerm, depth)
   if not oldPerm then
      return torch.randperm(size)
   end

   depth = (depth and math.max(2, math.min(depth, size))) or 2

   -- Make sure we have a different distribution
   -- when resetting the permutation to make test work.
   local oldval
   if oldPerm then
      oldval = (size >= depth and oldPerm[{{1, depth}}]:clone()) or nil
   end

   local newval
   -- Loop til they are different.
   -- Prob[k identical values | permSize == n] = (n-k)!/n!,
   -- so perfectly fine regarding to memory allocation even for small sizes.
   while not newval or (oldval and torch.add(newval,-1, oldval):abs():sum() == 0) do
      oldPerm = torch.randperm(size)
      newval = (size >= depth and oldPerm[{{1, depth}}]:clone()) or true
   end
   return oldPerm
end

local function PermutationSampler(index, label)
   assert(index.indexType ~= 'SlowFS', "PermutationSampler is not supported for SlowFS.")

   local n = index.itemCount(label)
   local p = getNewPerm(n, p)
   local i = 0
   return function()
      i = i + 1
      if i <= n then
         return index.itemAt(p[i], label)
      end
   end, function()
      p = getNewPerm(n, p)
      i = 0
   end
end

local function LabelUniformSampler(index)
   assert(index.indexType ~= 'SlowFS', "LabelUniformSampler is not supported for SlowFS.")
   return function()
      local label = index.labels[torch.random(1, #index.labels)]
      return index.itemAt(torch.random(1, index.itemCount(label)), label)
   end, function()
      -- nothing to do on reset
   end
end

local function LabelDistributionSampler(index, distribution)
   assert(index.indexType ~= 'SlowFS', "LabelDistributionSampler is not supported for SlowFS.")
   local weights = {}
   -- if autoFill, set missing class weights to 1.
   local autoFill
   if distribution.autoFill then
      autoFill = distribution.autoFill
      distribution.autoFill = nil
   end
   for i,class in ipairs(index.labels) do
      local weight = distribution[class]
      if autoFill then
         weight = weight or distribution.defaultWeight or 1
      else
         assert(weight, 'LabelUniformSampler: 2nd arg must be a table [class]=weight')
      end
      weights[i] = weight
   end
   local getClass = AliasMethod(weights)
   return function()
      local label = index.labels[getClass()]
      return index.itemAt(torch.random(1, index.itemCount(label)), label)
   end, function()
      -- nothing to do on reset
   end
end

local function LabelPermutationSampler(index)
   assert(index.indexType ~= 'SlowFS', "LabelPermutationSampler is not supported for SlowFS.")
   local filePermTable = {}
   local nClasses = #index.labels
   local classPerm
   local i = 0
   local fidxt = torch.IntTensor(nClasses):fill(0)

   return function()
      i = i % nClasses
      if i == 0 then
         classPerm = getNewPerm(nClasses, classPerm)
      end

      i = i + 1
      local cidx = classPerm[i]
      local label = index.labels[cidx]
      local n = index.itemCount(label)
      fidxt[cidx] = fidxt[cidx]%n

      if fidxt[cidx] == 0 then
         filePermTable[cidx] = getNewPerm(n, filePermTable[cidx])
      end

      fidxt[cidx] = fidxt[cidx] + 1
      local fidx = filePermTable[cidx][fidxt[cidx]]

      return index.itemAt(fidx, label)
   end, function()
      -- nothing to do on reset?
   end
end

local function PartLinearPermutationSampler(index)
   assert(index.indexType == 'SlowFS', "PartPermutationSampler is only supported for SlowFS. Type of index passed: " .. index.indexType)
   assert(index.numFiles > 0, "Must have at least one part file in SlowFS index.")

   local fileCounter = 1
   local itemCounter = 0
   local numFiles = index.numFiles
   local itemsInCurrentPart
   local itemPerm

   local function reset()
      -- clear the index
      index.reset()

      -- tell the index to start prefetching
      for partIndex = 1, numFiles do
         index.addPartIndex(partIndex)
      end

      -- reset the counters
      fileCounter = 1
      itemCounter = 0
   end

   local function next()
      itemCounter = itemCounter + 1

      if fileCounter <= numFiles then
         -- first time file access, trigger finish import and get num items
         -- create a permutation for items in the file
         if itemCounter == 1 then
            itemsInCurrentPart = index.itemsInPart(fileCounter)
            itemPerm = getNewPerm(itemsInCurrentPart, itemPerm)
         end

         if fileCounter and itemCounter <= itemsInCurrentPart then
            local url, offset, length = index.itemAt(fileCounter, itemPerm[itemCounter])
            return url, '', offset, length
         end
         -- ran out of items in this file
         -- start next file
         if itemCounter > itemsInCurrentPart then
            -- remove second to last file since the reader may
            -- not be done reading fromt the current file
            -- (all the files are cleared in reset)
            if fileCounter > 2 then
               index.doneWithPart(fileCounter - 1)
            end

            fileCounter = fileCounter + 1
            itemCounter = 0
            -- try again
            return next()
         end
         return nil
      end
   end
   -- start the sampler
   reset()

   return next, reset
end

local function PartPermutationPermutationSampler(index)
   assert(index.indexType == 'SlowFS', "PartPermutationPermutationSampler is only supported for SlowFS. Type of index passed: " .. index.indexType)
   assert(index.numFiles > 0, "Must have at least one part file in SlowFS index.")

   local partIndexOrder = { }
   local filePerm = { }
   local currentPartIndex
   local itemPerm
   local fileCounter = 1
   local itemCounter = 0
   local numFiles = index.numFiles
   local itemsInCurrentPart

   local function reset()
      -- clear the index
      index.reset()

      -- permute file order
      partIndexOrder = torch.totable(getNewPerm(numFiles))

      -- copy to keep track of what file we're using
      filePerm = tableCopy(partIndexOrder)

      -- tell the index to start prefetching
      for _,partIndex in ipairs(partIndexOrder) do
         index.addPartIndex(partIndex)
      end
      -- get the current part
      currentPartIndex = table.remove(partIndexOrder, 1)

      -- reset the counters
      fileCounter = 1
      itemCounter = 0
   end

   local function next()
      itemCounter = itemCounter + 1

      if fileCounter <= numFiles then
         -- first time file access, trigger finish import and get num items
         -- create a permutation for items in the file
         if itemCounter == 1 then
            itemsInCurrentPart = index.itemsInPart(currentPartIndex)
            itemPerm = getNewPerm(itemsInCurrentPart, itemPerm)
         end

         if currentPartIndex and itemCounter <= itemsInCurrentPart then
            local url, offset, length = index.itemAt(currentPartIndex, itemPerm[itemCounter])
            return url, '', offset, length
         end
         -- ran out of items in this file
         -- start next file
         if itemCounter > itemsInCurrentPart then
            -- remove second to last file since the reader may
            -- not be done reading fromt the current file
            -- (all the files are cleared in reset)
            if fileCounter > 2 then
               index.doneWithPart(filePerm[fileCounter - 1])
            end

            currentPartIndex = table.remove(partIndexOrder, 1)

            fileCounter = fileCounter + 1
            itemCounter = 0
            -- try again
            return next()
         end
         return nil
      end
   end
   -- start the sampler
   reset()

   return next, reset
end

local function Sampler(kind, index, label, options)
   if kind == 'linear' then
      return LinearSampler(index, label)
   elseif kind == 'part-linear' then
      return PartLinearSampler(index)
   elseif kind == 'part-linear-permutation' then
      return PartLinearPermutationSampler(index)
   elseif kind == 'part-permutation-permutation' then
      return PartPermutationPermutationSampler(index)
   elseif kind == 'uniform' then
      return UniformSampler(index, label)
   elseif kind == 'permutation' then
      return PermutationSampler(index, label)
   elseif kind == 'label-uniform' then
      return LabelUniformSampler(index)
   elseif kind == 'label-permutation' then
      return LabelPermutationSampler(index)
   elseif kind == 'label-distribution' then
      return LabelDistributionSampler(index, options)
   else
      print("Invalid sampler kind: "..kind..". See go/torch for the list of accepted samplers.")
      os.exit(1)
   end
end

return Sampler
