
local function IndexTensor(tensorOrTable, partition, partitions, opt)

   partition = partition or 1
   assert(partition >= 1)
   partitions = partitions or 1
   assert(partitions >= 1)
   assert(partition <= partitions)
   opt = opt or { }

   local function downsample(input)
      if partitions > 1 then
         local n = math.floor(input:size(1) / partitions)
         local m = input:size(1) % partitions
         if partition <= m then
            n = n + 1
         end
         local dims = { n }
         for i = 2,input:nDimension() do
            dims[i] = input:size(i)
         end
         local TT = torch[torch.typename(input):split("[.]")[2]]
         local output = TT(unpack(dims))
         local j = partition
         for i = 1,n do
            if #dims > 1 then
               output[i]:copy(input[j])
            else
               output[i] = input[j]
            end
            j = j + partitions
         end
         return output
      end
      return input
   end

   local classes = { }
   local labels = { }
   local totalItems = 0

   if type(tensorOrTable) == 'table' then
      assert(tensorOrTable.x ~= nil, 'IndexTensor requires "x" table entry for items')
      assert(tensorOrTable.y ~= nil, 'IndexTensor requires "y" table entry for labels')
      local items = downsample(tensorOrTable.x)
      local itemLabels = downsample(tensorOrTable.y)
      local seen = { }
      itemLabels:apply(function(label)
         if not seen[label] then
            table.insert(labels, label)
            seen[label] = 1
         else
            seen[label] = seen[label] + 1
         end
         totalItems = totalItems +1
      end)
      local dims = { 1 }
      for i = 2,items:nDimension() do
         dims[i] = items:size(i)
      end
      local TT = torch[torch.typename(items):split("[.]")[2]]
      for _,label in ipairs(labels) do
         dims[1] = seen[label]
         classes[label] = TT(unpack(dims))
      end
      seen = { }
      local i = 1
      itemLabels:apply(function(label)
         seen[label] = (seen[label] and seen[label] + 1) or 1
         classes[label][seen[label]]:copy(items[i])
         i = i + 1
      end)
   else
      classes['*'] = downsample(tensorOrTable)
      totalItems = classes['*']:size(1)
   end

   if opt.cuda then
      for _,c in pairs(classes) do
         c:cuda()
      end
   end

   table.sort(labels)
   local labelIndex = { }
   for i,v in ipairs(labels) do
      if v ~= '*' then
         labelIndex[v] = i
      end
   end

   local function itemCount(label)
      if label ~= nil then
         return classes[label]:size(1)
      else
         return totalItems
      end
   end

   local function itemAt(index, label)
      if label ~= nil then
         return classes[label][index], label
      else
         for _,v in ipairs(labels) do
            local n = classes[v]:size(1)
            if index <= n then
               return classes[v][index], v
            else
               index = index - n
            end
         end
         return classes['*'][index]
      end
   end

   return {
      labels = labels,
      labelIndex = labelIndex,
      itemCount = itemCount,
      itemAt = itemAt,
      indexType = 'Tensor'
   }
end

return IndexTensor
