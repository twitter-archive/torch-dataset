
local function IndexUtils(opt)

   opt = opt or { }

   local function startsWith(s, p)
      return string.sub(s, 1, string.len(p)) == p
   end

   local function addItem(classes, item, label)
      local class = classes[label]
      if not class then
         class = { label = label, numItems = 0, items = { } }
         classes[label] = class
      end
      class.numItems = class.numItems + (item.itemCount or 1)
      table.insert(class.items, item)
   end

   local function downsampleLabelled(classes, partition, partitions)
      assert(partition >= 1)
      assert(partitions >= 1)
      assert(partition <= partitions)

      if partitions == 1 then
         return classes
      else
         local ret = { }
         for _,class in pairs(classes) do
            local nc = { label = class.label, numItems = 0, items = { } }
            ret[class.label] = nc
            for i,item in ipairs(class.items) do
               if (i % partitions) == (partition - 1) then
                  nc.numItems = nc.numItems + (item.itemCount or 1)
                  table.insert(nc.items, item)
               end
            end
         end
         return ret
      end
   end

   local function withPrefix(filename, urlPrefix, urlSuffix)
      local ret = filename
      if urlPrefix ~= nil then
         ret = urlPrefix .. '/' .. ret
      end
      if urlSuffix ~= nil then
         ret = ret .. '?' .. urlSuffix
      end
      return ret
   end

   local function itemFromClassAt(class, label, index, urlPrefix, urlSuffix)
      assert(index <= class.numItems)
      local item = class.items[index]
      return withPrefix(item.filename, urlPrefix, urlSuffix), label, item.offset, item.length
   end

   local function downsampleAndGetLabels(loaded, partition, partitions)
      local classes = downsampleLabelled(loaded, partition, partitions)
      local labels = { }
      local totalItems = 0
      for _,class in pairs(classes) do
         if class.label ~= '*' then
            table.insert(labels, class.label)
         end
         totalItems = totalItems + class.numItems
      end
      table.sort(labels)
      local labelIndex = { }
      for i,v in ipairs(labels) do
         if v ~= '*' then
            labelIndex[v] = i
         end
      end
      return classes, totalItems, labels, labelIndex
   end

   local function itemCount(classes, totalItems)
      return function(label)
         if label ~= nil then
            return classes[label].numItems
         else
            return totalItems
         end
      end
   end

   local function itemAt(classes, totalItems, labels, urlPrefix, urlSuffix)
      return function(index, label)
         assert(index >= 1)
         if label ~= nil then
            return itemFromClassAt(classes[label], label, index, urlPrefix, urlSuffix)
         else
            assert(index <= totalItems)
            for _,v in ipairs(labels) do
               local n = classes[v].numItems
               if index <= n then
                  return itemFromClassAt(classes[v], v, index, urlPrefix, urlSuffix)
               else
                  index = index - n
               end
            end
            local unlabeled = classes['*']
            if unlabeled ~= nil and index <= unlabeled.numItems then
               return itemFromClassAt(unlabeled, '', index, urlPrefix, urlSuffix)
            end
         end
         error('Unreachable')
      end
   end

   return {
      addItem = addItem,
      itemFromClassAt = itemFromClassAt,
      downsampleAndGetLabels = downsampleAndGetLabels,
      itemCount = itemCount,
      itemAt = itemAt,
   }
end

return IndexUtils
