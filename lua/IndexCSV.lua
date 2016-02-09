local paths = require 'paths'
local IndexUtils = require 'dataset.IndexUtils'

local function IndexCSV(url, partition, partitions, opt)

   partition = partition or 1
   assert(partition >= 1)
   partitions = partitions or 1
   assert(partitions >= 1)
   assert(partition <= partitions)
   opt = opt or { }
   local indexUtils = IndexUtils(opt)

   local function loadURLPrefix(url, isFileBased)
      local metaURL = opt.metaURL or (paths.dirname(url) .. '/' .. paths.basename(url, paths.extname(url)) .. '-meta.csv')
      if paths.filep(metaURL) then
         local lines = io.input(metaURL):lines()
         local i = 1
         for line in lines do
            if i == 2 then
               return line
            end
            i = i + 1
         end
      elseif isFileBased then
         return paths.dirname(url)
      end
   end

   local function splitLine(line)
      local parts = { }
      for part in line:gmatch('[^,]+') do
         table.insert(parts, part)
      end
      return parts
   end

   local function loadCSV(url)
      local lines = io.input(url):lines()
      local classes = { }
      local lineno = 0
      local filenamePos
      local offsetPos
      local lengthPos
      local itemCountPos
      local itemSizePos
      local labelPos = { }
      for line in lines do
         local parts = splitLine(line)
         if lineno == 0 then
            for pos,part in ipairs(parts) do
               if part == 'filename' then
                  assert(filenamePos == nil, 'Index CSV can only have one filename column')
                  filenamePos = pos
               elseif part == 'offset' then
                  assert(offsetPos == nil, 'Index CSV can only have one offset column')
                  offsetPos = pos
               elseif part == 'length' then
                  assert(lengthPos == nil, 'Index CSV can only have one length column')
                  lengthPos = pos
               elseif part == 'itemCount' then
                  assert(itemCountPos == nil, 'Index CSV can only have one itemCount column')
                  itemCountPos = pos
               elseif part == 'itemSize' then
                  assert(itemSizePos == nil, 'Index CSV can only have one itemSize column')
                  itemSizePos = pos
               elseif string.sub(part, 1, 5) == 'label' then
                  table.insert(labelPos, pos)
               else
                  io.stderr:write('Index CSV unknown column: ' .. part .. '\n')
               end
            end
            assert(filenamePos ~= nil, 'Index CSV must have a filename column')
         else
            local item = {
               filename = parts[filenamePos]
            }
            assert(item.filename)
            if offsetPos ~= nil then
               item.offset = tonumber(parts[offsetPos])
               assert(item.offset)
            end
            if lengthPos ~= nil then
               item.length = tonumber(parts[lengthPos])
               assert(item.length)
            end
            if itemCountPos ~= nil then
               item.itemCount = tonumber(parts[itemCountPos])
               assert(item.itemCount)
            end
            if itemSizePos ~= nil then
               item.itemSize = tonumber(parts[itemSizePos])
               assert(item.itemSize)
            end
            if #labelPos > 0 then
               for _,pos in ipairs(labelPos) do
                  local label = parts[pos]
                  if label ~= nil then
                     indexUtils.addItem(classes, item, label)
                  end
               end
            else
               indexUtils.addItem(classes, item, '*')
            end
         end
         lineno = lineno + 1
      end
      for _,class in pairs(classes) do
         table.sort(class.items, function(a, b)
            if a.filename == b.filename then
               if a.offset ~= nil and a.offset ~= b.offset then
                  return a.offset < b.offset
               end
               if a.length ~= nil then
                  return a.length < b.length
               end
               return false
            else
               return a.filename < b.filename
            end
         end)
      end
      return classes,(itemCountPos ~= nil or offsetPos ~= nil)
   end

   local loaded,isFileBased = loadCSV(url)

   local urlPrefix = loadURLPrefix(url, isFileBased)
   local urlSuffix
   if opt.urlParams then
      local keys = { }
      for k,_ in pairs(opt.urlParams) do
         table.insert(keys, k)
      end
      table.sort(keys)
      for _,k in ipairs(keys) do
         local v = opt.urlParams[k]
         if urlSuffix then
            urlSuffix = urlSuffix .. '&' .. tostring(k) .. '=' .. tostring(v)
         else
            urlSuffix = tostring(k) .. '=' .. tostring(v)
         end
      end
   end

   local classes, totalItems, labels, labelIndex = indexUtils.downsampleAndGetLabels(loaded, partition, partitions)

   return {
      labels = labels,
      labelIndex = labelIndex,
      itemCount = indexUtils.itemCount(classes, totalItems),
      itemAt = indexUtils.itemAt(classes, totalItems, labels, urlPrefix, urlSuffix),
      urlPrefix = urlPrefix,
      reset = nil,
      indexType = 'CSV'
   }
end

return IndexCSV
