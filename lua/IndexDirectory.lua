local lfs = require 'lfs'
local IndexUtils = require 'dataset.IndexUtils'

local function IndexDirectory(url, partition, partitions, opt)

   partition = partition or 1
   assert(partition >= 1)
   partitions = partitions or 1
   assert(partitions >= 1)
   assert(partition <= partitions)
   opt = opt or { }
   local indexUtils = IndexUtils(opt)

   local function orderedDirWalk(dir, f)
      local fns = { }
      for fn in lfs.dir(dir) do
         table.insert(fns, fn)
      end
      table.sort(fns)
      for _,fn in ipairs(fns) do
         f(fn)
      end
   end

   local function loadDirectory(root)
      local classes = { }
      local function rcsvLoad(dn, label)
         orderedDirWalk(root..'/'..dn, function(fn)
            if string.sub(fn, 1, 1) ~= '.' then
               local dfn = dn..'/'..fn
               local mode = lfs.attributes(root..'/'..dfn).mode
               if mode == 'file' then
                  indexUtils.addItem(classes, { filename = dfn }, label)
               elseif mode == 'directory' then
                  rcsvLoad(dfn, label)
               end
            end
         end)
      end
      orderedDirWalk(root, function(fn)
         if string.sub(fn, 1, 1) ~= '.' then
            local mode = lfs.attributes(root..'/'..fn).mode
            if mode == 'file' then
               indexUtils.addItem(classes, { filename = fn }, '*')
            elseif mode == 'directory' then
               rcsvLoad(fn, fn)
            end
         end
      end)
      return classes
   end

   local loaded
   local urlPrefix = url
   loaded = loadDirectory(urlPrefix)

   local classes, totalItems, labels, labelIndex = indexUtils.downsampleAndGetLabels(loaded, partition, partitions)

   return {
      labels = labels,
      labelIndex = labelIndex,
      itemCount = indexUtils.itemCount(classes, totalItems),
      itemAt = indexUtils.itemAt(classes, totalItems, labels, urlPrefix),
      urlPrefix = urlPrefix,
      reset = nil,
      indexType = 'Directory'
   }
end

return IndexDirectory
