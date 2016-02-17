local ipc = require 'libipc'

local function _get(url, path, offset, length)
   return url
end

local function _processor(res, processorOpt, input)
   return true
end

local function _nget(n)
   local ret = { }
   for _ = 1,n do
      table.insert(ret, _get)
   end
   return ret
end

local function _tableize(x)
   if x then
      if type(x) == 'table' then
         return x
      end
      return { x }
   end
end

local function _sizeTable(tensor)
   if tensor ~= nil then
      local ret = { }
      for d = 1,tensor:nDimension() do
         table.insert(ret, tensor:size(d))
      end
      return ret
   end
end

local function Reader(next, opt)

   next = _tableize(next)
   opt = opt or { }
   local name = opt.name or tostring(math.random())
   local numWorkers = opt.poolSize or 64
   local init = {
      get = _tableize(opt.get) or _nget(#next),
      processor = opt.processor or _processor,
      processorOpt = opt.processorOpt or { },
      sizeTable = _sizeTable,
   }
   assert(#next == #init.get, 'expected one get function per next function')
   local q = ipc.workqueue(name)
   local workers = ipc.map(numWorkers, function(name, init)
      local torch = require 'torch'
      local sys = require 'sys'
      local ipc = require 'libipc'

      local q = ipc.workqueue(name)
      local get = init.get
      local processor = init.processor
      local processorOpt = init.processorOpt
      local sizeTable = init.sizeTable

      while true do
         local param = q:read()
         if param == nil then
            break
         else
            local res = { }
            for i,geti in ipairs(get) do
               local item = param.items[i]
               if item then
                  local ok,resi = pcall(function() return geti(item.url, item.offset, item.length) end)
                  if not ok then
                     io.stderr:write(tostring(resi)..'\n')
                  else
                     res[i] = resi
                  end
               end
            end
            if res[1] ~= nil then
               local before = sizeTable(param.input)
               res[#get + 1] = processorOpt
               res[#get + 2] = param.input
               local ok1, ok2, extra = pcall(function() return processor(unpack(res, 1, #get + 2)) end)
               if not ok1 then
                  io.stderr:write(tostring(ok2)..'\n')
               end
               q:write({
                  id = param.id,
                  ok = ok1 and ok2,
                  before = before,
                  after = sizeTable(param.input),
                  extra = ok1 and ok2 and extra,
               })
            else
               q:write({
                  id = param.id,
                  ok = false,
               })
            end
         end
      end
      q:close()
   end, name, init)

   local function hasDimChange(before, after)
      if #before == #after then
         for i,v in ipairs(before) do
            if v ~= after[i] then
               return true
            end
         end
         return false
      end
      return true
   end

   local id = 0
   local closures = { }

   local function nextId()
      id = (id + 1) % 10000
      return id
   end

   local function startBatch(batchSize, input)
      assert(batchSize > 0)
      assert(input == nil or input:size(1) == batchSize)
      local batch = {
         outputParam = { },
         input = input,
         batchSize = batchSize,
         numJobs = 0,
      }
      for i = 1,batchSize do
         local items = { }
         local label
         for j,n in ipairs(next) do
            local url,labelj,offset,length = n()
            if j == 1 then
               label = labelj
            end
            if url then
               items[j] = { url = url, offset = offset, length = length }
            end
         end
         if label == '' then
            label = nil
         end
         if items[1] ~= nil then
            local param = { items = items }
            if input ~= nil then
               param.input = input[i]
            end
            batch.numJobs = batch.numJobs + 1
            param.id = nextId()
            -- lazy, see how fast this is first...
            closures[param.id] = function(result)
               assert(batch.numJobs > 0)
               batch.numJobs = batch.numJobs - 1
               if result.ok then
                  batch.outputParam[i] = {
                     url = items[1].url,
                     offset = items[1].offset,
                     length = items[1].length,
                     label = label,
                     before = result.before,
                     after = result.after,
                     extra = result.extra,
                  }
               end
            end
            q:write(param)
         else
            break
         end
      end
      return batch
   end

   local function dojob(doNotBlock)
      workers:checkErrors()
      local result = q:read(doNotBlock)
      if result then
         closures[result.id](result)
         closures[result.id] = nil
         return true
      end
   end

   local function finishBatch(batch, doNotBlock)
      while (dojob(true)) do end
      if doNotBlock == true and batch.numJobs > 0 then
         return nil
      end
      while (batch.numJobs > 0) do
         dojob()
      end
      local ret = { }
      if batch.input ~= nil then
         if batch.batchSize == 1 then
            local first = batch.outputParam[1]
            if first ~= nil then
               if hasDimChange(first.before, first.after) == true then
                  table.insert(first.after, 1, 1)
                  batch.input:resize(torch.LongStorage(first.after))
               end
               first.index = 1
               table.insert(ret, first)
            end
         else
            for i,param in pairs(batch.outputParam) do
               assert(hasDimChange(param.before, param.after) == false, 'Illegal to resize input tensors on batch sizes > 1')
               param.index = i
               table.insert(ret, param)
               local j = #ret
               if i ~= j then
                  batch.input[j]:copy(batch.input[i])
               end
            end
         end
      else
         for i,param in pairs(batch.outputParam) do
            param.index = i
            table.insert(ret, param)
         end
      end
      return ret
   end

   local function get(batchSize, input)
      return finishBatch(startBatch(batchSize, input))
   end

   local function reset()
      q:drain()
      while (dojob(true)) do end
   end

   local function close()
      for _ = 1,numWorkers do
         q:write(nil)
      end
      workers:join()
      q:close()
   end

   return {
      startBatch = startBatch,
      finishBatch = finishBatch,
      get = get,
      reset = reset,
      close = close,
   }
end

return Reader
