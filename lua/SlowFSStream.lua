
local function SlowFSStream(slowFS, allParts, opt)
   opt = opt or { }
   local maxRecords = opt.maxRecords or 0
   local partition = opt.partition or 1
   local partitions = opt.partitions or 1

   local currentRecord = 0
   local stream, wait
   local currentPart = 1
   local parts = { }
   for i,part in ipairs(allParts) do
      if (((i - 1) % partitions) + 1) == partition then
         table.insert(parts, part)
      end
   end
   -- TODO: maybe make this automatic based on part sizes?
   if opt.mergeParts then
      parts = { table.concat(parts, ' ') }
   end
   -- Always persist unless they explicitly state otherwise
   opt.persist = (opt.persist == nil and true) or opt.persist

   local function reset(signal)
      if stream then
         wait(signal)
         stream = nil
      end
      currentPart = 1
      currentRecord = 0
   end

   local function next()
      if maxRecords ~= 0 and currentRecord == maxRecords then
         reset("TERM")
      else
         if stream then
            if stream:next() then
               currentRecord = currentRecord + 1
               return true
            end
            wait()
            stream = nil
            currentPart = currentPart + 1
         end
         if currentPart <= #parts then
            local streamx, waitx = slowFS.streamFormat(parts[currentPart], opt.persist)
            stream = streamx
            wait = waitx
            if stream:next() then
               currentRecord = currentRecord + 1
               return true
            end
         end
      end
   end

   local function record()
      return stream:record()
   end

   local function recordPointer()
      return stream:recordPointer()
   end

   if opt.cleanup then
      for i,part in ipairs(parts) do
         cache.evict(part)
      end
   end

   return {
      next = next,
      reset = reset,
      record = record,
      recordPointer = recordPointer,
   }
end

return SlowFSStream
