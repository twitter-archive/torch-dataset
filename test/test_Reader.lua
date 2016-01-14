local test = require 'regress'
local Reader = require 'dataset.Reader'

local function testBatch(batchSize)
   local urls = { '4', '1', '2', '9', '5', '7', '6' }
   local i = 0
   local function next()
      i = i + 1
      return urls[i]
   end
   local function get(url)
      local t1 = os.clock() + (tonumber(url) / 10)
      while t1 > os.clock() do end
      return url
   end
   local reader = Reader(next, { get = get })
   local seen = { }
   local numBatches = math.ceil(#urls / batchSize)
   for k = 1,numBatches do
      local batch = reader.get(batchSize)
      for _,res in ipairs(batch) do
         table.insert(seen, res.url)
      end
   end
   test.mustBeTrue(i == 7 or i == 8, 'all urls must be used: '..i..' for batchSize '..batchSize)
   test.mustBeTrue(#seen == 7, 'seen must have 7 items for batchSize '..batchSize)
   for j = 1,i-1 do
      test.mustBeTrue(seen[j] == urls[j], 'must see "'..urls[j]..'" at index '..j..' for batchSize '..batchSize)
   end
end

-- Tests:
test {
   testBatchedOrdering = function()
      testBatch(1)
      testBatch(2)
      testBatch(3)
      testBatch(64)
   end,

   testSkippingFailures = function()
      local i = 0
      local function next()
         i = i + 1
         return tostring(i)
      end
      local function get(url)
         if tonumber(url) % 3 > 0 then
            return url
         end
      end
      local function processor(res)
         if tonumber(res) % 13 == 0 then
            assert()
         end
         return true
      end
      local reader = Reader(next, { get = get, processor = processor })
      for i = 1,100 do
         local batch = reader.get(4)
         for _,v in ipairs(batch) do
            local j = tonumber(v.url)
            test.mustBeTrue(j % 3 > 0, 'should never see % 3 numbers')
            test.mustBeTrue(j % 13 > 0, 'should never see % 13 numbers')
         end
      end
   end,

   testWithTensors = function()
      local x = { 1,2,0,4, 5,6,7,8, 0,10,-1,12, 0,-1,0,16, 17,0,19,20, -1,0,0,-1, 25,26,27,28 }
      local y = { 1,2,4, 5,6,7,8, 10,12, 16, 17,19,20, 25,26,27,28 }
      local i = 0
      local function next()
         i = i + 1
         return tostring(x[i])
      end
      local function get(url)
         if url ~= '0'  then
            return url
         end
      end
      local function processor(res, processorOpt, input)
         if res ~= '-1' then
            input:fill(tonumber(res))
            return true
         end
      end
      local reader = Reader(next, { get = get, processor = processor })
      local input = torch.FloatTensor(4, 1)
      local z = 1
      for i = 1,(#x/4) do
         input:fill(0)
         local batch = reader.get(4, input)
         for k,v in ipairs(batch) do
            local j = tonumber(v.url)
            test.mustBeTrue(j == y[z], 'should see '..y[z]..' not '..j..' at '..z)
            z = z + 1
            test.mustBeTrue(j ~= 0, 'should never see 0')
            test.mustBeTrue(j ~= -1, 'should never see -1')
            local f = input[k][1]
            test.mustBeTrue(f == j, 'should have tensor value '..f..' equal to '..j)
         end
      end
   end,

   testMultipleGet = function()
      local ni1 = 0
      local function next1()
         ni1 = ni1 + 1
         return ni1
      end
      local ni2 = 1000
      local function next2()
         ni2 = ni2 + 1
         return ni2
      end
      local function processor(res1, res2, processorOpt, input)
         input[1] = res1
         input[2] = res2
         return true
      end
      local reader = Reader({ next1, next2 }, { processor = processor, verbose = true })
      local input = torch.FloatTensor(1, 2)
      for i = 1,100 do
         local batch = reader.get(1, input)
         test.mustBeTrue(input[1][1] == i)
         test.mustBeTrue(input[1][2] == 1000 + i)
      end
   end,

   testExtra = function()
      local ni1 = 0
      local function next1()
         ni1 = ni1 + 1
         return ni1
      end
      local function processor(res, processorOpt, input)
         input[1] = tonumber(res)
         local extra = torch.FloatTensor(3, 3)
         extra:fill(tonumber(res))
         return true, extra
      end
      local reader = Reader({ next1, next2 }, { processor = processor, verbose = true })
      local input = torch.FloatTensor(1, 1)
      for i = 1,100 do
         local batch = reader.get(1, input)
         test.mustBeTrue(input[1][1] == i)
         test.mustBeTrue(batch[1].extra:sum() == 9*i)
      end
   end,
}
