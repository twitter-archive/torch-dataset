local test = require 'regress'
local dataset = require 'libdataset'
local paths = require 'paths'

test {
   testNewlineOffsets = function()
      local fn = paths.concat(paths.dirname(paths.thisfile()), 'hdfs/files/part-r-00000')
      local offsets = dataset.offsets(fn)
      assert(offsets:size(1) == 22)
      local ok, json = pcall(require, 'cjson')
      if ok then
        local f = io.open(fn, 'r')
        for i = 2,offsets:size(1) do
          f:seek("set", offsets[i - 1])
          local data = f:read(offsets[i] - offsets[i - 1])
          assert(type(json.decode(data)) == 'table')
        end
        f:close()
      end
   end,

   testBlockReaderOffsets = function()
      local fn = paths.concat(paths.dirname(paths.thisfile()), 'hdfs/blockreader')
      local offsets = dataset.offsets(fn)
      assert(offsets:size(1) == 501)
      local ok, thrift = pcall(require, 'libthrift')
      if ok then
        local codec = thrift.codec({ i64string = true })
        local f = io.open(fn, 'r')
        for i = 2,offsets:size(1) do
          f:seek("set", offsets[i - 1])
          local data = f:read(offsets[i] - offsets[i - 1])
          assert(type(codec:read(data)) == 'table')
        end
        f:close()
      end
   end,
}
