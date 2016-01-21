local test = require 'regress'
local Getters = require 'dataset.Getters'
local paths = require 'paths'

local function simpleHTTPServer()
   local parallel = require 'libparallel'
   local q = parallel.workqueue('http')
   local s = parallel.map(1, function()
      local parallel = require 'libparallel'
      local q = parallel.workqueue('http')
      local socket = require 'socket'
      local server = socket.tcp()
      server:settimeout(1)
      server:bind("*", 0)
      server:listen(4)
      local ip,port = server:getsockname()
      q:write({ ip = ip, port = port })
      while 1 do
         local client,err = server:accept()
         if client then
            local line, err = client:receive()
            if not err then
               if line == 'GET /yes HTTP/1.1' then
                  client:send("HTTP/1.0 200 OK\r\n\r\nDATABYTES")
               else
                  client:send("HTTP/1.0 404\r\n\r\n")
               end
            end
            client:close()
         elseif q:read(true) == 'quit' then
            server:close()
            break
         end
      end
   end)
   local function quit()
      q:write('quit')
      s:join()
   end
   local where = q:read()
   return where.ip, where.port, quit
end

test {
   testFile = function()
      local get = Getters('file')
      test.mustBeTrue(get(paths.concat(paths.dirname(paths.thisfile()), 'files/missing')) == nil, 'missing files must return nil')
      test.mustBeTrue(get(paths.concat(paths.dirname(paths.thisfile()), 'files/1.txt')) == 'ABCDEFGH', 'present files must return their contents')
      test.mustBeTrue(get(paths.concat(paths.dirname(paths.thisfile()), 'files/1.txt'), 3, 3) == 'DEF', 'offset and length must return limited contents')
      test.mustBeTrue(get(paths.concat(paths.dirname(paths.thisfile()), 'files/1.txt'), 13, 3) == nil, 'out of bounds offset must return nil')
      test.mustBeTrue(get(paths.concat(paths.dirname(paths.thisfile()), 'files/1.txt'), -1, 3) == nil, 'negative offset must return nil')
      test.mustBeTrue(get(paths.concat(paths.dirname(paths.thisfile()), 'files/1.txt'), 3, 13) == 'DEFGH', 'out of bounds length must return as much as possible')
   end,
   testHTTP = function()
      local ip,port,quit = simpleHTTPServer()
      local get = Getters('http')
      test.mustBeTrue(get('http://'..ip..':'..port..'/yes') == 'DATABYTES', 'URLs with status code 200 must return non-nil')
      test.mustBeTrue(get('http://'..ip..':'..port..'/yes', 4, 2) == 'BY', 'URLs with offset and length should work')
      test.mustBeTrue(get('http://'..ip..':'..port..'/no') == nil, 'URLs with status code 404 must return nil')
      quit()
      test.mustBeTrue(get('http://'..ip..':'..port..'/yes') == nil, 'URLs with failed connects must return nil')
   end,
   testTensor = function()
      local get = Getters(nil, 'Tensor')
      local x = torch.randn(3, 3)
      test.mustBeTrue(get(x) == x, 'tensor getter must return tensor')
   end,
}
