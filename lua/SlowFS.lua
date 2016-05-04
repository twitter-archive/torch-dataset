
local constructors = { }

local function split(x, y)
   local i, j = x:find(y)
   if i ~= nil then
      return { x:sub(1, i - 1), x:sub(j + 1) }
   else
      return { }
   end
end

local function register(url, constructor)
   constructors[split(url, "://")[1]] = constructor
end

local function find(url)
   return constructors[split(url, "://")[1]]
end

-- Hack for Cortex
register('viewfs://', function(cache, opt)
   return require 'hdfs.HDFS'(opt and opt.cluster, opt and opt.user, opt, cache)
end)

return {
   register = register,
   find = find,
}
