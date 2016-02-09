
local constructors = { }

local function register(url, constructor)
   constructors[url:split("://")[1]] = constructor
end

local function find(url)
   return constructors[url:split("://")[1]]
end

-- Hack for Cortex
register('viewfs://', function(cache, opt)
   return require 'hdfs.HDFS'(opt and opt.cluster, opt and opt.user, opt, cache)
end)

return {
   register = register,
   find = find,
}
