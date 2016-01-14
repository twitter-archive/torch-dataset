
local constructors = { }

local function register(url, constructor)
   constructors[url:split("://")[1]] = constructor
end

local function find(url)
   return constructors[url:split("://")[1]]
end

return {
   register = register,
   find = find,
}
