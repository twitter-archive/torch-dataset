package = "dataset"
version = "scm-1"

source = {
   url = "git://github.com/twitter/torch-dataset.git",
}

description = {
   summary = "A dataset library, for Torch",
   homepage = "-",
   license = "MIT",
}

dependencies = {
   "torch >= 7.0",
   "luasocket",
   "luafilesystem",
   "paths",
   "parallel",
   "murmurhash3",
}

build = {
   type = "builtin",
   modules = {
      ['dataset.Batch'] = 'lua/Batch.lua',
      ['dataset.Cache'] = 'lua/Cache.lua',
      ['dataset.Dataset'] = 'lua/Dataset.lua',
      ['dataset.Getters'] = 'lua/Getters.lua',
      ['dataset.Index'] = 'lua/Index.lua',
      ['dataset.IndexCSV'] = 'lua/IndexCSV.lua',
      ['dataset.IndexDirectory'] = 'lua/IndexDirectory.lua',
      ['dataset.IndexSlowFS'] = 'lua/IndexSlowFS.lua',
      ['dataset.IndexTensor'] = 'lua/IndexTensor.lua',
      ['dataset.IndexUtils'] = 'lua/IndexUtils.lua',
      ['dataset.Reader'] = 'lua/Reader.lua',
      ['dataset.Sampler'] = 'lua/Sampler.lua',
      ['dataset.SlowFS'] = 'lua/SlowFS.lua',
   },
}
