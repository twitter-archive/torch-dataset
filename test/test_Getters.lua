local test = require 'regress'
local Getters = require 'dataset.Getters'
local paths = require 'paths'

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
      local get = Getters('http')
      test.mustBeTrue(get('http://torch.data.s3.amazonaws.com/dataset/cifar10/2914591350') == nil, 'URLs with status code 404 must return nil')
      test.mustBeTrue(get('http://torch.data.s3.amazonaws.com/dataset/cifar10/2914591350.png', 13, 2) == 'HD', 'URLs with offset and length should work')
      test.mustBeTrue(get('http://torch.data.s3.amazonaws.com/dataset/cifar10/2914591350.png') ~= nil, 'URLs with status code 200 must return non-nil')
      test.mustBeTrue(get('http://xtorch.data.s3.amazonaws.comx/dataset/cifar10/2914591350.png') == nil, 'URLs with invalid host names must return nil')
   end,
   testTensor = function()
      local get = Getters(nil, 'Tensor')
      local x = torch.randn(3, 3)
      test.mustBeTrue(get(x) == x, 'tensor getter must return tensor')
   end,
}
