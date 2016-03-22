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
   "regress",
   "luasocket",
   "luafilesystem",
   "paths",
   "ipc",
   "murmurhash3 >= 1.3",
}

build = {
   type = "command",
   build_command = [[
cmake -E make_directory build;
cd build;
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$(LUA_BINDIR)/.." -DCMAKE_INSTALL_PREFIX="$(PREFIX)" -DCMAKE_C_FLAGS=-fPIC -DCMAKE_CXX_FLAGS=-fPIC;
$(MAKE)
   ]],
   install_command = "cd build && $(MAKE) install"
}
