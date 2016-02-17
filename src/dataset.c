#include <TH/TH.h>
#include <unistd.h>
#include <errno.h>
#include "luaT.h"
#include "error.h"

int dataset_link(lua_State *L) {
   const char *src = lua_tostring(L, 1);
   const char *dst = lua_tostring(L, 2);
   int ret = link(src, dst);
   lua_pushinteger(L, ret < 0 ? errno : ret);
   return 1;
}

int dataset_symlink(lua_State *L) {
   const char *src = lua_tostring(L, 1);
   const char *dst = lua_tostring(L, 2);
   int ret = symlink(src, dst);
   lua_pushinteger(L, ret < 0 ? errno : ret);
   return 1;
}

int dataset_offsets(lua_State *L) {
   const char* filename = lua_tostring(L, 1);
   FILE* file = fopen(filename, "r");
   if (!file) return LUA_HANDLE_ERROR(L, errno);
   THLongStorage* offsets = THLongStorage_newWithSize(1024);
   size_t offset = 0;
   long num_offsets = 0;
   offsets->data[num_offsets] = offset;
   num_offsets++;
   char buff[8192];
   int eol = 0;
   while (1) {
      size_t n = fread(buff, 1, sizeof(buff), file);
      for (size_t i = 0; i < n; i++) {
         if (buff[i] == '\n') {
            eol = 1;
         } else if (eol) {
            if (offsets->size == num_offsets) {
               THLongStorage_resize(offsets, offsets->size + 1024);
            }
            offsets->data[num_offsets] = offset;
            num_offsets++;
            eol = 0;
         }
         offset++;
      }
      if (n != sizeof(buff)) {
         break;
      }
   }
   fclose(file);
   if (!eol) return LUA_HANDLE_ERROR_STR(L, "file did not end on a newline");
   THLongStorage_resize(offsets, num_offsets + 1);
   offsets->data[num_offsets] = offset;
   THLongStorage *size = THLongStorage_newWithSize(1);
   size->data[0] = offsets->size;
   THLongTensor *tensor = THLongTensor_newWithStorage(offsets, 0, size, NULL);
   THLongStorage_free(size);
   luaT_pushudata(L, tensor, "torch.LongTensor");
   return 1;
}

static const struct luaL_Reg dataset_routines[] = {
   {"link", dataset_link},
   {"symlink", dataset_symlink},
   {"offsets", dataset_offsets},
   {NULL, NULL}
};

DLL_EXPORT int luaopen_libdataset(lua_State *L) {
   lua_newtable(L);
   luaT_setfuncs(L, dataset_routines, 0);
   return 1;
}
