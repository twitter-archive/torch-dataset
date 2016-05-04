#include "luaT.h"
#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include <limits.h>
#include <sys/stat.h>
#include <string.h>

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

int dataset_dirsize(lua_State *L) {
   const char *dir_name = lua_tostring(L, 1);
   DIR *dir = opendir(dir_name);
   fprintf(stderr, "dir = %p\n", dir);
   if (!dir) {
      lua_pushinteger(L, -errno);
      return 1;
   }
   struct dirent *d;
   char file_name[PATH_MAX];
   size_t total = 0;
   while ((d = readdir(dir)) != NULL) {
      fprintf(stderr, "d = %p, %d\n", d, d->d_type);
      if (d->d_type == DT_REG) {
         snprintf(file_name, PATH_MAX, "%s/%s", dir_name, d->d_name);
         struct stat buf;
         int ret = stat(file_name, &buf);
         fprintf(stderr, "stat(%s) = %d\n", file_name, ret);
         if (!ret) {
            total += buf.st_size;
         } else if (errno != ENOENT) {
            closedir(dir);
            return luaL_error(L, "dataset.dirsize(%s): %s", file_name, strerror(errno));
         }
      }
   }
   closedir(dir);
   lua_pushinteger(L, total);
   return 1;
}

static const struct luaL_Reg dataset_routines[] = {
   {"link", dataset_link},
   {"symlink", dataset_symlink},
   {"dirsize", dataset_dirsize},
   {NULL, NULL}
};

DLL_EXPORT int luaopen_libdataset(lua_State *L) {
   lua_newtable(L);
   luaT_setfuncs(L, dataset_routines, 0);
   return 1;
}
