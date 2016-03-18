#include <TH/TH.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
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

/*
   This was all extracted from the ancient elephant bird scrolls
   https://github.com/twitter/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/mapreduce/io/BinaryBlockReader.java
*/

#define MARKER_SIZE (16)
static uint8_t _marker[MARKER_SIZE] = {
   0x29, 0xd8, 0xd5, 0x06, 0x58, 0xcd, 0x4c, 0x29,
   0xb2, 0xbc, 0x57, 0x99, 0x21, 0x71, 0xbd, 0xff
};

static int consume_marker(FILE *file, int scan) {
   uint8_t buff[MARKER_SIZE];
   size_t n = fread(buff, 1, MARKER_SIZE, file);
   if (n != MARKER_SIZE) {
      return 0;
   }
   while (memcmp(buff, _marker, MARKER_SIZE) != 0) {
      if (!scan) return 0;
      memmove(buff, buff + 1, MARKER_SIZE - 1);
      n = fread(buff + MARKER_SIZE - 1, 1, 1, file);
      if (n != 1) {
         return 0;
      }
   }
   return 1;
}

static int read_int(FILE *file) {
   uint8_t buff[4];
   size_t n = fread(buff, 1, 4, file);
   if (n != 4) {
      return -1;
   }
   return (int)buff[0] | ((int)buff[1] << 8) | ((int)buff[2] << 16) | ((int)buff[3] << 24);
}

static int unpack_tag_and_wiretype(FILE *file, uint32_t *tag, uint32_t *wiretype) {
   uint8_t x;
   size_t n = fread(&x, 1, 1, file);
   if (n != 1) {
      return -1;
   }
   *tag = (x & 0x7f) >> 3;
   *wiretype = x & 7;
   if ((x & 0x80) == 0) {
      return 0;
   }
   return -1;
}

static int unpack_varint_i32(FILE *file) {
   int value = 0;
   for (int i = 0; i < 10; i++) {
      uint8_t x;
      size_t n = fread(&x, 1, 1, file);
      if (n != 1) {
         return -1;
      }
      value |= ((int)x & 0x7F) << (i * 7);
      if ((x & 0x80) == 0) break;
   }
   return value;
}

static int unpack_string(FILE *file, char *out, size_t max_out_len) {
   int len = unpack_varint_i32(file);
   if (len < 0) return -1;
   size_t slen = len;
   if (slen + 1 > max_out_len) return -1;
   size_t n = fread(out, 1, slen, file);
   if (n != slen) return -1;
   out[n] = 0;
   return 0;
}

int dataset_offsets(lua_State *L) {
   const char* filename = lua_tostring(L, 1);
   FILE* file = fopen(filename, "r");
   if (!file) return LUA_HANDLE_ERROR(L, errno);
   int do_block_read = consume_marker(file, 0);
   fseek(file, 0, SEEK_SET);
   THLongStorage* offsets = THLongStorage_newWithSize(1024);
   long num_offsets = 0;
   if (do_block_read) {
      while (consume_marker(file, 1)) {
         int block_size = read_int(file);
         if (block_size > 0) {
            long block_end = ftell(file) + block_size;
            uint32_t tag, wiretype;
            if (unpack_tag_and_wiretype(file, &tag, &wiretype)) return LUA_HANDLE_ERROR_STR(L, "unsupported tag and wiretype");
            int version = unpack_varint_i32(file);
            if (version != 1) return LUA_HANDLE_ERROR_STR(L, "unsupported version");
            if (unpack_tag_and_wiretype(file, &tag, &wiretype)) return LUA_HANDLE_ERROR_STR(L, "unsupported tag and wiretype");
            char class_name[1024];
            if (unpack_string(file, class_name, 1023)) return LUA_HANDLE_ERROR_STR(L, "unsupported class name");
            while (ftell(file) < block_end) {
               if (unpack_tag_and_wiretype(file, &tag, &wiretype)) return LUA_HANDLE_ERROR_STR(L, "unsupported tag and wiretype");
               int record_size = unpack_varint_i32(file);
               if (offsets->size == num_offsets) {
                  THLongStorage_resize(offsets, offsets->size + 1024);
               }
               offsets->data[num_offsets] = ftell(file);
               num_offsets++;
               fseek(file, record_size, SEEK_CUR);
            }
         }
      }
      if (offsets->size == num_offsets) {
         THLongStorage_resize(offsets, offsets->size + 1024);
      }
      offsets->data[num_offsets] = ftell(file);
      num_offsets++;
   } else {
      size_t offset = 0;
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
      if (offsets->size == num_offsets) {
         THLongStorage_resize(offsets, offsets->size + 1);
      }
      offsets->data[num_offsets] = offset;
      num_offsets++;
   }
   THLongStorage_resize(offsets, num_offsets);
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
