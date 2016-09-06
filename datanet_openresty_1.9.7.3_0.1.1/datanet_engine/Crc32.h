
#ifndef __DATANET_CRC32__H
#define __DATANET_CRC32__H

uint32_t crc32_16bytes(const void* data, size_t length, uint32_t previousCrc32 = 0);


#endif
