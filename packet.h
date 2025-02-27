#ifndef DBSCALE_PACKET_H
#define DBSCALE_PACKET_H

/*
 * Copyright (C) 2012, 2013 Great OpenSource Inc. All Rights Reserved.
 */

#include <ace/Message_Block.h>
#include <memory_allocater.h>
namespace dbscale {

#define DEFAULT_PACKET_SIZE 1024
#define DEFAULT_MAX_LOAD_SELECT_LEN (1024 * 16)

typedef ACE_Data_Block DataBlock;

inline void copy_string(char *target, const char *source, size_t len) {
  strncpy(target, source ? source : "", len - 1);
  target[len - 1] = '\0';
}

class Packet : public ACE_Message_Block {
 public:
  static const size_t default_packet_size = 1024;
  Packet(size_t size = default_packet_size, PacketAllocater *alloc = NULL);

  Packet(char *buffer, size_t size = default_packet_size);

  static void packchar(char *packet, char value) { *(packet) = value; }
  static void pack2int(char *packet, int value) {
    *(packet) = (char)(value);
    *(packet + 1) = (char)((unsigned int)value >> 8);
  }
  static void pack3int(char *packet, int value) {
    *(packet) = (char)(value);
    *(packet + 1) = (char)((unsigned int)value >> 8);
    *(packet + 2) = (char)((unsigned int)value >> 16);
  }
  static void pack4int(char *packet, int value) {
    *(uint32_t *)packet = (uint32_t)value;
  }
  static uint8_t unpackuchar(const char *packet) { return *(uint8_t *)packet; }
  static uint16_t unpack2uint(const char *packet) {
    return *(uint16_t *)packet;
  }
  static uint32_t unpack3uint(const char *packet) {
    return ((uint32_t)((unsigned char)packet[0]) +
            ((uint32_t)((unsigned char)packet[1]) << 8) +
            ((uint32_t)((unsigned char)packet[2]) << 16));
  }
  static uint32_t unpack4uint(const char *packet) {
    return *(uint32_t *)packet;
  }

  static void pack8int(char *packet, long long value) {
    char *p = packet;
    *(p++) = (char)(value);
    *(p++) = (char)((unsigned long long)value >> 8);
    *(p++) = (char)((unsigned long long)value >> 16);
    *(p++) = (char)((unsigned long long)value >> 24);
    *(p++) = (char)((unsigned long long)value >> 32);
    *(p++) = (char)((unsigned long long)value >> 40);
    *(p++) = (char)((unsigned long long)value >> 48);
    *(p++) = (char)((unsigned long long)value >> 56);
  }

  static uint64_t unpack8uint(const char *packet) {
    const char *p = packet;
    uint64_t value = (uint64_t)(((uint64_t)((unsigned char)p[0])) +
                                ((uint64_t)((unsigned char)p[1]) << 8) +
                                ((uint64_t)((unsigned char)p[2]) << 16) +
                                ((uint64_t)((unsigned char)p[3]) << 24) +
                                ((uint64_t)((unsigned char)p[4]) << 32) +
                                ((uint64_t)((unsigned char)p[5]) << 40) +
                                ((uint64_t)((unsigned char)p[6]) << 48) +
                                ((uint64_t)((unsigned char)p[7]) << 56));
    return value;
  }

  static uint64_t unpack_lenenc_int(char const **packet);
  static char *pack_lenenc_int(char *packet, unsigned long long value);

  size_t tell() const { return rd_ptr() - base(); }
  void rewind() {
    this->wr_ptr(this->base());
    this->rd_ptr(this->base());
  }
  void skip(size_t offset) { this->rd_ptr(offset); }
  void fill(const char filler, size_t len) {
    char *p = wr_ptr();
    memset(p, filler, len);
    p += len;
    wr_ptr(p);
  }
  void packchar(const char value) {
    char *p = wr_ptr();
    *p++ = value;
    wr_ptr(p);
  }
  void packstr(const char *value) {
    size_t len = strlen(value) + 1;
    packdata(value, len);
  }
  void packstr(const char *value, size_t size) {
    size_t len = strnlen(value, size - 1);
    packdata(value, len);
    packchar('\0');
  }
  void packdata(const char *data, size_t len) {
    char *p = wr_ptr();
    memcpy(p, data, len);
    p += len;
    wr_ptr(p);
  }
  void pack2int(int value) {
    char *p = wr_ptr();
    pack2int(p, value);
    p += 2;
    wr_ptr(p);
  }
  void pack3int(int value) {
    char *p = wr_ptr();
    pack3int(p, value);
    p += 3;
    wr_ptr(p);
  }
  void pack4int(int value) {
    char *p = wr_ptr();
    pack4int(p, value);
    p += 4;
    wr_ptr(p);
  }
  void pack8int(uint64_t value) {
    char *p = wr_ptr();
    pack8int(p, value);
    p += 8;
    wr_ptr(p);
  }
  /**
   * Unpack given length of data and copy to target.
   */
  void unpackdata(char *target, size_t len) {
    char *p = rd_ptr();
    memcpy(target, p, len);
    p += len;
    rd_ptr(p);
  }
  /**
   * Unpack given length of data without copying it.
   */
  char *unpackdata(size_t len) {
    char *p = rd_ptr();
    rd_ptr(len);
    return p;
  }
  /**
   * Unpack the rest of packet without copying it.
   */
  char *unpackeof(size_t pkt_length, size_t *len) {
    // we use pkt_length instead of length(), because length() is sensitive
    // with wr_ptr() and rd_ptr(), these two values have been reset by rewind()
    *len = pkt_length - tell();
    return unpackdata(*len);
  }
  char *unpackstr() {
    char *s = rd_ptr();
    char *p = s;
    // packet buffer must be NULL terminated in all cases
    size_t len = strlen(p);
    p += len + 1;
    rd_ptr(p);
    return s;
  }
  bool has_null_term_str(size_t max_search_len, size_t *null_len) {
    char *s = rd_ptr();
    char *str = (char *)memchr(s, '\0', max_search_len);
    if (!str) return false;
    *null_len = str - s;
    return true;
  }
  void unpackstr(char *target, size_t len) {
    strncpy(target, unpackstr(), len - 1);
    target[len] = '\0';
  }
  char unpackchar() {
    char *p = rd_ptr();
    char c = *p++;
    rd_ptr(p);
    return c;
  }
  unsigned char unpackuchar() { return unpackchar(); }
  static uint8_t unpackuchar(char **pp) {
    char *p = *pp;
    uint8_t value = unpackuchar(p);
    *pp += 1;
    return value;
  }
  uint16_t unpack2uint_() {
    char *p = rd_ptr();
    uint16_t value = unpack2uint(p);
    p += 2;
    rd_ptr(p);
    return value;
  }
  static uint16_t unpack2uint(char **pp) {
    char *p = *pp;
    uint16_t value = unpack2uint(p);
    *pp += 2;
    return value;
  }
  uint16_t unpack2uint() {
    char *p = rd_ptr();
    uint16_t value = unpack2uint(&p);
    rd_ptr(p);
    return value;
  }
  uint32_t unpack3uint_() {
    char *p = rd_ptr();
    uint32_t value = unpack3uint(p);
    p += 3;
    rd_ptr(p);
    return value;
  }
  static uint32_t unpack3uint(char **pp) {
    char *p = *pp;
    uint32_t value = unpack3uint(p);
    *pp += 3;
    return value;
  }
  uint32_t unpack3uint() {
    char *p = rd_ptr();
    uint32_t r = unpack3uint(&p);
    rd_ptr(p);
    return r;
  }
  uint32_t unpack4uint_() {
    char *p = rd_ptr();
    uint32_t value = unpack4uint(p);
    p += 4;
    rd_ptr(p);
    return value;
  }
  static uint32_t unpack4uint(char **pp) {
    char *p = *pp;
    uint32_t value = unpack4uint(p);
    *pp += 4;
    return value;
  }
  uint32_t unpack4uint() {
    char *p = rd_ptr();
    uint32_t value = unpack4uint(&p);
    rd_ptr(p);
    return value;
  }
  uint64_t unpack8uint_() {
    char *p = rd_ptr();
    uint64_t value = unpack8uint(p);
    p += 8;
    rd_ptr(p);
    return value;
  }
  static uint64_t unpack8uint(char **pp) {
    char *p = *pp;
    uint64_t value = unpack8uint(p);
    *pp += 8;
    return value;
  }
  uint64_t unpack8uint() {
    char *p = rd_ptr();
    uint64_t value = unpack8uint(&p);
    rd_ptr(p);
    return value;
  }

  void pack_lenenc_int(unsigned long long value) {
    char *p = wr_ptr();
    p = pack_lenenc_int(p, value);
    wr_ptr(p);
  }

  static uint64_t unpack_lenenc_int(char **pp) {
    uint64_t r = unpack_lenenc_int((const char **)pp);
    return r;
  }
  uint64_t unpack_lenenc_int() {
    char *p = rd_ptr();
    uint64_t r = unpack_lenenc_int(&p);
    rd_ptr(p);
    return r;
  }

  void unpack_lenenc_str(char *target, size_t size) {
    uint64_t len = unpack_lenenc_int();
    if (len == static_cast<uint64_t>(~0)) len = 0;
    len = (len < size) ? len : size - 1;
    unpackdata(target, len);
    target[len] = '\0';
  }
  void unpack_lenenc_str(string &target) {
    uint64_t len = unpack_lenenc_int();
    if (len == static_cast<uint64_t>(~0)) len = 0;
    char *p = rd_ptr();
    target.clear();
    target.assign(p, len);
    p += len;
    rd_ptr(p);
  }

  void pack_lenenc_str(const char *source) {
    size_t len = strlen(source);
    pack_lenenc_str(source, len);
  }
  void pack_lenenc_str(const char *source, size_t len) {
    pack_lenenc_int(len);
    packdata(source, len);
  }
};

}  // namespace dbscale

#endif /* DBSCALE_PACKET_H */
