#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

void *new_fst_builder(void);

int32_t add_key(void *arg, const uint8_t *key, uint32_t len, uint64_t value);

const uint8_t *bytes(void *arg, uint32_t *len, uint32_t *cap);

void *load(uint8_t *key, uint32_t len, uint32_t cap);

int64_t get(void *arg, const uint8_t *key, uint32_t len);

int64_t get_first_key(void *arg, const uint8_t *key, uint32_t len);

int32_t finish(void *arg);