#pragma once
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include <atomic>

#include "common.h"

struct item;
typedef struct item item_t;
typedef item_t *item_handle_t;

// align structure to page size
struct control_block {
    std::atomic<std::uint64_t> head;
    std::atomic<std::uint64_t> tail;
    uint64_t size;
};

typedef struct control_block control_block_t;

struct circular_buffer {
    control_block_t cb;
    std::vector<lazylog::LogEntry *> buffer;
};

inline uint64_t next(struct circular_buffer *circ_buf, uint64_t p) { return (p + 1) % circ_buf->cb.size; }

inline struct circular_buffer *init(uint64_t capacity) {
    struct circular_buffer *buf = new struct circular_buffer;
    buf->cb.size = capacity + 1;
    buf->cb.head = 0;
    buf->cb.tail = 0;
    buf->buffer.resize(buf->cb.size);
    return buf;
}

inline bool push(struct circular_buffer *circ_buf, lazylog::LogEntry *itemp) {
    uint64_t tail = circ_buf->cb.tail.load(std::memory_order_relaxed);
    uint64_t next_tail = next(circ_buf, tail);

    if (next_tail != circ_buf->cb.head.load(std::memory_order_acquire)) {
        circ_buf->buffer[tail] = itemp;
        circ_buf->cb.tail.store(next_tail, std::memory_order_release);
        return true;
    }

    return false;
}

inline bool pop(struct circular_buffer *circ_buf, lazylog::LogEntry *&itemp) {
    uint64_t head = circ_buf->cb.head.load(std::memory_order_relaxed);
    uint64_t next_head;

    if (head == circ_buf->cb.tail.load(std::memory_order_acquire)) return false;

    next_head = next(circ_buf, head);
    itemp = circ_buf->buffer[head];
    circ_buf->cb.head.store(next_head, std::memory_order_release);
    return true;
}