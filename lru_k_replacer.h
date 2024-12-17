#pragma once

#include <limits>
#include <list>
#include <mutex> // NOLINT
#include <unordered_map>
#include <vector>
#include <deque>

#include "common/config.h"
#include "common/macros.h"

namespace bustub
{

  /**
   * LRUKReplacer implements the LRU-k replacement policy.
   */
  class LRUKReplacer
  {
  public:
    explicit LRUKReplacer(size_t num_frames, size_t k);

    DISALLOW_COPY_AND_MOVE(LRUKReplacer);

    ~LRUKReplacer() = default;

    auto Evict(frame_id_t *frame_id) -> bool;

    void RecordAccess(frame_id_t frame_id);

    void SetEvictable(frame_id_t frame_id, bool set_evictable);

    void Remove(frame_id_t frame_id);

    auto Size() -> size_t;

  private:
    [[maybe_unused]] size_t current_timestamp_{0}; // 时间戳
    [[maybe_unused]] size_t curr_size_{0};         // 可驱逐的帧数量
    [[maybe_unused]] size_t replacer_size_;        // 总帧数限制
    [[maybe_unused]] size_t k_;
    std::mutex latch_;

    // frame_data_：表示存储每个页面的相关数据，包括访问历史和是否可驱逐状态。
    // evict_frame：表示被驱逐的页面。
    // max_time_diff：表示最大时间差。
    // access_history：表示访问历史。

    struct AccessHistory
    {
      std::list<size_t> access_times;
      bool is_evictable{false};
    };

    std::unordered_map<frame_id_t, AccessHistory> frame_data_;

    std::list<frame_id_t> history_list_;
    std::list<frame_id_t> cache_list_;
  };

} // namespace bustub
