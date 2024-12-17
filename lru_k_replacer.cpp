#include "buffer/lru_k_replacer.h"

namespace bustub
{

  LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

  /*Evict淘汰帧：选择具有最大后退k-距离的帧进行淘汰
   遍历所有可淘汰的帧，计算其“后退k-距离”。
   后退k-距离是当前时间戳与第k次历史访问之间的差异。
   如果一个帧的历史访问次数小于k，则认为其后退k-距离为正无穷。
   如果有多个帧具有相同的最大后退k-距离，则选择时间戳最早的帧。
  */
  auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool
  {
    std::scoped_lock<std::mutex> lock(latch_);//加互斥锁
    // 如果没有可驱逐的页面，返回false
    if (curr_size_ == 0)
    {
        return false;
    }
    // 优先从历史访问列表中驱逐
    for (auto it = history_list_.begin(); it != history_list_.end(); ++it)
    {
      AccessHistory &history = frame_data_[*it]; // 获取页面的访问历史
      if (history.is_evictable)
      { // 判断该页面是否可驱逐
        *frame_id = *it;
        Remove(*frame_id); // 从存储中移除该页面
        return true;
      }
    }

    // 如果历史访问列表中没有可驱逐的页面，从缓存列表中寻找
    frame_id_t evict_frame = -1;
    size_t max_time_diff = 0;

    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it)
    {
      AccessHistory &history = frame_data_[*it]; // 获取页面的访问历史
      if (!history.is_evictable)
      {
        continue;
      }
      const auto &access_history = history.access_times;
      auto access_it = access_history.begin();
      std::advance(access_it, access_history.size() - k_); // 获取倒数第k次的访问时间
      size_t time_diff = current_timestamp_ - *access_it;  // 计算当前时间与上次访问的时间差

      // 选择时间差最大的页面作为驱逐目标
      if (evict_frame == -1 || time_diff > max_time_diff)
      {
        evict_frame = *it;
        max_time_diff = time_diff;
      }
    }

    if (evict_frame != -1)
    {
      *frame_id = evict_frame; // 返回驱逐的页面
      Remove(evict_frame);     // 从存储中移除该页面
      return true;
    }

    return false; // 如果没有可驱逐的页面，返回false
  }

  /*RecordAccess记录帧访问：更新帧的访问历史
   记录给定帧ID的访问时间戳。
   每次访问都会增加当前时间戳，并将其加入到该帧的访问历史中。
  */
  void LRUKReplacer::RecordAccess(frame_id_t frame_id)
  {
    std::scoped_lock<std::mutex> lock(latch_);
    BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id!");
    current_timestamp_++; // 更新时间戳
    // 处理新访问的页面，如果该页面没有记录
    auto &history = frame_data_[frame_id]; // 获取该页面的访问历史
    if (history.access_times.empty())
    {
      history.access_times.push_back(current_timestamp_); // 记录访问时间
      history_list_.push_back(frame_id);                  // 将页面添加到历史访问列表
      return;
    }

    // 更新页面的访问历史
    history.access_times.push_back(current_timestamp_);

    // 判断是否已达到k次访问，若达到则转移到缓存列表
    if (history.access_times.size() == k_)
    {
      history_list_.remove(frame_id);  // 从历史列表中移除
      cache_list_.push_back(frame_id); // 加入到缓存列表
    }
    else if (history.access_times.size() > k_)
    {
      // 如果访问次数超过k次，维护访问历史大小
      if (history.access_times.size() > k_ + 1)
      {
        history.access_times.pop_front(); // 保证历史记录不会超过k+1
      }
    }
  }

  /*SetEvictable设置帧的可淘汰状态
   控制指定帧是否可淘汰。
   当帧从不可淘汰变为可淘汰时，替换器的大小会增加；反之，替换器大小会减少。
  */
  void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable)
  {
    std::scoped_lock<std::mutex> lock(latch_);
    BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id!");

    auto it = frame_data_.find(frame_id);//查找frame_id
    if (it == frame_data_.end())
    {
      return; // 如果页面不存在，直接返回
    }

    AccessHistory &history = it->second; // 获取页面的访问历史
    if (history.is_evictable == set_evictable)
    {
      return; // 如果页面的可驱逐状态没有改变，直接返回
    }

    history.is_evictable = set_evictable; // 更新页面的可驱逐状态
    if (set_evictable)
    {
      curr_size_++; // 增加当前缓存中的页面数
    }
    else
    {
      curr_size_--; // 减少当前缓存中的页面数
    }
  }

  /*Remove移除指定帧
   清除该帧的访问历史,
   并且标记该帧为不可淘汰，减少替换器大小。
  */
  void LRUKReplacer::Remove(frame_id_t frame_id)
  {
    // 调用Remove时已经加了锁，不需要重复加锁
    auto it = frame_data_.find(frame_id);
    if (it == frame_data_.end())
    {
      return; // 未找到页面，直接返回
    }

    AccessHistory &history = it->second; // 获取页面的访问历史
    if (!history.is_evictable)
    {
      throw std::runtime_error("Cannot remove a non-evictable frame"); // 如果页面不可驱逐，抛出异常
    }

    // 从相应的列表中移除页面
    if (history.access_times.size() < k_)
    {
      history_list_.remove(frame_id); // 从历史访问列表中移除
    }
    else
    {
      cache_list_.remove(frame_id); // 从缓存列表中移除
    }
    // 从存储中删除该页面
    frame_data_.erase(frame_id);
    // 更新当前缓存中的页面数
    curr_size_--;
  }

  // Size:返回当前可淘汰帧的数量。
  auto LRUKReplacer::Size() -> size_t
  {
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_; // 返回当前缓存中的页面数量
  }

} // namespace bustub
