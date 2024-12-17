#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

 

namespace bustub {

/**
 * @brief ExtendibleHashTable 类的构造函数
 * 
 * 初始化哈希表，包括设置桶大小并创建第一个桶。
 * 
 * @tparam K 键的类型
 * @tparam V 值的类型
 * @param bucket_size 每个桶的最大元素数量
 */
template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : bucket_size_(bucket_size), dir_{std::make_shared<Bucket>(bucket_size, 0)} {
}

/**
 * @brief 计算给定键的哈希索引
 * 
 * 使用哈希函数与全局深度掩码来定位桶。
 * 
 * @param key 要哈希的键
 * @return size_t 返回计算出的目录索引
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

/**
 * @brief 获取全局深度
 * 
 * @return int 返回当前全局深度
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return global_depth_;
}

/**
 * @brief 获取指定目录索引的桶的局部深度
 * 
 * @param dir_index 目录索引
 * @return int 返回指定桶的局部深度
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[dir_index]->GetDepth();
}

/**
 * @brief 获取当前桶的数量
 * 
 * @return int 返回桶的总数量
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return num_buckets_;
}

/**
 * @brief 查找指定键的值
 * 
 * @param key 要查找的键
 * @param value 存储找到的值的引用
 * @return true 如果找到键，返回 true；否则返回 false
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

/**
 * @brief 从哈希表中移除指定键
 * 
 * @param key 要移除的键
 * @return true 如果成功移除，返回 true；否则返回 false
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

/**
 * @brief 向哈希表中插入键值对
 * 
 * 如果桶已满，会扩展目录并分裂桶。
 * 
 * @param key 要插入的键
 * @param value 要插入的值
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (true) {
    size_t index = IndexOf(key);
    auto bucket = dir_[index];

    if (bucket->Insert(key, value)) {
      return; // 插入成功
    }

    if (global_depth_ == bucket->GetDepth()) {
      DirectoryExtension(); // 扩展目录
      global_depth_++;
    }
    SplitTheBucket(key); // 分裂桶
  }
}

/**
 * @brief 扩展目录大小
 * 
 * 将目录大小加倍，并将旧目录的指针复制到新目录中。
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::DirectoryExtension() {
  size_t old_size = dir_.size();
  dir_.resize(2 * old_size);
  std::copy(dir_.begin(), dir_.begin() + old_size, dir_.begin() + old_size);
}

/**
 * @brief 分裂桶，将原桶的数据重新分配到两个新桶中
 * 
 * @param key 用于确定要分裂的桶
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitTheBucket(const K &key) {
  size_t index = IndexOf(key);
  auto bucket = dir_[index];
  int local_depth = bucket->GetDepth();
  bucket->IncrementDepth();

  auto new_bucket_0 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  auto new_bucket_1 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());

  size_t depth_mask = (1 << bucket->GetDepth()) - 1;
  size_t split_bit = 1 << local_depth;

  for (size_t i = 0; i < dir_.size(); i++) {
    if (dir_[i] == bucket) {
      dir_[i] = (i & split_bit) ? new_bucket_1 : new_bucket_0;
    }
  }

  ++num_buckets_;
  for (const auto &item : bucket->GetItems()) {
    size_t new_index = std::hash<K>()(item.first) & depth_mask;
    if (new_index & split_bit) {
      new_bucket_1->Insert(item.first, item.second);
    } else {
      new_bucket_0->Insert(item.first, item.second);
    }
  }
}

// ========================== Bucket 类实现 ==========================

/**
 * @brief 构造 Bucket 对象
 * 
 * @param array_size 桶的最大大小
 * @param depth 桶的初始局部深度
 */
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth)
    : size_(array_size), depth_(depth) {
}

/**
 * @brief 在桶中查找指定键的值
 * 
 * @param key 要查找的键
 * @param value 存储找到的值的引用
 * @return true 如果找到键，返回 true；否则返回 false
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [&](const auto &item) {
    return item.first == key;
  });
  if (it != list_.end()) {
    value = it->second;
    return true;
  }
  return false;
}

/**
 * @brief 从桶中移除指定的键值对
 * 
 * @param key 要移除的键
 * @return true 如果移除成功，返回 true；否则返回 false
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [&](const auto &item) {
    return item.first == key;
  });
  if (it != list_.end()) {
    list_.erase(it);
    return true;
  }
  return false;
}

/**
 * @brief 向桶中插入键值对
 * 
 * @param key 要插入的键
 * @param value 对应的值
 * @return true 如果插入成功，返回 true；如果桶已满，返回 false
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [&](const auto &item) {
    return item.first == key;
  });
  if (it != list_.end()) {
    it->second = value;  // 更新已有键的值
    return true;
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

// ========================== 模板类显式实例化 ==========================

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

