#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

 

namespace bustub {

/**
 * @brief ExtendibleHashTable ��Ĺ��캯��
 * 
 * ��ʼ����ϣ����������Ͱ��С��������һ��Ͱ��
 * 
 * @tparam K ��������
 * @tparam V ֵ������
 * @param bucket_size ÿ��Ͱ�����Ԫ������
 */
template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : bucket_size_(bucket_size), dir_{std::make_shared<Bucket>(bucket_size, 0)} {
}

/**
 * @brief ����������Ĺ�ϣ����
 * 
 * ʹ�ù�ϣ������ȫ�������������λͰ��
 * 
 * @param key Ҫ��ϣ�ļ�
 * @return size_t ���ؼ������Ŀ¼����
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

/**
 * @brief ��ȡȫ�����
 * 
 * @return int ���ص�ǰȫ�����
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return global_depth_;
}

/**
 * @brief ��ȡָ��Ŀ¼������Ͱ�ľֲ����
 * 
 * @param dir_index Ŀ¼����
 * @return int ����ָ��Ͱ�ľֲ����
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[dir_index]->GetDepth();
}

/**
 * @brief ��ȡ��ǰͰ������
 * 
 * @return int ����Ͱ��������
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return num_buckets_;
}

/**
 * @brief ����ָ������ֵ
 * 
 * @param key Ҫ���ҵļ�
 * @param value �洢�ҵ���ֵ������
 * @return true ����ҵ��������� true�����򷵻� false
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

/**
 * @brief �ӹ�ϣ�����Ƴ�ָ����
 * 
 * @param key Ҫ�Ƴ��ļ�
 * @return true ����ɹ��Ƴ������� true�����򷵻� false
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

/**
 * @brief ���ϣ���в����ֵ��
 * 
 * ���Ͱ����������չĿ¼������Ͱ��
 * 
 * @param key Ҫ����ļ�
 * @param value Ҫ�����ֵ
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (true) {
    size_t index = IndexOf(key);
    auto bucket = dir_[index];

    if (bucket->Insert(key, value)) {
      return; // ����ɹ�
    }

    if (global_depth_ == bucket->GetDepth()) {
      DirectoryExtension(); // ��չĿ¼
      global_depth_++;
    }
    SplitTheBucket(key); // ����Ͱ
  }
}

/**
 * @brief ��չĿ¼��С
 * 
 * ��Ŀ¼��С�ӱ���������Ŀ¼��ָ�븴�Ƶ���Ŀ¼�С�
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::DirectoryExtension() {
  size_t old_size = dir_.size();
  dir_.resize(2 * old_size);
  std::copy(dir_.begin(), dir_.begin() + old_size, dir_.begin() + old_size);
}

/**
 * @brief ����Ͱ����ԭͰ���������·��䵽������Ͱ��
 * 
 * @param key ����ȷ��Ҫ���ѵ�Ͱ
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

// ========================== Bucket ��ʵ�� ==========================

/**
 * @brief ���� Bucket ����
 * 
 * @param array_size Ͱ������С
 * @param depth Ͱ�ĳ�ʼ�ֲ����
 */
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth)
    : size_(array_size), depth_(depth) {
}

/**
 * @brief ��Ͱ�в���ָ������ֵ
 * 
 * @param key Ҫ���ҵļ�
 * @param value �洢�ҵ���ֵ������
 * @return true ����ҵ��������� true�����򷵻� false
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
 * @brief ��Ͱ���Ƴ�ָ���ļ�ֵ��
 * 
 * @param key Ҫ�Ƴ��ļ�
 * @return true ����Ƴ��ɹ������� true�����򷵻� false
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
 * @brief ��Ͱ�в����ֵ��
 * 
 * @param key Ҫ����ļ�
 * @param value ��Ӧ��ֵ
 * @return true �������ɹ������� true�����Ͱ���������� false
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [&](const auto &item) {
    return item.first == key;
  });
  if (it != list_.end()) {
    it->second = value;  // �������м���ֵ
    return true;
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

// ========================== ģ������ʽʵ���� ==========================

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

