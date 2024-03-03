//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  /*throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");*/

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * 
{
    latch_.lock();
    Page* newPage = nullptr;
    page_id_t pageId;
    frame_id_t frameId;
    if(free_list_.size() > 0){                     //there is available page in free_list
        frameId = *free_list_.begin();
        free_list_.pop_front();                                         //get a frame_id
        pageId = AllocatePage();                                        //get a page_id
        page_table_.emplace(std::make_pair(pageId, frameId));           //combine the frame_id and page_id
        newPage = &pages_[frameId];                                     //get a new page
        newPage->ResetMemory();
        newPage->page_id_ = pageId;
        newPage->pin_count_++;
        *page_id = pageId;                                              //set page's id
        replacer_->SetEvictable(frameId, false);
        replacer_->RecordAccess(frameId);
    }
    else{                                                       //need to get new page from replacer_
        if(replacer_->Evict(&frameId) == true){
            if(pages_[frameId].is_dirty_){
                disk_manager_->WritePage(pages_[frameId].page_id_, pages_[frameId].data_);          //write dirty page back to disk
            }
            pageId = AllocatePage();                            //get a page_id
            newPage = &pages_[frameId];
            page_table_.erase(newPage->page_id_);
            page_table_[pageId] = frameId;                      //change the old page_id to the new one
            newPage->ResetMemory();
            newPage->page_id_ = pageId;
            newPage->pin_count_++;
            *page_id = pageId;
            replacer_->SetEvictable(frameId, false);
            replacer_->RecordAccess(frameId);
        }
        else{
            newPage = nullptr;
        }
    }
    latch_.unlock(); 
    return newPage;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
    latch_.lock();
    Page* fetchPage = nullptr;
    frame_id_t frameId;
    if(page_table_.find(page_id) != page_table_.end()){
        frameId = page_table_[page_id];
        fetchPage = &pages_[frameId];
        replacer_->RecordAccess(frameId);
    }
    else{
        if(free_list_.size() > 0){
            frameId = *free_list_.begin();
            free_list_.pop_front();
            page_table_.emplace(std::make_pair(page_id, frameId));
            fetchPage = &pages_[frameId];
            fetchPage->ResetMemory();
            fetchPage->page_id_ = page_id;
            fetchPage->pin_count_++;
            disk_manager_->ReadPage(page_id, fetchPage->data_);
            replacer_->SetEvictable(frameId, false);
            replacer_->RecordAccess(frameId);
        }
        else if(replacer_->Evict(&frameId) == true){
            fetchPage = &pages_[frameId];
            if(fetchPage->is_dirty_){
                disk_manager_->WritePage(fetchPage->page_id_, fetchPage->data_);          //write dirty page back to disk
            }
            fetchPage->ResetMemory();
            fetchPage->page_id_ = page_id;
            fetchPage->pin_count_++;
            page_table_.erase(fetchPage->page_id_);
            page_table_.emplace(page_id, frameId);
            disk_manager_->ReadPage(page_id, fetchPage->data_);                                 //read page from disk
            replacer_->SetEvictable(frameId, false);
            replacer_->RecordAccess(frameId);
        }
        else{
            fetchPage = nullptr;
        }
    }
    latch_.unlock();
    return fetchPage;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
    latch_.lock();
    bool result;
    frame_id_t frameId;
    Page* targetPage = nullptr;
    if(page_table_.find(page_id) == page_table_.end()){
        result = false;
    }
    else{
        frameId = page_table_[page_id];
        targetPage = &pages_[frameId];
        targetPage->is_dirty_ |= is_dirty;
        if(targetPage->pin_count_ == 0)
            result = false;
        else if(--targetPage->pin_count_ == 0){
            replacer_->SetEvictable(frameId, true);
            result = true;
        }
    }
    latch_.unlock();
    return result;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool
{
    latch_.lock();
    bool result;
    frame_id_t frameId;
    Page* targetPage = nullptr;
    if(page_table_.find(page_id) == page_table_.end())
        result = false;
    else{
        frameId = page_table_[page_id];
        targetPage = &pages_[frameId];
        disk_manager_->WritePage(page_id, targetPage->data_);
        targetPage->is_dirty_ = false;
        result = true;
    }
    latch_.unlock();
    return result;
}

void BufferPoolManager::FlushAllPages()
{
    latch_.lock();
    frame_id_t frameId;
    Page* targetPage = nullptr;
    for(auto page : page_table_){
        frameId = page.second;
        targetPage = &pages_[frameId];
        disk_manager_->WritePage(targetPage->page_id_, targetPage->data_);
        targetPage->is_dirty_ = false;
    }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool
{
    latch_.lock();
    Page* targetPage = nullptr;
    frame_id_t frameId;
    if(page_table_.find(page_id) == page_table_.end()){
        latch_.unlock();
        return true;
    }
    else{
        frameId = page_table_[page_id];
        targetPage = &pages_[frameId];
        if(targetPage->pin_count_ > 0){
            latch_.unlock();
            return false;
        }
        page_table_.erase(page_id);
        replacer_->Remove(frameId);
        free_list_.push_back(frameId);
        targetPage->ResetMemory();
        DeallocatePage(page_id);
        latch_.unlock();
        return true;
    }
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub