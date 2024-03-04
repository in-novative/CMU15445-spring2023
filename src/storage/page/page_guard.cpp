#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept 
{
    //memcpy(this->bpm_, that.bpm_, sizeof(BufferPoolManager));
    //memcpy(this->page_, that.page_, BUSTUB_PAGE_SIZE);
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    this->page_->pin_count_++;

    that.Drop();
}

void BasicPageGuard::Drop()
{
    page_->pin_count_--;
    bpm_->DeletePage(page_->page_id_);                    //? 在test中page的资源会被锁住，没有找到问题来源
    //memset(bpm_, 0, sizeof(BufferPoolManager));
    //memset(page_, 0, BUSTUB_PAGE_SIZE);
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & 
{
    this->Drop();
    //memcpy(this->bpm_, that.bpm_, sizeof(BufferPoolManager));
    //memcpy(this->page_, that.page_, BUSTUB_PAGE_SIZE);
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    this->page_->pin_count_++;
    return *this;    
}
BasicPageGuard::~BasicPageGuard(){};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
{
    this->latch_.lock();
    that.latch_.lock();
    this->guard_ = std::move(that.guard_);
    that.Drop();
    this->latch_.unlock();
    that.latch_.unlock();
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &
{
    this->latch_.lock();
    that.latch_.lock();
    this->Drop();
    this->guard_ = std::move(that.guard_);
    this->latch_.unlock();
    that.latch_.unlock();
    return *this;
}

void ReadPageGuard::Drop()
{
    this->latch_.lock();
    this->guard_.Drop();
    this->latch_.unlock();
}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
{
    this->latch_.lock();
    that.latch_.lock();
    this->guard_ = std::move(that.guard_);
    that.Drop();
    this->latch_.unlock();
    that.latch_.unlock();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard &
{
    this->latch_.lock();
    that.latch_.lock();
    this->Drop();
    this->guard_ = std::move(that.guard_);
    this->latch_.unlock();
    that.latch_.unlock();
    return *this;
}

void WritePageGuard::Drop()
{
    this->latch_.lock();
    this->guard_.Drop();
    this->latch_.unlock();
}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub