//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool
{
	latch_.lock();
	bool isInfinity = false;
	size_t kMax = 0;
	size_t earlistTimestamp = std::numeric_limits<size_t>::max();
	for(auto node : node_store_){
		if(!node.second.is_evictable_)
			continue;
		if(node.second.kDistance == std::numeric_limits<size_t>::infinity() && (earlistTimestamp > node.second.history_.back())){
			isInfinity = true;
			kMax = node.second.kDistance;
			earlistTimestamp = node.second.history_.back();
			*frame_id = node.first;
		}
		else if(!isInfinity && node.second.kDistance > kMax){
			kMax = node.second.kDistance;
			*frame_id = node.first;
		}
	}
	if(isInfinity || kMax > 0){
		curr_size_--;										//decrease the size of replacer
		node_store_[*frame_id].history_.clear();			//remove the frame's access history
		node_store_.erase(*frame_id);
		latch_.unlock();
		//std::cout << "[Info] evict " << *frame_id << std::endl;
		return true;
	}
	else{
		latch_.unlock();
		//std::cout << "[Info] evict failed" << std::endl;
		return false;
	}
}

//! What is access_type? How can I get it?
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]]AccessType access_type)
{
	if(static_cast<size_t>(frame_id) > replacer_size_){
		//TODO throw out an exception
		latch_.unlock();
		return ;
	}
	latch_.lock();
	if(node_store_.find(frame_id) == node_store_.end()){		//frame_id didn't exist in node_store_
		LRUKNode thisFrame;
		thisFrame.history_ = std::list<size_t>{current_timestamp_};
		thisFrame.kDistance = std::numeric_limits<size_t>::infinity();
		thisFrame.is_evictable_ = false;
		node_store_.insert(std::make_pair(frame_id, thisFrame));
	}
	else{
		node_store_[frame_id].history_.emplace_back(current_timestamp_);
		if(node_store_[frame_id].history_.size() >= k_){
			auto ptr = node_store_[frame_id].history_.end();
			for(size_t i=0; i<k_; i++)
				ptr = std::prev(ptr);
			node_store_[frame_id].kDistance = current_timestamp_ - *ptr;
		}
	}
	current_timestamp_++;
	latch_.unlock();
	return ;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable)
{
	if(static_cast<size_t>(frame_id) > replacer_size_){			//illegal id(lager than the maximum id of replacer)
		//TODO throw out an exception
		return ;
	}
	latch_.lock();
	if(node_store_[frame_id].is_evictable_ && !set_evictable){
		curr_size_--;
	}
	else if(!node_store_[frame_id].is_evictable_ && set_evictable){
		curr_size_++;
	}
	node_store_[frame_id].is_evictable_ = set_evictable;
	latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id)
{
	if(static_cast<size_t>(frame_id) > replacer_size_){
		//TODO throw out an exception
		return ;
	}
	latch_.lock();
	if(node_store_.find(frame_id) == node_store_.end()){			//didn't find specific frame id
		latch_.unlock();
		return ;
	}
	if(node_store_[frame_id].is_evictable_){
		node_store_[frame_id].history_.clear();
		node_store_.erase(frame_id);
		curr_size_--;
		latch_.unlock();
		return;
	}
	else{
		//TODO throw out an exception
		latch_.unlock();
		return ;
	}
}

auto LRUKReplacer::Size() -> size_t
{
	latch_.lock();
	/*for(auto node : node_store_){
		std::cout << "[Info] " << node.first;
		if(node.second.is_evictable_)
			std::cout << "  T" << std::endl;
		else
			std::cout << "  F" << std::endl;
	}
	std::cout << ">>>>>>>>>>>>" << std::endl;*/
	size_t result = curr_size_;
	latch_.unlock();
	return result;
}
}  
// namespace bustub