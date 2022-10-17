// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Kun  Ren  <kun.ren@yale.edu>
//

#include "fs/calvinfs_client_app.h"
#include "machine/app/app.h"

REGISTER_APP(CalvinFSClientApp) {
  return new CalvinFSClientApp();
}

MessageBuffer* CalvinFSClientApp::GetMetadataEntry(const Slice& path) {
  // Find out what machine to run this on.
  uint64 mds_machine =
      config_->LookupMetadataShard(config_->HashFileName(path), replica_);

  // Run if local.
  if (mds_machine == machine()->machine_id()) {
    Action a;
    //a.set_version(scheduler_->SafeVersion());

    LOG(ERROR)<<"li lun shang RPC shi zhe li hui zhi xing!"<<path.data()<<"  size is "<<path.size();
    a.set_action_type(MetadataAction::LOOKUP);
    MetadataAction::LookupInput in;
    in.set_path(path.data(), path.size());
    //gaoxuan --就是上面这小子把路径传错了，看看是不是这里in.path就设置失败了！！！如果是，这里就是症结所在
    LOG(ERROR)<<"shi bu shi zhe li in.path bu dui jin le :::"<<in.path();//gaoxuan --查过了，不是它，in.path是对的 
    
    in.SerializeToString(a.mutable_input());
    metadata_->GetRWSets(&a);//理论上这里都是没有任何问题的
    metadata_->Run(&a);
    return new MessageBuffer(a);

  // If not local, get result from the right machine (within this replica).
  } else {
    Header* header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(mds_machine);
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("LOOKUP");
    header->add_misc_string(path.data(), path.size());
    MessageBuffer* m = NULL;
    header->set_data_ptr(reinterpret_cast<uint64>(&m));
    machine()->SendMessage(header, new MessageBuffer());
    while (m == NULL) {
      usleep(10);
      Noop<MessageBuffer*>(m);
    }
    return m;
  }
}

MessageBuffer* CalvinFSClientApp::CreateFile(const Slice& path, FileType type) {
  string channel_name = "action-result-" + UInt64ToString(machine()->GetGUID());
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));

  Action* a = new Action();
  a->set_client_machine(machine()->machine_id());
  a->set_client_channel(channel_name);
  a->set_action_type(MetadataAction::CREATE_FILE);
  MetadataAction::CreateFileInput in;
  in.set_path(path.data(), path.size());
  in.set_type(type);
  in.SerializeToString(a->mutable_input());
  metadata_->GetRWSets(a);
  log_->Append(a);

  MessageBuffer* m = NULL;
  while (!channel->Pop(&m)) {
    // Wait for action to complete and be sent back.
    usleep(100);
  }

  Action result;
  result.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;
  MetadataAction::AppendOutput out;
  out.ParseFromString(result.output());

  if (out.success()) {
    return new MessageBuffer();
  } else {
    return new MessageBuffer(new string("error creating file/dir\n"));
  }
}

MessageBuffer* CalvinFSClientApp::AppendStringToFile(
    const Slice& data,
    const Slice& path) {
  // Write data block.
  uint64 block_id = machine()->GetGUID() * 2 + (data.size() > 1024 ? 1 : 0);
  blocks_->Put(block_id, data);

  string channel_name = "action-result-" + UInt64ToString(machine()->GetGUID());
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));

  // Update metadata.
  Action* a = new Action();
  a->set_client_machine(machine()->machine_id());
  a->set_client_channel(channel_name);
  a->set_action_type(MetadataAction::APPEND);
  MetadataAction::AppendInput in;
  in.set_path(path.data(), path.size());
  in.add_data();
  in.mutable_data(0)->set_length(data.size());
  in.mutable_data(0)->set_block_id(block_id);
  in.SerializeToString(a->mutable_input());
  metadata_->GetRWSets(a);
  log_->Append(a);

  MessageBuffer* m = NULL;
  while (!channel->Pop(&m)) {
    // Wait for action to complete and be sent back.
    usleep(100);
  }

  Action result;
  result.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;
  MetadataAction::AppendOutput out;
  out.ParseFromString(result.output());

  if (out.success()) {
    return new MessageBuffer();
  } else {
    return new MessageBuffer(new string("error appending string to file\n"));
  }
}

MessageBuffer* CalvinFSClientApp::ReadFile(const Slice& path) {
  MessageBuffer* serialized = GetMetadataEntry(path);
  Action a;
  a.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
  delete serialized;

  MetadataAction::LookupOutput out;
  out.ParseFromString(a.output());

  if (out.success() && out.entry().type() == DATA) {
    vector<MessageBuffer*> blocks(out.entry().file_parts_size(), NULL);
    for (int i = 0; i < out.entry().file_parts_size(); i++) {
      Header* header = new Header();
      header->set_from(machine()->machine_id());
      header->set_to(config_->LookupBlucket(config_->HashBlockID(
          out.entry().file_parts(i).block_id()),
          replica_));
      header->set_type(Header::RPC);
      header->set_app("blockstore");
      header->set_rpc("GET");
      header->add_misc_int(out.entry().file_parts(i).block_id());
      header->set_data_ptr(reinterpret_cast<uint64>(&blocks[i]));
      machine()->SendMessage(header, new MessageBuffer());
    }
    bool done = false;
    while (!done) {
      done = true;
      for (int i = 0; i < out.entry().file_parts_size(); i++) {
        Noop<MessageBuffer*>(blocks[i]);
        if (blocks[i] == NULL) {
          done = false;
          break;
        }
      }
    }

    MessageBuffer* result = new MessageBuffer(out.entry());
    for (int i = 0; i < out.entry().file_parts_size(); i++) {
      result->AppendPart(blocks[i]->PopBack());
    }
    return result;

  } else {
    return new MessageBuffer(new string("metadata lookup error\n"));
  }
}

MessageBuffer* CalvinFSClientApp::LS(const Slice& path) {
  MessageBuffer* serialized = GetMetadataEntry(path);
  Action a;
  a.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
  delete serialized;

  MetadataAction::LookupOutput out;
  out.ParseFromString(a.output());
  if (out.success() && out.entry().type() == DIR) {
    string* result = new string();
    for (int i = 0; i < out.entry().dir_contents_size(); i++) {
      result->append(out.entry().dir_contents(i));
      result->append("\n");
    }
    return new MessageBuffer(result);

  } else {
    return new MessageBuffer(new string("metadata lookup error\n"));
  }
}

MessageBuffer* CalvinFSClientApp::CopyFile(const Slice& from_path, const Slice& to_path) {
  uint64 distinct_id = machine()->GetGUID();
  string channel_name = "action-result-" + UInt64ToString(distinct_id);
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));

  Action* a = new Action();
  a->set_client_machine(machine()->machine_id());
  a->set_client_channel(channel_name);
  a->set_action_type(MetadataAction::COPY);

  MetadataAction::CopyInput in;
  in.set_from_path(from_path.data(), from_path.size());
  in.set_to_path(to_path.data(), to_path.size());
  in.SerializeToString(a->mutable_input());
  metadata_->GetRWSets(a);
  log_->Append(a);

  MessageBuffer* m = NULL;
  while (!channel->Pop(&m)) {
    // Wait for action to complete and be sent back.
    usleep(100);
  }

  Action result;
  result.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;
  MetadataAction::CopyOutput out;
  out.ParseFromString(result.output());

  if (out.success()) {
    return new MessageBuffer();
  } else {
    return new MessageBuffer(new string("error creating file/dir\n"));
  }
}

MessageBuffer* CalvinFSClientApp::RenameFile(const Slice& from_path, const Slice& to_path) {//gaoxuan --这里被调用了，应该这里才是rename的逻辑
  //LOG(ERROR)<<"Is it executed? --calvinfs_client_app.cc's RenameFile()";// gaoxuan --
  uint64 distinct_id = machine()->GetGUID();
  string channel_name = "action-result-" + UInt64ToString(distinct_id);
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));

  Action* a = new Action();
  a->set_client_machine(machine()->machine_id());
  a->set_client_channel(channel_name);//gaoxuan --这个channel应该指的是一个行为的数据通路吧
  a->set_action_type(MetadataAction::RENAME);//gaoxuan --这里确定action类型是RENAME

  MetadataAction::RenameInput in;
  in.set_from_path(from_path.data(), from_path.size());
  in.set_to_path(to_path.data(), to_path.size());
  in.SerializeToString(a->mutable_input());
  metadata_->setAPPname(name());//gaoxuan --这一行是我加的，用于在metadata_store.cc里面获得app name
  //LOG(ERROR)<<"app name is "<<name();gaoxuan --看一下name,输出的全是client
  metadata_->GetRWSets(a);
  log_->Append(a);
  //gaoxuan --这里把Rename操作确定后加入元数据日志，等待scheduler进行调度！结果就是执行metadata_store.cc中的Run函数，Run函数再去调用这个文件里面的Rename_Internal()
  //gaoxuan --Rename_Internal()才是Rename的真正逻辑所在
  MessageBuffer* m = NULL;
  while (!channel->Pop(&m)) {
    // Wait for action to complete and be sent back.
    usleep(100);
  }

  Action result;
  result.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;
  MetadataAction::RenameOutput out;
  out.ParseFromString(result.output());

  if (out.success()) {
    return new MessageBuffer();
  } else {
    return new MessageBuffer(new string("error creating file/dir\n"));
  }
}


