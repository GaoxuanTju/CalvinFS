// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Kun  Ren  <kun.ren@yale.edu>
//

#include "fs/calvinfs_client_app.h"
#include "machine/app/app.h"



REGISTER_APP(CalvinFSClientApp)
{
  return new CalvinFSClientApp();
}

MessageBuffer *CalvinFSClientApp::GetMetadataEntry(const Slice &path)
{
  // Find out what machine to run this on.
  uint64 mds_machine =
      config_->LookupMetadataShard(config_->HashFileName(path), replica_);

  // Run if local.
  if (mds_machine == machine()->machine_id())
  {
    Action a;

    a.set_action_type(MetadataAction::LOOKUP);
    MetadataAction::LookupInput in;
    in.set_path(path.data(), path.size());

    a.set_version(1000000000);//gaoxuan --this line is very important for LOOKUP
    in.SerializeToString(a.mutable_input());
    metadata_->GetRWSets(&a); 
    metadata_->Run(&a);
    return new MessageBuffer(a);

    // If not local, get result from the right machine (within this replica).
  }
  else
  {
    Header *header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(mds_machine);
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("LOOKUP");
    header->add_misc_string(path.data(), path.size());
    //gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

    //第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用四个空格填充上
    //拆分的算法，遇到一个/就把之前的字符串放进去
    //将拆分后的元素添加去的方法：header->add_split_string(拆分的字符串)
    int flag = 0 ;//用来标识此时split_string 里面有多少子串
    char pattern = '/' ;//根据/进行字符串拆分

    string temp_from = path.data(); 
    temp_from = temp_from.substr(1,temp_from.size());//这一行是为了去除最前面的/
    temp_from = temp_from + pattern ; //在最后面添加一个/便于处理
    int pos = temp_from.find(pattern);//找到第一个/的位置
    while(pos != temp_from.npos)//循环不断找/，找到一个拆分一次
    {
      string temp1 = temp_from.substr(0,pos);//temp里面就是拆分出来的第一个子串
      string temp = temp1;
      for(int i = temp.size() ; i < 4 ; i++)
      {
        temp = temp + " ";
      }
      header->add_split_string(temp);//将拆出来的子串加到header里面去
      flag++;//拆分的字符串数量++
      temp_from = temp_from.substr(pos+1,temp_from.size());
      pos = temp_from.find(pattern);
    }

    while(flag != 8)
    {
      string temp = "    ";//用四个空格填充一下
      header->add_split_string(temp);//将拆出来的子串加到header里面去
      flag++;//拆分的字符串数量++     
    }

    //这一行之前是gaoxuan添加的


    MessageBuffer *m = NULL;
    header->set_data_ptr(reinterpret_cast<uint64>(&m));
    machine()->SendMessage(header, new MessageBuffer());
    while (m == NULL)
    {
      usleep(10);
      Noop<MessageBuffer *>(m);
    }
    return m;
  }
}

MessageBuffer *CalvinFSClientApp::CreateFile(const Slice &path, FileType type)
{
  string channel_name = "action-result-" + UInt64ToString(machine()->GetGUID());
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));

  Action *a = new Action();
  a->set_client_machine(machine()->machine_id());
  a->set_client_channel(channel_name);
  a->set_action_type(MetadataAction::CREATE_FILE);
  MetadataAction::CreateFileInput in;
  in.set_path(path.data(), path.size());
  in.set_type(type);
  in.SerializeToString(a->mutable_input());
  metadata_->GetRWSets(a);
  log_->Append(a);

  MessageBuffer *m = NULL;
  while (!channel->Pop(&m))
  {
    // Wait for action to complete and be sent back.
    usleep(100);
  }

  Action result;
  result.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;
  MetadataAction::AppendOutput out;
  out.ParseFromString(result.output());


  if (out.success())
  {
    
    return new MessageBuffer();
  }
  else
  {
    return new MessageBuffer(new string("error creating file/dir\n"));
  }
}

MessageBuffer *CalvinFSClientApp::AppendStringToFile(
    const Slice &data,
    const Slice &path)
{
  // Write data block.
  uint64 block_id = machine()->GetGUID() * 2 + (data.size() > 1024 ? 1 : 0);
  blocks_->Put(block_id, data);

  string channel_name = "action-result-" + UInt64ToString(machine()->GetGUID());
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));

  // Update metadata.
  Action *a = new Action();
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

  MessageBuffer *m = NULL;
  while (!channel->Pop(&m))
  {
    // Wait for action to complete and be sent back.
    usleep(100);
  }

  Action result;
  result.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;
  MetadataAction::AppendOutput out;
  out.ParseFromString(result.output());

  if (out.success())
  {
    return new MessageBuffer();
  }
  else
  {
    return new MessageBuffer(new string("error appending string to file\n"));
  }
}

MessageBuffer *CalvinFSClientApp::ReadFile(const Slice &path)
{
  MessageBuffer *serialized = GetMetadataEntry(path);
  Action a;
  a.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
  delete serialized;

  MetadataAction::LookupOutput out;
  out.ParseFromString(a.output());

  if (out.success() && out.entry().type() == DATA)
  {
    vector<MessageBuffer *> blocks(out.entry().file_parts_size(), NULL);
    for (int i = 0; i < out.entry().file_parts_size(); i++)
    {
      Header *header = new Header();
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
    while (!done)
    {
      done = true;
      for (int i = 0; i < out.entry().file_parts_size(); i++)
      {
        Noop<MessageBuffer *>(blocks[i]);
        if (blocks[i] == NULL)
        {
          done = false;
          break;
        }
      }
    }

    MessageBuffer *result = new MessageBuffer(out.entry());
    for (int i = 0; i < out.entry().file_parts_size(); i++)
    {
      result->AppendPart(blocks[i]->PopBack());
    }
    return result;
  }
  else
  {
    return new MessageBuffer(new string("metadata lookup error\n"));
  }
}

MessageBuffer *CalvinFSClientApp::LS(const Slice &path)
{
  MessageBuffer *serialized = GetMetadataEntry(path);
  Action a;
  a.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
  delete serialized;

  MetadataAction::LookupOutput out;
  out.ParseFromString(a.output());
  if (out.success() && out.entry().type() == DIR)
  {
    string *result = new string();
    for (int i = 0; i < out.entry().dir_contents_size(); i++)
    {
      result->append(out.entry().dir_contents(i));
      result->append("\n");
    }
    return new MessageBuffer(result);
  }
  else
  {
    return new MessageBuffer(new string("metadata lookup error\n"));
  }
}

MessageBuffer *CalvinFSClientApp::CopyFile(const Slice &from_path, const Slice &to_path)
{
  uint64 distinct_id = machine()->GetGUID();
  string channel_name = "action-result-" + UInt64ToString(distinct_id);
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));

  Action *a = new Action();
  a->set_client_machine(machine()->machine_id());
  a->set_client_channel(channel_name);
  a->set_action_type(MetadataAction::COPY);

  MetadataAction::CopyInput in;
  in.set_from_path(from_path.data(), from_path.size());
  in.set_to_path(to_path.data(), to_path.size());
  in.SerializeToString(a->mutable_input());
  metadata_->GetRWSets(a);
  log_->Append(a);

  MessageBuffer *m = NULL;
  while (!channel->Pop(&m))
  {
    // Wait for action to complete and be sent back.
    usleep(100);
  }

  Action result;
  result.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;
  MetadataAction::CopyOutput out;
  out.ParseFromString(result.output());

  if (out.success())
  {
    return new MessageBuffer();
  }
  else
  {
    return new MessageBuffer(new string("error creating file/dir\n"));
  }
}

MessageBuffer *CalvinFSClientApp::RenameFile(const Slice &from_path, const Slice &to_path)
{ 
  uint64 distinct_id = machine()->GetGUID();
  string channel_name = "action-result-" + UInt64ToString(distinct_id);
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));
 
  Action *a = new Action();
  a->set_client_machine(machine()->machine_id());
  a->set_client_channel(channel_name);        
  a->set_action_type(MetadataAction::RENAME); 
  
  MetadataAction::RenameInput in;
  in.set_from_path(from_path.data(), from_path.size());
  in.set_to_path(to_path.data(), to_path.size());
  in.SerializeToString(a->mutable_input());
  metadata_->setAPPname(name()); // gaoxuan --this line is added by me which is uesd to getAPPname in metadata_store.cc
  metadata_->GetRWSets(a);
 
  log_->Append(a);

  MessageBuffer *m = NULL;
  while (!channel->Pop(&m))
  {
    // Wait for action to complete and be sent back.
    usleep(100);
  }

  Action result;
  result.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;
  MetadataAction::RenameOutput out;
  out.ParseFromString(result.output());
  
  if (out.success())
  {
    return new MessageBuffer();
  }
  else
  {
    return new MessageBuffer(new string("error creating file/dir\n"));
  }
}
