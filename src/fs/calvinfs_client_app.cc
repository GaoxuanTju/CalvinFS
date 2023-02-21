// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Kun  Ren  <kun.ren@yale.edu>
//

#include "fs/calvinfs_client_app.h"
#include "machine/app/app.h"

REGISTER_APP(CalvinFSClientApp)
{
  return new CalvinFSClientApp();
}

BTNode *copy_create(BTNode *from)
{
  if (from == NULL)
  {
    return NULL;
  }
  BTNode *lchild = copy_create(from->child);
  BTNode *rchild = copy_create(from->sibling);
  BTNode *newnode = new BTNode;
  newnode->path = from->path;
  newnode->child = lchild;
  newnode->sibling = rchild;
  return newnode;
}

void delete_tree(BTNode *&root)
{
  if (root == NULL)
  {
    return;
  }
  delete_tree(root->child);
  delete_tree(root->sibling);
  delete root;
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

    a.set_version(1000000000); // gaoxuan --this line is very important for LOOKUP
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
    // gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

    // 第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用五个空格填充上
    // 拆分的算法，遇到一个/就把之前的字符串放进去
    // 将拆分后的元素添加去的方法：header->add_split_string(拆分的字符串)
    int flag = 0;       // 用来标识此时split_string 里面有多少子串
    char pattern = '/'; // 根据/进行字符串拆分

    string temp_from = path.data();
    temp_from = temp_from.substr(1, temp_from.size()); // 这一行是为了去除最前面的/
    temp_from = temp_from + pattern;                   // 在最后面添加一个/便于处理
    int pos = temp_from.find(pattern);                 // 找到第一个/的位置
    while (pos != std::string::npos)                   // 循环不断找/，找到一个拆分一次
    {
      string temp1 = temp_from.substr(0, pos); // temp里面就是拆分出来的第一个子串
      string temp = temp1;
      for (int i = temp.size(); i < 5; i++)
      {
        temp = temp + " ";
      }
      header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
      flag++;                              // 拆分的字符串数量++
      temp_from = temp_from.substr(pos + 1, temp_from.size());
      pos = temp_from.find(pattern);
    }
    header->set_from_length(flag);
    while (flag != 8)
    {
      string temp = "     ";               // 用五个空格填充一下
      header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
      flag++;                              // 拆分的字符串数量++
    }

    // 这一行之前是gaoxuan添加的

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
MessageBuffer *CalvinFSClientApp::DeleteFile(const Slice &path, FileType type)
{
  string channel_name = "action-result-" + UInt64ToString(machine()->GetGUID());
  auto channel = machine()->DataChannel(channel_name);
  CHECK(!channel->Pop(NULL));

  Action *a = new Action();
  a->set_client_machine(machine()->machine_id());
  a->set_client_channel(channel_name);
  a->set_action_type(MetadataAction::ERASE);
  MetadataAction::EraseInput in;
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
    return new MessageBuffer(new string("error deleting file/dir\n"));
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

BTNode *CalvinFSClientApp::find_path(BTNode* dir_tree, string path, BTNode* &pre)
{
  BTNode *temp = dir_tree->child;
  pre = dir_tree;

  char pattern = '/';
  string split_string = path;
  split_string = split_string + pattern; // 最后加个/便于处理
  split_string = split_string.substr(1, split_string.size()); 
  while (temp != NULL && split_string != "")
  {
    int pos = split_string.find(pattern);
    string first_substr = split_string.substr(0, pos);
    split_string = split_string.substr(pos + 1, split_string.size());
    // 前面是拆分路径的逻辑
    if (temp->path == first_substr)
    {
      if (split_string == "")
      {
        break; // 查找到了
      }
      pre = temp;
      temp = temp->child;
    }
    else
    {
      // 如果没有直接找到，那么就去不断看兄弟，直到找到或者没有兄弟为止，没有兄弟还没找到，失败。直接return
      pre = temp;
      temp = temp->sibling;
      while (temp != NULL)
      {
        if (temp->path == first_substr)
        {
          break;
        }
        else
        {
          pre = temp;
          temp = temp->sibling;
        }
      }

      if (temp == NULL)
      {
        pre = NULL;
        return NULL;
      }
      else
      {
        if (split_string == "")
        {
          break; // 查找到了
        }
        pre = temp;
        temp = temp->child;
      }
    }
  }
  if (temp == NULL)
  {
    pre = NULL;
    return NULL;
  }
  else
  {
    return temp;
  }
}
void CalvinFSClientApp::rename_dir_tree(BTNode* &dir_tree, string from_path, string to_path)
{
  // 这个函数用于根据from_path和to_path操作一下dir_tree这个目录树
  //  1、找：找到from，to路径的位置
  BTNode *from_pre = NULL;
  BTNode *to_pre = NULL;

  int pos = to_path.rfind('/');
  string parent_to_path = to_path.substr(0, pos);

  int pos_ = from_path.rfind('/');
  string parent_from_path = from_path.substr(0,pos_);

  int index = to_path.rfind('/');
  string filename = to_path.substr(index + 1);

 //这里如果是父目录相同的话，只改名字不改指针，名字不同才需要改指针
  BTNode *from = find_path(dir_tree, from_path, from_pre);
  //父目录相同
  if(parent_from_path == parent_to_path)
  {
    from->path = filename;
    return;
  }

  BTNode *to = find_path(dir_tree, parent_to_path, to_pre);

  if(from != NULL && to != NULL)
  {
    //todo：这现在有bug，文件名字不能和父亲的兄弟相同
    //如果是左孩子
    if(from_pre->child->path == from->path)
    {
      from_pre->child = from->sibling;
    }
    //如果是兄弟
    if(from_pre->sibling->path ==from->path)
    {
      from_pre->sibling = from->sibling;
    }

    LOG(ERROR)<<" zhe kuai1 mei you bei hzhix ix";
    //下边改变父亲的指向
    from_path = filename;
    from->sibling = to->child;
    to->child = from;
  }
  else
  {
    return;
  }
/*
  //只能是这里有问题呀，新位置能够遍历到新指针，但是原位置还是存在指针遍历到，这是怎么回事
  if (from != NULL && to != NULL)
  {

    if(from_pre->child != NULL)
    {
      if (from_pre->child->path == from->path)
      {
        from_pre->child = from->sibling;
      }
    }
    else
    {
      from_pre->sibling = from->sibling;
    }

    from->path = filename;
    from->sibling = to->child;
    to->child = from;
  
  }
  else
  {
    return;
  }
*/
  
}
void CalvinFSClientApp::copy_dir_tree(BTNode* &dir_tree, string from_path, string to_path)
{
  /*
  copy的本质，是把一个地方的目录树，粘贴到另一个位置，是完整的粘贴，不能光修改指针
  所以分三步：
  1、找：找到原位置指针，目的位置的父指针
  2、查：看目的位置的孩子中是否存在同名，存在失败，不存在下一步
  3、复制：根据原位置指针，一次拷贝，只用拷贝原位置指针的孩子就行
  4、改类似，一个头插法
  */

  // 1、找
  BTNode *from_pre = NULL;
  BTNode *from = find_path(dir_tree, from_path, from_pre);
  BTNode *to_pre = NULL;
  int pos = to_path.rfind('/');
  string parent_to_path = to_path.substr(0, pos);
  BTNode *to = find_path(dir_tree, parent_to_path, to_pre);
  if (from != NULL && to != NULL)
  {
    // 2、查：查看目的为止的孩子是否存在同名文件
    string filename = to_path.substr(pos + 1);
    BTNode *check = to->child;
    while (check != NULL)
    {
      if (check->path == filename)
      {
        return; // 有同名文件
      }
      check = check->sibling;
    }

    // 经过了上面的筛查，证明下一级没有同名文件，我们接下来需要创建需要拷贝的目录子树

    // 3、复制建立，需要从from这个指针开始，将其下的目录子树全盘拷贝下来，先序遍历
    BTNode *new_tree = new BTNode;
    new_tree->path = from->path;
    new_tree->sibling = NULL;
    new_tree->child = copy_create(from->child);

    // 4、指向，new_tree就是子树的根，
    new_tree->sibling = to->child;
    to->child = new_tree;
  }
  else
  {
    return;
  }
}
void CalvinFSClientApp::create_dir_tree(BTNode* &dir_tree, string path)
{
  /*
  1、找：找到路径的父目录
  2、查：看看父目录的孩子是不是存在一个同名的
  3、建：新建一个节点，头插一下
  */
  // 1、找
  int pos = path.rfind('/');
  string parent_from_path = path.substr(0, pos);
  BTNode *parent_pre = NULL;
  BTNode *parent = find_path(dir_tree, parent_from_path, parent_pre);
  if (parent != NULL)
  {

    // 2、查
    BTNode *check = parent->child;
    while (check != NULL)
    {
      if (check->path == path)
      {
        return;
      }
      check = check->sibling;
    }

    // 3、插
    BTNode *create_node = new BTNode;
    create_node->path = path;
    create_node->child = NULL;
    create_node->sibling = parent->child;
    parent->child = create_node;
  }
}
void CalvinFSClientApp::delete_dir_tree(BTNode* &dir_tree, string path)
{
  /*
  1、找：找到要删除的路径和指向它的指针
  2、删：将这个指针和所有child都删掉
  */
  BTNode *from_pre = NULL;
  BTNode *from = find_path(dir_tree, path, from_pre);
  // 改下指向
  if (from_pre->child->path == path)
  {
    from_pre->child = from->sibling;
  }
  else
  {
    from_pre->sibling = from->sibling;
  }
  from->sibling = NULL;

  // 2、删
  delete_tree(from);
}
