// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Kun  Ren  <kun.ren@yale.edu>
//

#include "fs/metadata_store.h"

#include <glog/logging.h>
#include <map>
#include <set>
#include <string>
#include "btree/btree_map.h"
#include "common/utils.h"
#include "components/store/store_app.h"
#include "components/store/versioned_kvstore.pb.h"
#include "components/store/hybrid_versioned_kvstore.h"
#include "machine/app/app.h"
#include "machine/machine.h"
#include "machine/message_buffer.h"
#include "fs/calvinfs.h"
#include "fs/metadata.pb.h"
#include "machine/app/app.h"
#include "proto/action.pb.h"
#include <stack> //gaoxuan --the stack for DFS
#include <queue> //gaoxuan --the queue for BFS
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
using std::map;
using std::set;
using std::string;

REGISTER_APP(MetadataStoreApp)
{
  return new StoreApp(new MetadataStore(new HybridVersionedKVStore()));
}

///////////////////////        ExecutionContext        ////////////////////////
//
// TODO(agt): The implementation below is a LOCAL execution context.
//            Extend this to be a DISTRIBUTED one.
// TODO(agt): Generalize and move to components/store/store.{h,cc}.
//
class ExecutionContext
{
public:
  // Constructor performs all reads.
  ExecutionContext(VersionedKVStore *store, Action *action)
      : store_(store), version_(action->version()), aborted_(false)
  {
    for (int i = 0; i < action->readset_size(); i++)
    {
      string path = action->readset(i);
      // 判断涉不涉及分层，目前是看有没有b，如果有b就分层

      string hash_name;
      hash_name = path;

      if (!store_->Get(hash_name,
                       version_,
                       &reads_[hash_name]))
      {
        reads_.erase(action->readset(i));
      }
    }

    if (action->readset_size() > 0)
    {
      reader_ = true;
    }
    else
    {
      reader_ = false;
    }

    if (action->writeset_size() > 0)
    {
      writer_ = true;
    }
    else
    {
      writer_ = false;
    }
  }

  // Destructor installs all writes.
  ~ExecutionContext()
  {
    if (!aborted_)
    {
      for (auto it = writes_.begin(); it != writes_.end(); ++it)
      {
        store_->Put(it->first, it->second, version_);
      }
      for (auto it = deletions_.begin(); it != deletions_.end(); ++it)
      {
        store_->Delete(*it, version_);
      }
    }
  }

  bool EntryExists(const string &path)
  {
    return reads_.count(path) != 0;
  }

  bool GetEntry(const string &path, MetadataEntry *entry)
  {
    entry->Clear();
    if (reads_.count(path) != 0)
    {
      entry->ParseFromString(reads_[path]);
      return true;
    }
    return false;
  }

  void PutEntry(const string &path, const MetadataEntry &entry)
  {
    deletions_.erase(path);
    entry.SerializeToString(&writes_[path]);
    if (reads_.count(path) != 0)
    {
      entry.SerializeToString(&reads_[path]);
    }
  }

  void DeleteEntry(const string &path)
  {
    reads_.erase(path);
    writes_.erase(path);
    deletions_.insert(path);
  }

  bool IsWriter()
  {
    if (writer_)
      return true;
    else
      return false;
  }

  void Abort()
  {
    aborted_ = true;
  }

protected:
  ExecutionContext() {}
  VersionedKVStore *store_;
  uint64 version_;
  bool aborted_;
  map<string, string> reads_;
  map<string, string> writes_;
  set<string> deletions_;

  // True iff any reads are at this partition.
  bool reader_;

  // True iff any writes are at this partition.
  bool writer_;
};

////////////////////      DistributedExecutionContext      /////////////////////
//
// TODO(agt): Generalize and move to components/store/store.{h,cc}.
//
class DistributedExecutionContext : public ExecutionContext
{
public:
  // Constructor performs all reads.
  DistributedExecutionContext(
      Machine *machine,
      CalvinFSConfigMap *config,
      VersionedKVStore *store,
      Action *action)
      : machine_(machine), config_(config)
  {
    // Initialize parent class variables.
    store_ = store;
    version_ = action->version();
    aborted_ = false;

    // Look up what replica we're at.
    replica_ = config_->LookupReplica(machine_->machine_id());

    // Figure out what machines are readers (and perform local reads).
    reader_ = false;
    set<uint64> remote_readers;
    for (int i = 0; i < action->readset_size(); i++)
    { // gaoxuan --now handle the read/write set passed from GetRWs

      string path = action->readset(i); // 获取这个路径
      // 判断涉不涉及分层，目前是看有没有b，如果有b就分层

      string hash_name;
      hash_name = path;

      uint64 mds = config_->HashFileName(hash_name);
      uint64 machine = config_->LookupMetadataShard(mds, replica_); // gaoxuan --get right machine of the path
      if (machine == machine_->machine_id())
      { // gaoxuan --if the path in read/write set is local,put it's metadataentry into map reads_,key is path,value is entry
        // Local read.
        if (!store_->Get(hash_name,
                         version_,
                         &reads_[hash_name]))
        {
          reads_.erase(hash_name);
        }
        reader_ = true;
      }
      else
      { // gaoxuan --this machine is that we want to read. Put it into set remote_readers
        remote_readers.insert(machine);
      }
    }

    // Figure out what machines are writers.
    writer_ = false;
    set<uint64> remote_writers;
    for (int i = 0; i < action->writeset_size(); i++)
    {

      string path = action->writeset(i);

      string hash_name;

      hash_name = path;

      uint64 mds = config_->HashFileName(hash_name);
      uint64 machine = config_->LookupMetadataShard(mds, replica_);
      if (machine == machine_->machine_id())
      {
        writer_ = true;
      }
      else
      {
        remote_writers.insert(machine);
      }
    }

    // If any reads were performed locally, broadcast them to writers.
    if (reader_)
    {
      MapProto local_reads;
      for (auto it = reads_.begin(); it != reads_.end(); ++it)
      {
        MapProto::Entry *e = local_reads.add_entries();
        e->set_key(it->first);
        e->set_value(it->second);
      }
      // gaoxuan --why do we want to read but we need to broadcast to writers?
      for (auto it = remote_writers.begin(); it != remote_writers.end(); ++it)
      {
        Header *header = new Header();
        header->set_from(machine_->machine_id());
        header->set_to(*it);
        header->set_type(Header::DATA);                                 // gaoxuan --deliver packets directly,and the specific logic refers to lock
        header->set_data_channel("action-" + UInt64ToString(version_)); // gaoxuan --Can this version make a difference ?
        machine_->SendMessage(header, new MessageBuffer(local_reads));
      }
    }

    // If any writes will be performed locally, wait for all remote reads.
    if (writer_)
    {
      // Get channel.
      AtomicQueue<MessageBuffer *> *channel =
          machine_->DataChannel("action-" + UInt64ToString(version_));
      for (uint32 i = 0; i < remote_readers.size(); i++)
      { // gaoxuan --in this part we get remote entry
        MessageBuffer *m = NULL;
        // Get results.
        while (!channel->Pop(&m))
        {
          usleep(10);
        }
        MapProto remote_read;
        remote_read.ParseFromArray((*m)[0].data(), (*m)[0].size());
        for (int j = 0; j < remote_read.entries_size(); j++)
        {
          CHECK(reads_.count(remote_read.entries(j).key()) == 0);
          reads_[remote_read.entries(j).key()] = remote_read.entries(j).value();
        }
      }
      // Close channel.
      machine_->CloseDataChannel("action-" + UInt64ToString(version_));
    }
  }

  // Destructor installs all LOCAL writes.
  ~DistributedExecutionContext()
  {
    LOG(ERROR)<<"context distructor";
    double start = GetTime();
    if (!aborted_)
    {
      for (auto it = writes_.begin(); it != writes_.end(); ++it)
      {
        uint64 mds = config_->HashFileName(it->first);
        uint64 machine = config_->LookupMetadataShard(mds, replica_);
        if (machine == machine_->machine_id())
        {
          store_->Put(it->first, it->second, version_);
        }
      }
      for (auto it = deletions_.begin(); it != deletions_.end(); ++it)
      {
        uint64 mds = config_->HashFileName(*it);
        uint64 machine = config_->LookupMetadataShard(mds, replica_);
        if (machine == machine_->machine_id())
        {
          store_->Delete(*it, version_);
        }
      }
    }
    LOG(ERROR)<<"context des : "<<GetTime() - start;
  }

private:
  // Local machine.
  Machine *machine_;

  // Deployment configuration.
  CalvinFSConfigMap *config_;

  // Local replica id.
  uint64 replica_;
};

///////////////////////          MetadataStore          ///////////////////////
// use rfind to get the index of last '/',so the parentdir is for index 0 to index of last '/'
string ParentDir(const string &path)
{
  // Root dir is a special case.
  if (path.empty())
  {
    LOG(FATAL) << "root dir has no parent";
  }
  uint32 offset = path.rfind('/');
  CHECK_NE(string::npos, offset);    // at least 1 slash required
  CHECK_NE(path.size() - 1, offset); // filename cannot be empty
  return string(path, 0, offset);
}
// get the name of file, without the absolute path
string FileName(const string &path)
{
  // Root dir is a special case.
  if (path.empty())
  {
    return path;
  }
  uint32 offset = path.rfind('/');
  CHECK_NE(string::npos, offset);    // at least 1 slash required
  CHECK_NE(path.size() - 1, offset); // filename cannot be empty
  return string(path, offset + 1);
}
int Dir_depth(const string &path)
{
  int depth = 0;
  char pattern = '/';
  string temp = path;
  int pos = temp.find(pattern);
  while (pos != std::string::npos)
  {
    depth++;
    temp = temp.substr(pos + 1);
    pos = temp.find(pattern);
  }

  return depth;
}
MetadataStore::MetadataStore(VersionedKVStore *store)
    : store_(store), machine_(NULL), config_(NULL)
{
}

MetadataStore::~MetadataStore()
{
  delete store_;
}

void MetadataStore::SetMachine(Machine *m)
{

  machine_ = m;
  config_ = new CalvinFSConfigMap(machine_);

  // Initialize by inserting an entry for the root directory "/" (actual
  // representation is "" since trailing slashes are always removed).
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }
}

int RandomSize()
{
  return 1 + rand() % 2047;
}
// 最初始的三层Init
void MetadataStore::Init()
{

  int asize = machine_->config().size();
  int bsize = 1000;
  int csize = 500;

  double start = GetTime();

  // Update root dir.
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i = 0; i < 1000; i++)
    {
      entry.add_dir_contents("a" + IntToString(i));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }

  // Add dirs.
  for (int i = 0; i < asize; i++)
  {
    string dir("/a" + IntToString(i));
    if (IsLocal(dir))
    {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int j = 0; j < bsize; j++)
      {
        entry.add_dir_contents("b" + IntToString(j));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(dir, serialized_entry, 0);
    }
    // Add subdirs.
    for (int j = 0; j < bsize; j++)
    {
      string subdir(dir + "/b" + IntToString(j));
      if (IsLocal(subdir))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        for (int k = 0; k < csize; k++)
        {
          entry.add_dir_contents("c" + IntToString(k));
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(subdir, serialized_entry, 0);
      }
      // Add files.
      for (int k = 0; k < csize; k++)
      {
        string file(subdir + "/c" + IntToString(k));
        if (IsLocal(file))
        {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DATA);
          FilePart *fp = entry.add_file_parts();
          fp->set_length(RandomSize());
          fp->set_block_id(0);
          fp->set_block_offset(0);
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(file, serialized_entry, 0);
        }
      }
      if (j % 100 == 0)
      {
        LOG(ERROR) << "[" << machine_->machine_id() << "] "
                   << "MDS::Init() progress: " << (i * bsize + j) / 100 + 1
                   << "/" << asize * bsize / 100;
      }
    }
  }

  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::Init() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}
// 增加了数指针的三层Init
void MetadataStore::Init(BTNode *dir_tree)
{
  // gaoxuan --这里面会涉及到目录树的建立初始化。
  int asize = machine_->config().size();
  int bsize = 5;
  int csize = 5;
  // 改成5,5测试的时候容易看出来
  double start = GetTime();

  // Update root dir.
  // 因为我们需要的是完整的目录树，所以我们在if ISLOCAL之前直接放进去就好，同时也要把孩子和兄弟的关系理清楚
  // 提前先建立起来吧

  // gaoxuan --根节点的指针
  dir_tree->child = NULL;
  dir_tree->sibling = NULL;
  dir_tree->path = "";
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i = 0; i < asize; i++)
    {
      entry.add_dir_contents("a" + IntToString(i));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }
  BTNode *a_level = NULL; // 这个就指向该层第一个节点
  // Add dirs.
  for (int i = 0; i < asize; i++)
  {
    string dir("/a" + IntToString(i));
    string dir_("a" + IntToString(i));
    BTNode *temp_a = new BTNode;
    temp_a->child = NULL;
    temp_a->path = dir_;
    temp_a->sibling = NULL;

    if (i == 0)
    {
      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
      dir_tree->child = temp_a;
      a_level = temp_a; // a_level指针作为上一个兄弟节点
    }
    else
    {
      // 如果不是第一个节点，就是上一个节点的兄弟节点
      a_level->sibling = temp_a;
      a_level = a_level->sibling; // a_level移动到下一个兄弟节点
    }

    if (IsLocal(dir))
    {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int j = 0; j < bsize; j++)
      {
        entry.add_dir_contents("b" + IntToString(j));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(dir, serialized_entry, 0);
    }

    // Add subdirs.
    BTNode *b_level = NULL; // 这个就指向该层第二个节点
    for (int j = 0; j < bsize; j++)
    {
      string subdir(dir + "/b" + IntToString(j));
      string subdir_("b" + IntToString(j));
      BTNode *temp_b = new BTNode;
      temp_b->child = NULL;
      temp_b->path = subdir_;
      temp_b->sibling = NULL;

      if (j == 0)
      {
        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
        a_level->child = temp_b;
        b_level = temp_b;
      }
      else
      {
        // gaoxuan --不是第一个节点，是上一个的兄弟节点
        b_level->sibling = temp_b;
        b_level = b_level->sibling;
      }
      if (IsLocal(subdir))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        for (int k = 0; k < csize; k++)
        {
          entry.add_dir_contents("c" + IntToString(k));
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(subdir, serialized_entry, 0);
      }
      // Add files.
      BTNode *c_level = NULL; // 这个就指向该层第三个节点
      for (int k = 0; k < csize; k++)
      {
        string file(subdir + "/c" + IntToString(k));
        string file_("c" + IntToString(k));
        BTNode *temp_c = new BTNode;
        temp_c->child = NULL;
        temp_c->path = file_;
        temp_c->sibling = NULL;
        if (k == 0)
        {
          // gaoxuan --如果是第一个节点，就作为上一层的孩子
          b_level->child = temp_c;
          c_level = temp_c;
        }
        else
        {
          c_level->sibling = temp_c;
          c_level = c_level->sibling;
        }

        if (IsLocal(file))
        {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DATA);
          FilePart *fp = entry.add_file_parts();
          fp->set_length(RandomSize());
          fp->set_block_id(0);
          fp->set_block_offset(0);
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(file, serialized_entry, 0);
        }
      }
      if (j % 100 == 0)
      {
        LOG(ERROR) << "[" << machine_->machine_id() << "] "
                   << "MDS::Init() progress: " << (i * bsize + j) / 100 + 1
                   << "/" << asize * bsize / 100;
      }
    }
  }

  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::Init() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

// 这个初始化函数不知道干什么的了
void MetadataStore::Init(BTNode *dir_tree, string level)
{
  // gaoxuan --这里面会涉及到目录树的建立初始化。
  int asize = machine_->config().size();
  int bsize = 5;
  int csize = 5;
  // 改成5,5测试的时候容易看出来
  double start = GetTime();

  // Update root dir.
  // 因为我们需要的是完整的目录树，所以我们在if ISLOCAL之前直接放进去就好，同时也要把孩子和兄弟的关系理清楚
  // 提前先建立起来吧

  // gaoxuan --根节点的指针
  dir_tree->child = NULL;
  dir_tree->sibling = NULL;
  dir_tree->path = "";
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i = 0; i < asize; i++)
    {
      entry.add_dir_contents("a" + IntToString(i));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }
  BTNode *a_level = NULL; // 这个就指向该层第一个节点
  // Add dirs.
  for (int i = 0; i < asize; i++)
  {
    string dir("/a" + IntToString(i));
    string dir_("a" + IntToString(i));
    BTNode *temp_a = new BTNode;
    temp_a->child = NULL;
    temp_a->path = dir_;
    temp_a->sibling = NULL;

    if (i == 0)
    {
      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
      dir_tree->child = temp_a;
      a_level = temp_a; // a_level指针作为上一个兄弟节点
    }
    else
    {
      // 如果不是第一个节点，就是上一个节点的兄弟节点
      a_level->sibling = temp_a;
      a_level = a_level->sibling; // a_level移动到下一个兄弟节点
    }

    if (IsLocal(dir))
    {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int j = 0; j < bsize; j++)
      {
        entry.add_dir_contents("b" + IntToString(j));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(dir, serialized_entry, 0);
    }

    // Add subdirs.
    BTNode *b_level = NULL; // 这个就指向该层第二个节点

    string flag_level = IntToString(i);
    // 现在flag_level目前就是要对拼接到字符串前的标识了
    for (int j = 0; j < bsize; j++)
    {
      string subdir(dir + "/b" + IntToString(j));
      string subdir_("b" + IntToString(j));
      string subdir__("/" + flag_level + subdir_);
      // gaoxuan --目前写死在b这一层开始分层
      // 1、对相对路径hash
      // 2、键值对继续存储元数据

      // 现在无法绕开的问题就是相对路径很大概率是相同的，那么无论是hash
      // 还是键值对存储数据都不能实现
      /*
      这一点是分层点，我们在分层点确定是


      */

      BTNode *temp_b = new BTNode;
      temp_b->child = NULL;
      temp_b->path = subdir_;
      temp_b->sibling = NULL;

      if (j == 0)
      {
        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
        a_level->child = temp_b;
        b_level = temp_b;
      }
      else
      {
        // gaoxuan --不是第一个节点，是上一个的兄弟节点
        b_level->sibling = temp_b;
        b_level = b_level->sibling;
      }
      if (IsLocal(subdir__))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        for (int k = 0; k < csize; k++)
        {
          entry.add_dir_contents("c" + IntToString(k));
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(subdir__, serialized_entry, 0);
      }
      // Add files.
      BTNode *c_level = NULL; // 这个就指向该层第三个节点
      for (int k = 0; k < csize; k++)
      {
        string file(subdir__ + "/c" + IntToString(k));
        string file_("c" + IntToString(k));

        BTNode *temp_c = new BTNode;
        temp_c->child = NULL;
        temp_c->path = file_;
        temp_c->sibling = NULL;
        if (k == 0)
        {
          // gaoxuan --如果是第一个节点，就作为上一层的孩子
          b_level->child = temp_c;
          c_level = temp_c;
        }
        else
        {
          c_level->sibling = temp_c;
          c_level = c_level->sibling;
        }

        if (IsLocal(file))
        {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DATA);
          FilePart *fp = entry.add_file_parts();
          fp->set_length(RandomSize());
          fp->set_block_id(0);
          fp->set_block_offset(0);
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(file, serialized_entry, 0);
        }
      }
      if (j % 100 == 0)
      {
        LOG(ERROR) << "[" << machine_->machine_id() << "] "
                   << "MDS::Init() progress: " << (i * bsize + j) / 100 + 1
                   << "/" << asize * bsize / 100;
      }
    }
  }

  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::Init() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

// 这个初始化函数是增加了分层的，但是没加uid，没用
void MetadataStore::Init_for_depth(BTNode *dir_tree)
{
  // gaoxuan --这里面会涉及到目录树的建立初始化。
  int a_0 = machine_->config().size();
  int a_1 = 2;
  int a_2 = 2;
  int a_3 = 2;
  int a_4 = 2;
  int a_5 = 2;
  int a_6 = 2;
  int a_7 = 2;
  int a_8 = 2;
  int a_9 = 2;
  int a_10 = 2;
  int a_11 = 2;
  int a_12 = 2;
  int a_13 = 2;
  int a_14 = 2;
  int a_15 = 2;
  int a_16 = 2;
  int a_17 = 2;
  int a_18 = 2;
  int a_19 = 2;
  int a_20 = 2;
  int bsize = 2;
  int csize = 2;
  // 改成5,5测试的时候容易看出来
  double start = GetTime();
  // gaoxuan --根节点的指针
  dir_tree->child = NULL;
  dir_tree->sibling = NULL;
  dir_tree->path = "";
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i_0 = 0; i_0 < a_0; i_0++)
    {
      entry.add_dir_contents("a_0" + IntToString(i_0));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }
  BTNode *a_0_level = NULL; // 这个就指向该层第一个节点
  for (int i_0 = 0; i_0 < a_0; i_0++)
  {
    string a_0_dir("/a_0" + IntToString(i_0));
    string a_0_dir_("a_0" + IntToString(i_0));
    BTNode *temp_a = new BTNode;
    temp_a->child = NULL;
    temp_a->path = a_0_dir_;
    temp_a->sibling = NULL;
    if (i_0 == 0)
    {
      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
      dir_tree->child = temp_a;
      a_0_level = temp_a; // a_level指针作为上一个兄弟节点
    }
    else
    {
      // 如果不是第一个节点，就是上一个节点的兄弟节点
      a_0_level->sibling = temp_a;
      a_0_level = a_0_level->sibling; // a_level移动到下一个兄弟节点
    }
    if (IsLocal(a_0_dir))
    {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int i_1 = 0; i_1 < a_1; i_1++)
      {
        entry.add_dir_contents("a_1" + IntToString(i_1));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(a_0_dir, serialized_entry, 0);
    }
    BTNode *a_1_level = NULL; // 这个就指向该层第一个节点
    for (int i_1 = 0; i_1 < a_1; i_1++)
    {
      string a_1_dir(a_0_dir + "/a_1" + IntToString(i_1));
      string a_1_dir_("a_1" + IntToString(i_1));
      BTNode *temp_a = new BTNode;
      temp_a->child = NULL;
      temp_a->path = a_1_dir_;
      temp_a->sibling = NULL;
      if (i_1 == 0)
      {
        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
        a_0_level->child = temp_a;
        a_1_level = temp_a; // a_level指针作为上一个兄弟节点
      }
      else
      {
        // 如果不是第一个节点，就是上一个节点的兄弟节点
        a_1_level->sibling = temp_a;
        a_1_level = a_1_level->sibling; // a_level移动到下一个兄弟节点
      }
      if (IsLocal(a_1_dir))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        for (int i_2 = 0; i_2 < a_2; i_2++)
        {
          entry.add_dir_contents("a_2" + IntToString(i_2));
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(a_1_dir, serialized_entry, 0);
      }
      BTNode *a_2_level = NULL; // 这个就指向该层第一个节点
      for (int i_2 = 0; i_2 < a_2; i_2++)
      {
        string a_2_dir(a_1_dir + "/a_2" + IntToString(i_2));
        string a_2_dir_("a_2" + IntToString(i_2));
        BTNode *temp_a = new BTNode;
        temp_a->child = NULL;
        temp_a->path = a_2_dir_;
        temp_a->sibling = NULL;
        if (i_2 == 0)
        {
          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
          a_1_level->child = temp_a;
          a_2_level = temp_a; // a_level指针作为上一个兄弟节点
        }
        else
        {
          // 如果不是第一个节点，就是上一个节点的兄弟节点
          a_2_level->sibling = temp_a;
          a_2_level = a_2_level->sibling; // a_level移动到下一个兄弟节点
        }
        if (IsLocal(a_2_dir))
        {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DIR);
          for (int i_3 = 0; i_3 < a_3; i_3++)
          {
            entry.add_dir_contents("a_3" + IntToString(i_3));
          }
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(a_2_dir, serialized_entry, 0);
        }
        BTNode *a_3_level = NULL; // 这个就指向该层第一个节点
        for (int i_3 = 0; i_3 < a_3; i_3++)
        {
          string a_3_dir(a_2_dir + "/a_3" + IntToString(i_3));
          string a_3_dir_("a_3" + IntToString(i_3));
          BTNode *temp_a = new BTNode;
          temp_a->child = NULL;
          temp_a->path = a_3_dir_;
          temp_a->sibling = NULL;
          if (i_3 == 0)
          {
            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
            a_2_level->child = temp_a;
            a_3_level = temp_a; // a_level指针作为上一个兄弟节点
          }
          else
          {
            // 如果不是第一个节点，就是上一个节点的兄弟节点
            a_3_level->sibling = temp_a;
            a_3_level = a_3_level->sibling; // a_level移动到下一个兄弟节点
          }
          if (IsLocal(a_3_dir))
          {
            MetadataEntry entry;
            entry.mutable_permissions();
            entry.set_type(DIR);
            for (int i_4 = 0; i_4 < a_4; i_4++)
            {
              entry.add_dir_contents("a_4" + IntToString(i_4));
            }
            string serialized_entry;
            entry.SerializeToString(&serialized_entry);
            store_->Put(a_3_dir, serialized_entry, 0);
          }
          BTNode *a_4_level = NULL; // 这个就指向该层第一个节点
          for (int i_4 = 0; i_4 < a_4; i_4++)
          {
            string a_4_dir(a_3_dir + "/a_4" + IntToString(i_4));
            string a_4_dir_("a_4" + IntToString(i_4));
            BTNode *temp_a = new BTNode;
            temp_a->child = NULL;
            temp_a->path = a_4_dir_;
            temp_a->sibling = NULL;
            if (i_4 == 0)
            {
              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
              a_3_level->child = temp_a;
              a_4_level = temp_a; // a_level指针作为上一个兄弟节点
            }
            else
            {
              // 如果不是第一个节点，就是上一个节点的兄弟节点
              a_4_level->sibling = temp_a;
              a_4_level = a_4_level->sibling; // a_level移动到下一个兄弟节点
            }
            if (IsLocal(a_4_dir))
            {
              MetadataEntry entry;
              entry.mutable_permissions();
              entry.set_type(DIR);
              for (int i_5 = 0; i_5 < a_5; i_5++)
              {
                entry.add_dir_contents("a_5" + IntToString(i_5));
              }
              string serialized_entry;
              entry.SerializeToString(&serialized_entry);
              store_->Put(a_4_dir, serialized_entry, 0);
            }
            BTNode *a_5_level = NULL; // 这个就指向该层第一个节点
            for (int i_5 = 0; i_5 < a_5; i_5++)
            {
              string a_5_dir(a_4_dir + "/a_5" + IntToString(i_5));
              string a_5_dir_("a_5" + IntToString(i_5));
              BTNode *temp_a = new BTNode;
              temp_a->child = NULL;
              temp_a->path = a_5_dir_;
              temp_a->sibling = NULL;
              if (i_5 == 0)
              {
                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                a_4_level->child = temp_a;
                a_5_level = temp_a; // a_level指针作为上一个兄弟节点
              }
              else
              {
                // 如果不是第一个节点，就是上一个节点的兄弟节点
                a_5_level->sibling = temp_a;
                a_5_level = a_5_level->sibling; // a_level移动到下一个兄弟节点
              }
              if (IsLocal(a_5_dir))
              {
                MetadataEntry entry;
                entry.mutable_permissions();
                entry.set_type(DIR);
                for (int i_6 = 0; i_6 < a_6; i_6++)
                {
                  entry.add_dir_contents("a_6" + IntToString(i_6));
                }
                string serialized_entry;
                entry.SerializeToString(&serialized_entry);
                store_->Put(a_5_dir, serialized_entry, 0);
              }
              BTNode *a_6_level = NULL; // 这个就指向该层第一个节点
              for (int i_6 = 0; i_6 < a_6; i_6++)
              {
                string a_6_dir(a_5_dir + "/a_6" + IntToString(i_6));
                string a_6_dir_("a_6" + IntToString(i_6));
                BTNode *temp_a = new BTNode;
                temp_a->child = NULL;
                temp_a->path = a_6_dir_;
                temp_a->sibling = NULL;
                if (i_6 == 0)
                {
                  // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                  a_5_level->child = temp_a;
                  a_6_level = temp_a; // a_level指针作为上一个兄弟节点
                }
                else
                {
                  // 如果不是第一个节点，就是上一个节点的兄弟节点
                  a_6_level->sibling = temp_a;
                  a_6_level = a_6_level->sibling; // a_level移动到下一个兄弟节点
                }
                if (IsLocal(a_6_dir))
                {
                  MetadataEntry entry;
                  entry.mutable_permissions();
                  entry.set_type(DIR);
                  for (int i_7 = 0; i_7 < a_7; i_7++)
                  {
                    entry.add_dir_contents("a_7" + IntToString(i_7));
                  }
                  string serialized_entry;
                  entry.SerializeToString(&serialized_entry);
                  store_->Put(a_6_dir, serialized_entry, 0);
                }
                BTNode *a_7_level = NULL; // 这个就指向该层第一个节点
                for (int i_7 = 0; i_7 < a_7; i_7++)
                {
                  string a_7_dir(a_6_dir + "/a_7" + IntToString(i_7));
                  string a_7_dir_("a_7" + IntToString(i_7));
                  BTNode *temp_a = new BTNode;
                  temp_a->child = NULL;
                  temp_a->path = a_7_dir_;
                  temp_a->sibling = NULL;
                  if (i_7 == 0)
                  {
                    // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                    a_6_level->child = temp_a;
                    a_7_level = temp_a; // a_level指针作为上一个兄弟节点
                  }
                  else
                  {
                    // 如果不是第一个节点，就是上一个节点的兄弟节点
                    a_7_level->sibling = temp_a;
                    a_7_level = a_7_level->sibling; // a_level移动到下一个兄弟节点
                  }
                  if (IsLocal(a_7_dir))
                  {
                    MetadataEntry entry;
                    entry.mutable_permissions();
                    entry.set_type(DIR);
                    for (int i_8 = 0; i_8 < a_8; i_8++)
                    {
                      entry.add_dir_contents("a_8" + IntToString(i_8));
                    }
                    string serialized_entry;
                    entry.SerializeToString(&serialized_entry);
                    store_->Put(a_7_dir, serialized_entry, 0);
                  }
                  BTNode *a_8_level = NULL; // 这个就指向该层第一个节点
                  for (int i_8 = 0; i_8 < a_8; i_8++)
                  {
                    string a_8_dir(a_7_dir + "/a_8" + IntToString(i_8));
                    string a_8_dir_("a_8" + IntToString(i_8));
                    BTNode *temp_a = new BTNode;
                    temp_a->child = NULL;
                    temp_a->path = a_8_dir_;
                    temp_a->sibling = NULL;
                    if (i_8 == 0)
                    {
                      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                      a_7_level->child = temp_a;
                      a_8_level = temp_a; // a_level指针作为上一个兄弟节点
                    }
                    else
                    {
                      // 如果不是第一个节点，就是上一个节点的兄弟节点
                      a_8_level->sibling = temp_a;
                      a_8_level = a_8_level->sibling; // a_level移动到下一个兄弟节点
                    }
                    if (IsLocal(a_8_dir))
                    {
                      MetadataEntry entry;
                      entry.mutable_permissions();
                      entry.set_type(DIR);
                      for (int i_9 = 0; i_9 < a_9; i_9++)
                      {
                        entry.add_dir_contents("a_9" + IntToString(i_9));
                      }
                      string serialized_entry;
                      entry.SerializeToString(&serialized_entry);
                      store_->Put(a_8_dir, serialized_entry, 0);
                    }
                    BTNode *a_9_level = NULL; // 这个就指向该层第一个节点
                    for (int i_9 = 0; i_9 < a_9; i_9++)
                    {
                      string a_9_dir(a_8_dir + "/a_9" + IntToString(i_9));
                      string a_9_dir_("a_9" + IntToString(i_9));
                      BTNode *temp_a = new BTNode;
                      temp_a->child = NULL;
                      temp_a->path = a_9_dir_;
                      temp_a->sibling = NULL;
                      if (i_9 == 0)
                      {
                        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                        a_8_level->child = temp_a;
                        a_9_level = temp_a; // a_level指针作为上一个兄弟节点
                      }
                      else
                      {
                        // 如果不是第一个节点，就是上一个节点的兄弟节点
                        a_9_level->sibling = temp_a;
                        a_9_level = a_9_level->sibling; // a_level移动到下一个兄弟节点
                      }
                      if (IsLocal(a_9_dir))
                      {
                        MetadataEntry entry;
                        entry.mutable_permissions();
                        entry.set_type(DIR);
                        for (int i_10 = 0; i_10 < a_10; i_10++)
                        {
                          entry.add_dir_contents("a_10" + IntToString(i_10));
                        }
                        string serialized_entry;
                        entry.SerializeToString(&serialized_entry);
                        store_->Put(a_9_dir, serialized_entry, 0);
                      }
                      BTNode *a_10_level = NULL; // 这个就指向该层第一个节点
                      for (int i_10 = 0; i_10 < a_10; i_10++)
                      {
                        string a_10_dir(a_9_dir + "/a_10" + IntToString(i_10));
                        string a_10_dir_("a_10" + IntToString(i_10));
                        BTNode *temp_a = new BTNode;
                        temp_a->child = NULL;
                        temp_a->path = a_10_dir_;
                        temp_a->sibling = NULL;
                        if (i_10 == 0)
                        {
                          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                          a_9_level->child = temp_a;
                          a_10_level = temp_a; // a_level指针作为上一个兄弟节点
                        }
                        else
                        {
                          // 如果不是第一个节点，就是上一个节点的兄弟节点
                          a_10_level->sibling = temp_a;
                          a_10_level = a_10_level->sibling; // a_level移动到下一个兄弟节点
                        }
                        if (IsLocal(a_10_dir))
                        {
                          MetadataEntry entry;
                          entry.mutable_permissions();
                          entry.set_type(DIR);
                          for (int i_11 = 0; i_11 < a_11; i_11++)
                          {
                            entry.add_dir_contents("a_11" + IntToString(i_11));
                          }
                          string serialized_entry;
                          entry.SerializeToString(&serialized_entry);
                          store_->Put(a_10_dir, serialized_entry, 0);
                        }
                        BTNode *a_11_level = NULL; // 这个就指向该层第一个节点
                        for (int i_11 = 0; i_11 < a_11; i_11++)
                        {
                          string a_11_dir(a_10_dir + "/a_11" + IntToString(i_11));
                          string a_11_dir_("a_11" + IntToString(i_11));
                          BTNode *temp_a = new BTNode;
                          temp_a->child = NULL;
                          temp_a->path = a_11_dir_;
                          temp_a->sibling = NULL;
                          if (i_11 == 0)
                          {
                            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                            a_10_level->child = temp_a;
                            a_11_level = temp_a; // a_level指针作为上一个兄弟节点
                          }
                          else
                          {
                            // 如果不是第一个节点，就是上一个节点的兄弟节点
                            a_11_level->sibling = temp_a;
                            a_11_level = a_11_level->sibling; // a_level移动到下一个兄弟节点
                          }
                          if (IsLocal(a_11_dir))
                          {
                            MetadataEntry entry;
                            entry.mutable_permissions();
                            entry.set_type(DIR);
                            for (int i_12 = 0; i_12 < a_12; i_12++)
                            {
                              entry.add_dir_contents("a_12" + IntToString(i_12));
                            }
                            string serialized_entry;
                            entry.SerializeToString(&serialized_entry);
                            store_->Put(a_11_dir, serialized_entry, 0);
                          }
                          BTNode *a_12_level = NULL; // 这个就指向该层第一个节点
                          for (int i_12 = 0; i_12 < a_12; i_12++)
                          {
                            string a_12_dir(a_11_dir + "/a_12" + IntToString(i_12));
                            string a_12_dir_("a_12" + IntToString(i_12));
                            BTNode *temp_a = new BTNode;
                            temp_a->child = NULL;
                            temp_a->path = a_12_dir_;
                            temp_a->sibling = NULL;
                            if (i_12 == 0)
                            {
                              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                              a_11_level->child = temp_a;
                              a_12_level = temp_a; // a_level指针作为上一个兄弟节点
                            }
                            else
                            {
                              // 如果不是第一个节点，就是上一个节点的兄弟节点
                              a_12_level->sibling = temp_a;
                              a_12_level = a_12_level->sibling; // a_level移动到下一个兄弟节点
                            }
                            if (IsLocal(a_12_dir))
                            {
                              MetadataEntry entry;
                              entry.mutable_permissions();
                              entry.set_type(DIR);
                              for (int i_13 = 0; i_13 < a_13; i_13++)
                              {
                                entry.add_dir_contents("a_13" + IntToString(i_13));
                              }
                              string serialized_entry;
                              entry.SerializeToString(&serialized_entry);
                              store_->Put(a_12_dir, serialized_entry, 0);
                            }
                            BTNode *a_13_level = NULL; // 这个就指向该层第一个节点
                            for (int i_13 = 0; i_13 < a_13; i_13++)
                            {
                              string a_13_dir(a_12_dir + "/a_13" + IntToString(i_13));
                              string a_13_dir_("a_13" + IntToString(i_13));
                              BTNode *temp_a = new BTNode;
                              temp_a->child = NULL;
                              temp_a->path = a_13_dir_;
                              temp_a->sibling = NULL;
                              if (i_13 == 0)
                              {
                                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                a_12_level->child = temp_a;
                                a_13_level = temp_a; // a_level指针作为上一个兄弟节点
                              }
                              else
                              {
                                // 如果不是第一个节点，就是上一个节点的兄弟节点
                                a_13_level->sibling = temp_a;
                                a_13_level = a_13_level->sibling; // a_level移动到下一个兄弟节点
                              }
                              if (IsLocal(a_13_dir))
                              {
                                MetadataEntry entry;
                                entry.mutable_permissions();
                                entry.set_type(DIR);
                                for (int i_14 = 0; i_14 < a_14; i_14++)
                                {
                                  entry.add_dir_contents("a_14" + IntToString(i_14));
                                }
                                string serialized_entry;
                                entry.SerializeToString(&serialized_entry);
                                store_->Put(a_13_dir, serialized_entry, 0);
                              }
                              BTNode *a_14_level = NULL; // 这个就指向该层第一个节点
                              for (int i_14 = 0; i_14 < a_14; i_14++)
                              {
                                string a_14_dir(a_13_dir + "/a_14" + IntToString(i_14));
                                string a_14_dir_("a_14" + IntToString(i_14));
                                BTNode *temp_a = new BTNode;
                                temp_a->child = NULL;
                                temp_a->path = a_14_dir_;
                                temp_a->sibling = NULL;
                                if (i_14 == 0)
                                {
                                  // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                  a_13_level->child = temp_a;
                                  a_14_level = temp_a; // a_level指针作为上一个兄弟节点
                                }
                                else
                                {
                                  // 如果不是第一个节点，就是上一个节点的兄弟节点
                                  a_14_level->sibling = temp_a;
                                  a_14_level = a_14_level->sibling; // a_level移动到下一个兄弟节点
                                }
                                if (IsLocal(a_14_dir))
                                {
                                  MetadataEntry entry;
                                  entry.mutable_permissions();
                                  entry.set_type(DIR);
                                  for (int i_15 = 0; i_15 < a_15; i_15++)
                                  {
                                    entry.add_dir_contents("a_15" + IntToString(i_15));
                                  }
                                  string serialized_entry;
                                  entry.SerializeToString(&serialized_entry);
                                  store_->Put(a_14_dir, serialized_entry, 0);
                                }
                                BTNode *a_15_level = NULL; // 这个就指向该层第一个节点
                                for (int i_15 = 0; i_15 < a_15; i_15++)
                                {
                                  string a_15_dir(a_14_dir + "/a_15" + IntToString(i_15));
                                  string a_15_dir_("a_15" + IntToString(i_15));
                                  BTNode *temp_a = new BTNode;
                                  temp_a->child = NULL;
                                  temp_a->path = a_15_dir_;
                                  temp_a->sibling = NULL;
                                  if (i_15 == 0)
                                  {
                                    // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                    a_14_level->child = temp_a;
                                    a_15_level = temp_a; // a_level指针作为上一个兄弟节点
                                  }
                                  else
                                  {
                                    // 如果不是第一个节点，就是上一个节点的兄弟节点
                                    a_15_level->sibling = temp_a;
                                    a_15_level = a_15_level->sibling; // a_level移动到下一个兄弟节点
                                  }
                                  if (IsLocal(a_15_dir))
                                  {
                                    MetadataEntry entry;
                                    entry.mutable_permissions();
                                    entry.set_type(DIR);
                                    for (int i_16 = 0; i_16 < a_16; i_16++)
                                    {
                                      entry.add_dir_contents("a_16" + IntToString(i_16));
                                    }
                                    string serialized_entry;
                                    entry.SerializeToString(&serialized_entry);
                                    store_->Put(a_15_dir, serialized_entry, 0);
                                  }
                                  BTNode *a_16_level = NULL; // 这个就指向该层第一个节点
                                  for (int i_16 = 0; i_16 < a_16; i_16++)
                                  {
                                    string a_16_dir(a_15_dir + "/a_16" + IntToString(i_16));
                                    string a_16_dir_("a_16" + IntToString(i_16));
                                    BTNode *temp_a = new BTNode;
                                    temp_a->child = NULL;
                                    temp_a->path = a_16_dir_;
                                    temp_a->sibling = NULL;
                                    if (i_16 == 0)
                                    {
                                      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                      a_15_level->child = temp_a;
                                      a_16_level = temp_a; // a_level指针作为上一个兄弟节点
                                    }
                                    else
                                    {
                                      // 如果不是第一个节点，就是上一个节点的兄弟节点
                                      a_16_level->sibling = temp_a;
                                      a_16_level = a_16_level->sibling; // a_level移动到下一个兄弟节点
                                    }
                                    if (IsLocal(a_16_dir))
                                    {
                                      MetadataEntry entry;
                                      entry.mutable_permissions();
                                      entry.set_type(DIR);
                                      for (int i_17 = 0; i_17 < a_17; i_17++)
                                      {
                                        entry.add_dir_contents("a_17" + IntToString(i_17));
                                      }
                                      string serialized_entry;
                                      entry.SerializeToString(&serialized_entry);
                                      store_->Put(a_16_dir, serialized_entry, 0);
                                    }
                                    BTNode *a_17_level = NULL; // 这个就指向该层第一个节点
                                    for (int i_17 = 0; i_17 < a_17; i_17++)
                                    {
                                      string a_17_dir(a_16_dir + "/a_17" + IntToString(i_17));
                                      string a_17_dir_("a_17" + IntToString(i_17));
                                      BTNode *temp_a = new BTNode;
                                      temp_a->child = NULL;
                                      temp_a->path = a_17_dir_;
                                      temp_a->sibling = NULL;
                                      if (i_17 == 0)
                                      {
                                        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                        a_16_level->child = temp_a;
                                        a_17_level = temp_a; // a_level指针作为上一个兄弟节点
                                      }
                                      else
                                      {
                                        // 如果不是第一个节点，就是上一个节点的兄弟节点
                                        a_17_level->sibling = temp_a;
                                        a_17_level = a_17_level->sibling; // a_level移动到下一个兄弟节点
                                      }
                                      if (IsLocal(a_17_dir))
                                      {
                                        MetadataEntry entry;
                                        entry.mutable_permissions();
                                        entry.set_type(DIR);
                                        for (int i_18 = 0; i_18 < a_18; i_18++)
                                        {
                                          entry.add_dir_contents("a_18" + IntToString(i_18));
                                        }
                                        string serialized_entry;
                                        entry.SerializeToString(&serialized_entry);
                                        store_->Put(a_17_dir, serialized_entry, 0);
                                      }
                                      BTNode *a_18_level = NULL; // 这个就指向该层第一个节点
                                      for (int i_18 = 0; i_18 < a_18; i_18++)
                                      {
                                        string a_18_dir(a_17_dir + "/a_18" + IntToString(i_18));
                                        string a_18_dir_("a_18" + IntToString(i_18));
                                        BTNode *temp_a = new BTNode;
                                        temp_a->child = NULL;
                                        temp_a->path = a_18_dir_;
                                        temp_a->sibling = NULL;
                                        if (i_18 == 0)
                                        {
                                          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                          a_17_level->child = temp_a;
                                          a_18_level = temp_a; // a_level指针作为上一个兄弟节点
                                        }
                                        else
                                        {
                                          // 如果不是第一个节点，就是上一个节点的兄弟节点
                                          a_18_level->sibling = temp_a;
                                          a_18_level = a_18_level->sibling; // a_level移动到下一个兄弟节点
                                        }
                                        if (IsLocal(a_18_dir))
                                        {
                                          MetadataEntry entry;
                                          entry.mutable_permissions();
                                          entry.set_type(DIR);
                                          for (int i_19 = 0; i_19 < a_19; i_19++)
                                          {
                                            entry.add_dir_contents("a_19" + IntToString(i_19));
                                          }
                                          string serialized_entry;
                                          entry.SerializeToString(&serialized_entry);
                                          store_->Put(a_18_dir, serialized_entry, 0);
                                        }
                                        BTNode *a_19_level = NULL; // 这个就指向该层第一个节点
                                        for (int i_19 = 0; i_19 < a_19; i_19++)
                                        {
                                          string a_19_dir(a_18_dir + "/a_19" + IntToString(i_19));
                                          string a_19_dir_("a_19" + IntToString(i_19));
                                          BTNode *temp_a = new BTNode;
                                          temp_a->child = NULL;
                                          temp_a->path = a_19_dir_;
                                          temp_a->sibling = NULL;
                                          if (i_19 == 0)
                                          {
                                            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                            a_18_level->child = temp_a;
                                            a_19_level = temp_a; // a_level指针作为上一个兄弟节点
                                          }
                                          else
                                          {
                                            // 如果不是第一个节点，就是上一个节点的兄弟节点
                                            a_19_level->sibling = temp_a;
                                            a_19_level = a_19_level->sibling; // a_level移动到下一个兄弟节点
                                          }
                                          if (IsLocal(a_19_dir))
                                          {
                                            MetadataEntry entry;
                                            entry.mutable_permissions();
                                            entry.set_type(DIR);
                                            for (int i_20 = 0; i_20 < a_20; i_20++)
                                            {
                                              entry.add_dir_contents("a_20" + IntToString(i_20));
                                            }
                                            string serialized_entry;
                                            entry.SerializeToString(&serialized_entry);
                                            store_->Put(a_19_dir, serialized_entry, 0);
                                          }
                                          BTNode *a_20_level = NULL; // 这个就指向该层第一个节点
                                          for (int i_20 = 0; i_20 < a_20; i_20++)
                                          {
                                            string a_20_dir(a_19_dir + "/a_20" + IntToString(i_20));
                                            string a_20_dir_("a_20" + IntToString(i_20));
                                            BTNode *temp_a = new BTNode;
                                            temp_a->child = NULL;
                                            temp_a->path = a_20_dir_;
                                            temp_a->sibling = NULL;
                                            if (i_20 == 0)
                                            {
                                              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                              a_19_level->child = temp_a;
                                              a_20_level = temp_a; // a_level指针作为上一个兄弟节点
                                            }
                                            else
                                            {
                                              // 如果不是第一个节点，就是上一个节点的兄弟节点
                                              a_20_level->sibling = temp_a;
                                              a_20_level = a_20_level->sibling; // a_level移动到下一个兄弟节点
                                            }
                                            if (IsLocal(a_20_dir))
                                            {
                                              MetadataEntry entry;
                                              entry.mutable_permissions();
                                              entry.set_type(DIR);
                                              for (int j = 0; j < bsize; j++)
                                              {
                                                entry.add_dir_contents("b" + IntToString(j));
                                              }
                                              string serialized_entry;
                                              entry.SerializeToString(&serialized_entry);
                                              store_->Put(a_20_dir, serialized_entry, 0);
                                            }

                                            // Add subdirs.
                                            BTNode *b_level = NULL; // 这个就指向该层第二个节点

                                            string flag_level = IntToString(i_20);
                                            // 现在flag_level目前就是要对拼接到字符串前的标识了
                                            for (int j = 0; j < bsize; j++)
                                            {
                                              string subdir(a_20_dir + "/b" + IntToString(j));
                                              string subdir_("b" + IntToString(j));
                                              string subdir__("/" + flag_level + subdir_);

                                              BTNode *temp_b = new BTNode;
                                              temp_b->child = NULL;
                                              temp_b->path = subdir_;
                                              temp_b->sibling = NULL;

                                              if (j == 0)
                                              {
                                                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                a_20_level->child = temp_b;
                                                b_level = temp_b;
                                              }
                                              else
                                              {
                                                // gaoxuan --不是第一个节点，是上一个的兄弟节点
                                                b_level->sibling = temp_b;
                                                b_level = b_level->sibling;
                                              }
                                              if (IsLocal(subdir__))
                                              {
                                                MetadataEntry entry;
                                                entry.mutable_permissions();
                                                entry.set_type(DIR);
                                                for (int k = 0; k < csize; k++)
                                                {
                                                  entry.add_dir_contents("c" + IntToString(k));
                                                }
                                                string serialized_entry;
                                                entry.SerializeToString(&serialized_entry);
                                                store_->Put(subdir__, serialized_entry, 0);
                                              }
                                              // Add files.
                                              BTNode *c_level = NULL; // 这个就指向该层第三个节点
                                              for (int k = 0; k < csize; k++)
                                              {
                                                string file(subdir__ + "/c" + IntToString(k));
                                                string file_("c" + IntToString(k));

                                                BTNode *temp_c = new BTNode;
                                                temp_c->child = NULL;
                                                temp_c->path = file_;
                                                temp_c->sibling = NULL;
                                                if (k == 0)
                                                {
                                                  // gaoxuan --如果是第一个节点，就作为上一层的孩子
                                                  b_level->child = temp_c;
                                                  c_level = temp_c;
                                                }
                                                else
                                                {
                                                  c_level->sibling = temp_c;
                                                  c_level = c_level->sibling;
                                                }

                                                if (IsLocal(file))
                                                {
                                                  MetadataEntry entry;
                                                  entry.mutable_permissions();
                                                  entry.set_type(DATA);
                                                  FilePart *fp = entry.add_file_parts();
                                                  fp->set_length(RandomSize());
                                                  fp->set_block_id(0);
                                                  fp->set_block_offset(0);
                                                  string serialized_entry;
                                                  entry.SerializeToString(&serialized_entry);
                                                  store_->Put(file, serialized_entry, 0);
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::Init() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

// 这个函数厉害了，增加了uid，上面20层是树，下面10层是hash，这个必须要改，层数太大了，系统炸掉了
void MetadataStore::Init_for_30(BTNode *dir_tree)
{
  // gaoxuan --这里面会涉及到目录树的建立初始化。
  int a_0 = machine_->config().size();
  int a_1 = 2;
  int a_2 = 2;
  int a_3 = 2;
  int a_4 = 2;
  int a_5 = 2;
  int a_6 = 2;
  int a_7 = 2;
  int a_8 = 2;
  int a_9 = 2;
  int a_10 = 2;
  int a_11 = 2;
  int a_12 = 2;
  int a_13 = 2;
  int a_14 = 2;
  int a_15 = 2;
  int a_16 = 2;
  int a_17 = 2;
  int a_18 = 2;
  int a_19 = 2;
  // 下面10层是hash
  int a_20 = 2;
  int a_21 = 2;
  int a_22 = 2;
  int a_23 = 2;
  int a_24 = 2;
  int a_25 = 2;
  int a_26 = 2;
  int a_27 = 2;
  int a_28 = 2;
  int a_29 = 2;

  // 改成5,5测试的时候容易看出来
  // 一种很蠢的方式得到uid
  int a_00 = a_0;
  int a_01 = a_00 * a_1;
  int a_02 = a_01 * a_2;
  int a_03 = a_02 * a_3;
  int a_04 = a_03 * a_4;
  int a_05 = a_04 * a_5;
  int a_06 = a_05 * a_6;
  int a_07 = a_06 * a_7;
  int a_08 = a_07 * a_8;
  int a_09 = a_08 * a_9;
  int a_010 = a_09 * a_10;
  int a_011 = a_010 * a_11;
  int a_012 = a_011 * a_12;
  int a_013 = a_012 * a_13;
  int a_014 = a_013 * a_14;
  int a_015 = a_014 * a_15;
  int a_016 = a_015 * a_16;
  int a_017 = a_016 * a_17;
  int a_018 = a_017 * a_18;
  int a_019 = a_018 * a_19;
  // 下面10层是hash
  int a_020 = a_019 * a_20;
  int a_021 = a_020 * a_21;
  int a_022 = a_021 * a_22;
  int a_023 = a_022 * a_23;
  int a_024 = a_023 * a_24;
  int a_025 = a_024 * a_25;
  int a_026 = a_025 * a_26;
  int a_027 = a_026 * a_27;
  int a_028 = a_027 * a_28;
  int a_029 = a_028 * a_29;

  // 一种很蠢的方式
  int uid_a0 = 1;
  int uid_a1 = a_00 + uid_a0;
  int uid_a2 = a_01 + uid_a1;
  int uid_a3 = a_02 + uid_a2;
  int uid_a4 = a_03 + uid_a3;
  int uid_a5 = a_04 + uid_a4;
  int uid_a6 = a_05 + uid_a5;
  int uid_a7 = a_06 + uid_a6;
  int uid_a8 = a_07 + uid_a7;
  int uid_a9 = a_08 + uid_a8;
  int uid_a10 = a_09 + uid_a9;
  int uid_a11 = a_010 + uid_a10;
  int uid_a12 = a_011 + uid_a11;
  int uid_a13 = a_012 + uid_a12;
  int uid_a14 = a_013 + uid_a13;
  int uid_a15 = a_014 + uid_a14;
  int uid_a16 = a_015 + uid_a15;
  int uid_a17 = a_016 + uid_a16;
  int uid_a18 = a_017 + uid_a17;
  int uid_a19 = a_018 + uid_a18;

  // 下面是分层点的十层
  int uid_a20 = a_019 + uid_a19;
  int uid_a21 = a_020 + uid_a20;
  int uid_a22 = a_021 + uid_a21;
  int uid_a23 = a_022 + uid_a22;
  int uid_a24 = a_023 + uid_a23;
  int uid_a25 = a_024 + uid_a24;
  int uid_a26 = a_025 + uid_a25;
  int uid_a27 = a_026 + uid_a26;
  int uid_a28 = a_027 + uid_a27;
  int uid_a29 = a_028 + uid_a28;
  //

  double start = GetTime();
  // gaoxuan --根节点的指针
  dir_tree->child = NULL;
  dir_tree->sibling = NULL;
  dir_tree->path = "";
  string root_uid = "0";
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    entry.add_dir_contents(root_uid);
    for (int i_0 = 0; i_0 < a_0; i_0++)
    {
      entry.add_dir_contents("a_0" + IntToString(i_0));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }
  BTNode *a_0_level = NULL; // 这个就指向该层第一个节点
  for (int i_0 = 0; i_0 < a_0; i_0++)
  {
    string a_0_dir("/" + root_uid + "a_0" + IntToString(i_0));
    string a_0_dir_("a_0" + IntToString(i_0));
    BTNode *temp_a = new BTNode;
    temp_a->child = NULL;
    temp_a->path = a_0_dir_;
    temp_a->sibling = NULL;
    if (i_0 == 0)
    {
      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
      dir_tree->child = temp_a;
      a_0_level = temp_a; // a_level指针作为上一个兄弟节点
    }
    else
    {
      // 如果不是第一个节点，就是上一个节点的兄弟节点
      a_0_level->sibling = temp_a;
      a_0_level = a_0_level->sibling; // a_level移动到下一个兄弟节点
    }
    string a0_uid = IntToString(uid_a0++); // 第一层的uid，
    if (IsLocal(a_0_dir))
    {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      entry.add_dir_contents(a0_uid);
      for (int i_1 = 0; i_1 < a_1; i_1++)
      {
        entry.add_dir_contents("a_1" + IntToString(i_1));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(a_0_dir, serialized_entry, 0);
    }
    BTNode *a_1_level = NULL; // 这个就指向该层第一个节点

    for (int i_1 = 0; i_1 < a_1; i_1++)
    {
      string a_1_dir("/" + a0_uid + "a_1" + IntToString(i_1));
      string a_1_dir_("a_1" + IntToString(i_1));
      BTNode *temp_a = new BTNode;
      temp_a->child = NULL;
      temp_a->path = a_1_dir_;
      temp_a->sibling = NULL;
      if (i_1 == 0)
      {
        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
        a_0_level->child = temp_a;
        a_1_level = temp_a; // a_level指针作为上一个兄弟节点
      }
      else
      {
        // 如果不是第一个节点，就是上一个节点的兄弟节点
        a_1_level->sibling = temp_a;
        a_1_level = a_1_level->sibling; // a_level移动到下一个兄弟节点
      }
      string a1_uid = IntToString(uid_a1++); // 第二层的uid,这个也没错
      if (IsLocal(a_1_dir))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        entry.add_dir_contents(a1_uid);
        for (int i_2 = 0; i_2 < a_2; i_2++)
        {
          entry.add_dir_contents("a_2" + IntToString(i_2));
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(a_1_dir, serialized_entry, 0);
      }
      BTNode *a_2_level = NULL; // 这个就指向该层第一个节点
      for (int i_2 = 0; i_2 < a_2; i_2++)
      {
        string a_2_dir("/" + a1_uid + "a_2" + IntToString(i_2));
        string a_2_dir_("a_2" + IntToString(i_2));
        BTNode *temp_a = new BTNode;
        temp_a->child = NULL;
        temp_a->path = a_2_dir_;
        temp_a->sibling = NULL;
        if (i_2 == 0)
        {
          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
          a_1_level->child = temp_a;
          a_2_level = temp_a; // a_level指针作为上一个兄弟节点
        }
        else
        {
          // 如果不是第一个节点，就是上一个节点的兄弟节点
          a_2_level->sibling = temp_a;
          a_2_level = a_2_level->sibling; // a_level移动到下一个兄弟节点
        }
        string a2_uid = IntToString(uid_a2++); // 第三层的uid
        if (IsLocal(a_2_dir))
        {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DIR);
          entry.add_dir_contents(a2_uid);
          for (int i_3 = 0; i_3 < a_3; i_3++)
          {
            entry.add_dir_contents("a_3" + IntToString(i_3));
          }
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(a_2_dir, serialized_entry, 0);
        }
        BTNode *a_3_level = NULL; // 这个就指向该层第一个节点
        for (int i_3 = 0; i_3 < a_3; i_3++)
        {
          string a_3_dir("/" + a2_uid + "a_3" + IntToString(i_3));
          string a_3_dir_("a_3" + IntToString(i_3));
          BTNode *temp_a = new BTNode;
          temp_a->child = NULL;
          temp_a->path = a_3_dir_;
          temp_a->sibling = NULL;
          if (i_3 == 0)
          {
            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
            a_2_level->child = temp_a;
            a_3_level = temp_a; // a_level指针作为上一个兄弟节点
          }
          else
          {
            // 如果不是第一个节点，就是上一个节点的兄弟节点
            a_3_level->sibling = temp_a;
            a_3_level = a_3_level->sibling; // a_level移动到下一个兄弟节点
          }

          string a3_uid = IntToString(uid_a3++); // 第四层
          if (IsLocal(a_3_dir))
          {
            MetadataEntry entry;
            entry.mutable_permissions();
            entry.set_type(DIR);
            entry.add_dir_contents(a3_uid);
            for (int i_4 = 0; i_4 < a_4; i_4++)
            {
              entry.add_dir_contents("a_4" + IntToString(i_4));
            }
            string serialized_entry;
            entry.SerializeToString(&serialized_entry);
            store_->Put(a_3_dir, serialized_entry, 0);
          }
          BTNode *a_4_level = NULL; // 这个就指向该层第一个节点
          for (int i_4 = 0; i_4 < a_4; i_4++)
          {
            string a_4_dir("/" + a3_uid + "a_4" + IntToString(i_4));
            string a_4_dir_("a_4" + IntToString(i_4));
            BTNode *temp_a = new BTNode;
            temp_a->child = NULL;
            temp_a->path = a_4_dir_;
            temp_a->sibling = NULL;
            if (i_4 == 0)
            {
              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
              a_3_level->child = temp_a;
              a_4_level = temp_a; // a_level指针作为上一个兄弟节点
            }
            else
            {
              // 如果不是第一个节点，就是上一个节点的兄弟节点
              a_4_level->sibling = temp_a;
              a_4_level = a_4_level->sibling; // a_level移动到下一个兄弟节点
            }
            string a4_uid = IntToString(uid_a4++); // 第四层
            if (IsLocal(a_4_dir))
            {
              MetadataEntry entry;
              entry.mutable_permissions();
              entry.set_type(DIR);
              entry.add_dir_contents(a4_uid);
              for (int i_5 = 0; i_5 < a_5; i_5++)
              {
                entry.add_dir_contents("a_5" + IntToString(i_5));
              }
              string serialized_entry;
              entry.SerializeToString(&serialized_entry);
              store_->Put(a_4_dir, serialized_entry, 0);
            }
            BTNode *a_5_level = NULL; // 这个就指向该层第一个节点
            for (int i_5 = 0; i_5 < a_5; i_5++)
            {
              string a_5_dir("/" + a4_uid + "a_5" + IntToString(i_5));
              string a_5_dir_("a_5" + IntToString(i_5));
              BTNode *temp_a = new BTNode;
              temp_a->child = NULL;
              temp_a->path = a_5_dir_;
              temp_a->sibling = NULL;
              if (i_5 == 0)
              {
                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                a_4_level->child = temp_a;
                a_5_level = temp_a; // a_level指针作为上一个兄弟节点
              }
              else
              {
                // 如果不是第一个节点，就是上一个节点的兄弟节点
                a_5_level->sibling = temp_a;
                a_5_level = a_5_level->sibling; // a_level移动到下一个兄弟节点
              }
              string a5_uid = IntToString(uid_a5++); // 第四层
              if (IsLocal(a_5_dir))
              {
                MetadataEntry entry;
                entry.mutable_permissions();
                entry.set_type(DIR);
                entry.add_dir_contents(a5_uid);
                for (int i_6 = 0; i_6 < a_6; i_6++)
                {
                  entry.add_dir_contents("a_6" + IntToString(i_6));
                }
                string serialized_entry;
                entry.SerializeToString(&serialized_entry);
                store_->Put(a_5_dir, serialized_entry, 0);
              }
              BTNode *a_6_level = NULL; // 这个就指向该层第一个节点
              for (int i_6 = 0; i_6 < a_6; i_6++)
              {
                string a_6_dir("/" + a5_uid + "a_6" + IntToString(i_6));
                string a_6_dir_("a_6" + IntToString(i_6));
                BTNode *temp_a = new BTNode;
                temp_a->child = NULL;
                temp_a->path = a_6_dir_;
                temp_a->sibling = NULL;
                if (i_6 == 0)
                {
                  // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                  a_5_level->child = temp_a;
                  a_6_level = temp_a; // a_level指针作为上一个兄弟节点
                }
                else
                {
                  // 如果不是第一个节点，就是上一个节点的兄弟节点
                  a_6_level->sibling = temp_a;
                  a_6_level = a_6_level->sibling; // a_level移动到下一个兄弟节点
                }
                string a6_uid = IntToString(uid_a6++); // 第四层
                if (IsLocal(a_6_dir))
                {
                  MetadataEntry entry;
                  entry.mutable_permissions();
                  entry.set_type(DIR);
                  entry.add_dir_contents(a6_uid);
                  for (int i_7 = 0; i_7 < a_7; i_7++)
                  {
                    entry.add_dir_contents("a_7" + IntToString(i_7));
                  }
                  string serialized_entry;
                  entry.SerializeToString(&serialized_entry);
                  store_->Put(a_6_dir, serialized_entry, 0);
                }
                BTNode *a_7_level = NULL; // 这个就指向该层第一个节点
                for (int i_7 = 0; i_7 < a_7; i_7++)
                {
                  string a_7_dir("/" + a6_uid + "a_7" + IntToString(i_7));
                  string a_7_dir_("a_7" + IntToString(i_7));
                  BTNode *temp_a = new BTNode;
                  temp_a->child = NULL;
                  temp_a->path = a_7_dir_;
                  temp_a->sibling = NULL;
                  if (i_7 == 0)
                  {
                    // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                    a_6_level->child = temp_a;
                    a_7_level = temp_a; // a_level指针作为上一个兄弟节点
                  }
                  else
                  {
                    // 如果不是第一个节点，就是上一个节点的兄弟节点
                    a_7_level->sibling = temp_a;
                    a_7_level = a_7_level->sibling; // a_level移动到下一个兄弟节点
                  }
                  string a7_uid = IntToString(uid_a7++); // 第四层
                  if (IsLocal(a_7_dir))
                  {
                    MetadataEntry entry;
                    entry.mutable_permissions();
                    entry.set_type(DIR);
                    entry.add_dir_contents(a7_uid);
                    for (int i_8 = 0; i_8 < a_8; i_8++)
                    {
                      entry.add_dir_contents("a_8" + IntToString(i_8));
                    }
                    string serialized_entry;
                    entry.SerializeToString(&serialized_entry);
                    store_->Put(a_7_dir, serialized_entry, 0);
                  }
                  BTNode *a_8_level = NULL; // 这个就指向该层第一个节点
                  for (int i_8 = 0; i_8 < a_8; i_8++)
                  {
                    string a_8_dir("/" + a7_uid + "a_8" + IntToString(i_8));
                    string a_8_dir_("a_8" + IntToString(i_8));
                    BTNode *temp_a = new BTNode;
                    temp_a->child = NULL;
                    temp_a->path = a_8_dir_;
                    temp_a->sibling = NULL;
                    if (i_8 == 0)
                    {
                      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                      a_7_level->child = temp_a;
                      a_8_level = temp_a; // a_level指针作为上一个兄弟节点
                    }
                    else
                    {
                      // 如果不是第一个节点，就是上一个节点的兄弟节点
                      a_8_level->sibling = temp_a;
                      a_8_level = a_8_level->sibling; // a_level移动到下一个兄弟节点
                    }
                    string a8_uid = IntToString(uid_a8++); // 第四层
                    if (IsLocal(a_8_dir))
                    {
                      MetadataEntry entry;
                      entry.mutable_permissions();
                      entry.set_type(DIR);
                      entry.add_dir_contents(a8_uid);
                      for (int i_9 = 0; i_9 < a_9; i_9++)
                      {
                        entry.add_dir_contents("a_9" + IntToString(i_9));
                      }
                      string serialized_entry;
                      entry.SerializeToString(&serialized_entry);
                      store_->Put(a_8_dir, serialized_entry, 0);
                    }
                    BTNode *a_9_level = NULL; // 这个就指向该层第一个节点
                    for (int i_9 = 0; i_9 < a_9; i_9++)
                    {
                      string a_9_dir("/" + a8_uid + "a_9" + IntToString(i_9));
                      string a_9_dir_("a_9" + IntToString(i_9));
                      BTNode *temp_a = new BTNode;
                      temp_a->child = NULL;
                      temp_a->path = a_9_dir_;
                      temp_a->sibling = NULL;
                      if (i_9 == 0)
                      {
                        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                        a_8_level->child = temp_a;
                        a_9_level = temp_a; // a_level指针作为上一个兄弟节点
                      }
                      else
                      {
                        // 如果不是第一个节点，就是上一个节点的兄弟节点
                        a_9_level->sibling = temp_a;
                        a_9_level = a_9_level->sibling; // a_level移动到下一个兄弟节点
                      }
                      string a9_uid = IntToString(uid_a9++); // 第四层
                      if (IsLocal(a_9_dir))
                      {
                        MetadataEntry entry;
                        entry.mutable_permissions();
                        entry.set_type(DIR);
                        entry.add_dir_contents(a9_uid);
                        for (int i_10 = 0; i_10 < a_10; i_10++)
                        {
                          entry.add_dir_contents("a_10" + IntToString(i_10));
                        }
                        string serialized_entry;
                        entry.SerializeToString(&serialized_entry);
                        store_->Put(a_9_dir, serialized_entry, 0);
                      }
                      BTNode *a_10_level = NULL; // 这个就指向该层第一个节点
                      for (int i_10 = 0; i_10 < a_10; i_10++)
                      {
                        string a_10_dir("/" + a9_uid + "a_10" + IntToString(i_10));
                        string a_10_dir_("a_10" + IntToString(i_10));
                        BTNode *temp_a = new BTNode;
                        temp_a->child = NULL;
                        temp_a->path = a_10_dir_;
                        temp_a->sibling = NULL;
                        if (i_10 == 0)
                        {
                          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                          a_9_level->child = temp_a;
                          a_10_level = temp_a; // a_level指针作为上一个兄弟节点
                        }
                        else
                        {
                          // 如果不是第一个节点，就是上一个节点的兄弟节点
                          a_10_level->sibling = temp_a;
                          a_10_level = a_10_level->sibling; // a_level移动到下一个兄弟节点
                        }
                        string a10_uid = IntToString(uid_a10++); // 第四层
                        if (IsLocal(a_10_dir))
                        {
                          MetadataEntry entry;
                          entry.mutable_permissions();
                          entry.set_type(DIR);
                          entry.add_dir_contents(a10_uid);
                          for (int i_11 = 0; i_11 < a_11; i_11++)
                          {
                            entry.add_dir_contents("a_11" + IntToString(i_11));
                          }
                          string serialized_entry;
                          entry.SerializeToString(&serialized_entry);
                          store_->Put(a_10_dir, serialized_entry, 0);
                        }
                        BTNode *a_11_level = NULL; // 这个就指向该层第一个节点
                        for (int i_11 = 0; i_11 < a_11; i_11++)
                        {
                          string a_11_dir("/" + a10_uid + "a_11" + IntToString(i_11));
                          string a_11_dir_("a_11" + IntToString(i_11));
                          BTNode *temp_a = new BTNode;
                          temp_a->child = NULL;
                          temp_a->path = a_11_dir_;
                          temp_a->sibling = NULL;
                          if (i_11 == 0)
                          {
                            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                            a_10_level->child = temp_a;
                            a_11_level = temp_a; // a_level指针作为上一个兄弟节点
                          }
                          else
                          {
                            // 如果不是第一个节点，就是上一个节点的兄弟节点
                            a_11_level->sibling = temp_a;
                            a_11_level = a_11_level->sibling; // a_level移动到下一个兄弟节点
                          }
                          string a11_uid = IntToString(uid_a11++); // 第四层
                          if (IsLocal(a_11_dir))
                          {
                            MetadataEntry entry;
                            entry.mutable_permissions();
                            entry.set_type(DIR);
                            entry.add_dir_contents(a11_uid);
                            for (int i_12 = 0; i_12 < a_12; i_12++)
                            {
                              entry.add_dir_contents("a_12" + IntToString(i_12));
                            }
                            string serialized_entry;
                            entry.SerializeToString(&serialized_entry);
                            store_->Put(a_11_dir, serialized_entry, 0);
                          }
                          BTNode *a_12_level = NULL; // 这个就指向该层第一个节点
                          for (int i_12 = 0; i_12 < a_12; i_12++)
                          {
                            string a_12_dir("/" + a11_uid + "a_12" + IntToString(i_12));
                            string a_12_dir_("a_12" + IntToString(i_12));
                            BTNode *temp_a = new BTNode;
                            temp_a->child = NULL;
                            temp_a->path = a_12_dir_;
                            temp_a->sibling = NULL;
                            if (i_12 == 0)
                            {
                              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                              a_11_level->child = temp_a;
                              a_12_level = temp_a; // a_level指针作为上一个兄弟节点
                            }
                            else
                            {
                              // 如果不是第一个节点，就是上一个节点的兄弟节点
                              a_12_level->sibling = temp_a;
                              a_12_level = a_12_level->sibling; // a_level移动到下一个兄弟节点
                            }
                            string a12_uid = IntToString(uid_a12++); // 第四层
                            if (IsLocal(a_12_dir))
                            {
                              MetadataEntry entry;
                              entry.mutable_permissions();
                              entry.set_type(DIR);
                              entry.add_dir_contents(a12_uid);
                              for (int i_13 = 0; i_13 < a_13; i_13++)
                              {
                                entry.add_dir_contents("a_13" + IntToString(i_13));
                              }
                              string serialized_entry;
                              entry.SerializeToString(&serialized_entry);
                              store_->Put(a_12_dir, serialized_entry, 0);
                            }
                            BTNode *a_13_level = NULL; // 这个就指向该层第一个节点
                            for (int i_13 = 0; i_13 < a_13; i_13++)
                            {
                              string a_13_dir("/" + a12_uid + "a_13" + IntToString(i_13));
                              string a_13_dir_("a_13" + IntToString(i_13));
                              BTNode *temp_a = new BTNode;
                              temp_a->child = NULL;
                              temp_a->path = a_13_dir_;
                              temp_a->sibling = NULL;
                              if (i_13 == 0)
                              {
                                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                a_12_level->child = temp_a;
                                a_13_level = temp_a; // a_level指针作为上一个兄弟节点
                              }
                              else
                              {
                                // 如果不是第一个节点，就是上一个节点的兄弟节点
                                a_13_level->sibling = temp_a;
                                a_13_level = a_13_level->sibling; // a_level移动到下一个兄弟节点
                              }
                              string a13_uid = IntToString(uid_a13++); // 第四层
                              if (IsLocal(a_13_dir))
                              {
                                MetadataEntry entry;
                                entry.mutable_permissions();
                                entry.set_type(DIR);
                                entry.add_dir_contents(a13_uid);
                                for (int i_14 = 0; i_14 < a_14; i_14++)
                                {
                                  entry.add_dir_contents("a_14" + IntToString(i_14));
                                }
                                string serialized_entry;
                                entry.SerializeToString(&serialized_entry);
                                store_->Put(a_13_dir, serialized_entry, 0);
                              }
                              BTNode *a_14_level = NULL; // 这个就指向该层第一个节点
                              for (int i_14 = 0; i_14 < a_14; i_14++)
                              {
                                string a_14_dir("/" + a13_uid + "a_14" + IntToString(i_14));
                                string a_14_dir_("a_14" + IntToString(i_14));
                                BTNode *temp_a = new BTNode;
                                temp_a->child = NULL;
                                temp_a->path = a_14_dir_;
                                temp_a->sibling = NULL;
                                if (i_14 == 0)
                                {
                                  // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                  a_13_level->child = temp_a;
                                  a_14_level = temp_a; // a_level指针作为上一个兄弟节点
                                }
                                else
                                {
                                  // 如果不是第一个节点，就是上一个节点的兄弟节点
                                  a_14_level->sibling = temp_a;
                                  a_14_level = a_14_level->sibling; // a_level移动到下一个兄弟节点
                                }
                                string a14_uid = IntToString(uid_a14++); // 第四层
                                if (IsLocal(a_14_dir))
                                {
                                  MetadataEntry entry;
                                  entry.mutable_permissions();
                                  entry.set_type(DIR);
                                  entry.add_dir_contents(a14_uid);
                                  for (int i_15 = 0; i_15 < a_15; i_15++)
                                  {
                                    entry.add_dir_contents("a_15" + IntToString(i_15));
                                  }
                                  string serialized_entry;
                                  entry.SerializeToString(&serialized_entry);
                                  store_->Put(a_14_dir, serialized_entry, 0);
                                }
                                BTNode *a_15_level = NULL; // 这个就指向该层第一个节点
                                for (int i_15 = 0; i_15 < a_15; i_15++)
                                {
                                  string a_15_dir("/" + a14_uid + "a_15" + IntToString(i_15));
                                  string a_15_dir_("a_15" + IntToString(i_15));
                                  BTNode *temp_a = new BTNode;
                                  temp_a->child = NULL;
                                  temp_a->path = a_15_dir_;
                                  temp_a->sibling = NULL;
                                  if (i_15 == 0)
                                  {
                                    // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                    a_14_level->child = temp_a;
                                    a_15_level = temp_a; // a_level指针作为上一个兄弟节点
                                  }
                                  else
                                  {
                                    // 如果不是第一个节点，就是上一个节点的兄弟节点
                                    a_15_level->sibling = temp_a;
                                    a_15_level = a_15_level->sibling; // a_level移动到下一个兄弟节点
                                  }
                                  string a15_uid = IntToString(uid_a15++); // 第四层
                                  if (IsLocal(a_15_dir))
                                  {
                                    MetadataEntry entry;
                                    entry.mutable_permissions();
                                    entry.set_type(DIR);
                                    entry.add_dir_contents(a15_uid);
                                    for (int i_16 = 0; i_16 < a_16; i_16++)
                                    {
                                      entry.add_dir_contents("a_16" + IntToString(i_16));
                                    }
                                    string serialized_entry;
                                    entry.SerializeToString(&serialized_entry);
                                    store_->Put(a_15_dir, serialized_entry, 0);
                                  }
                                  BTNode *a_16_level = NULL; // 这个就指向该层第一个节点
                                  for (int i_16 = 0; i_16 < a_16; i_16++)
                                  {
                                    string a_16_dir("/" + a15_uid + "a_16" + IntToString(i_16));
                                    string a_16_dir_("a_16" + IntToString(i_16));
                                    BTNode *temp_a = new BTNode;
                                    temp_a->child = NULL;
                                    temp_a->path = a_16_dir_;
                                    temp_a->sibling = NULL;
                                    if (i_16 == 0)
                                    {
                                      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                      a_15_level->child = temp_a;
                                      a_16_level = temp_a; // a_level指针作为上一个兄弟节点
                                    }
                                    else
                                    {
                                      // 如果不是第一个节点，就是上一个节点的兄弟节点
                                      a_16_level->sibling = temp_a;
                                      a_16_level = a_16_level->sibling; // a_level移动到下一个兄弟节点
                                    }
                                    string a16_uid = IntToString(uid_a16++); // 第四层
                                    if (IsLocal(a_16_dir))
                                    {
                                      MetadataEntry entry;
                                      entry.mutable_permissions();
                                      entry.set_type(DIR);
                                      entry.add_dir_contents(a16_uid);
                                      for (int i_17 = 0; i_17 < a_17; i_17++)
                                      {
                                        entry.add_dir_contents("a_17" + IntToString(i_17));
                                      }
                                      string serialized_entry;
                                      entry.SerializeToString(&serialized_entry);
                                      store_->Put(a_16_dir, serialized_entry, 0);
                                    }
                                    BTNode *a_17_level = NULL; // 这个就指向该层第一个节点
                                    for (int i_17 = 0; i_17 < a_17; i_17++)
                                    {
                                      string a_17_dir("/" + a16_uid + "a_17" + IntToString(i_17));
                                      string a_17_dir_("a_17" + IntToString(i_17));
                                      BTNode *temp_a = new BTNode;
                                      temp_a->child = NULL;
                                      temp_a->path = a_17_dir_;
                                      temp_a->sibling = NULL;
                                      if (i_17 == 0)
                                      {
                                        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                        a_16_level->child = temp_a;
                                        a_17_level = temp_a; // a_level指针作为上一个兄弟节点
                                      }
                                      else
                                      {
                                        // 如果不是第一个节点，就是上一个节点的兄弟节点
                                        a_17_level->sibling = temp_a;
                                        a_17_level = a_17_level->sibling; // a_level移动到下一个兄弟节点
                                      }
                                      string a17_uid = IntToString(uid_a17++); // 第四层
                                      if (IsLocal(a_17_dir))
                                      {
                                        MetadataEntry entry;
                                        entry.mutable_permissions();
                                        entry.set_type(DIR);
                                        entry.add_dir_contents(a17_uid);
                                        for (int i_18 = 0; i_18 < a_18; i_18++)
                                        {
                                          entry.add_dir_contents("a_18" + IntToString(i_18));
                                        }
                                        string serialized_entry;
                                        entry.SerializeToString(&serialized_entry);
                                        store_->Put(a_17_dir, serialized_entry, 0);
                                      }
                                      BTNode *a_18_level = NULL; // 这个就指向该层第一个节点
                                      for (int i_18 = 0; i_18 < a_18; i_18++)
                                      {
                                        string a_18_dir("/" + a17_uid + "a_18" + IntToString(i_18));
                                        string a_18_dir_("a_18" + IntToString(i_18));
                                        BTNode *temp_a = new BTNode;
                                        temp_a->child = NULL;
                                        temp_a->path = a_18_dir_;
                                        temp_a->sibling = NULL;
                                        if (i_18 == 0)
                                        {
                                          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                          a_17_level->child = temp_a;
                                          a_18_level = temp_a; // a_level指针作为上一个兄弟节点
                                        }
                                        else
                                        {
                                          // 如果不是第一个节点，就是上一个节点的兄弟节点
                                          a_18_level->sibling = temp_a;
                                          a_18_level = a_18_level->sibling; // a_level移动到下一个兄弟节点
                                        }
                                        string a18_uid = IntToString(uid_a18++); // 第四层
                                        if (IsLocal(a_18_dir))
                                        {
                                          MetadataEntry entry;
                                          entry.mutable_permissions();
                                          entry.set_type(DIR);
                                          entry.add_dir_contents(a18_uid);
                                          for (int i_19 = 0; i_19 < a_19; i_19++)
                                          {
                                            entry.add_dir_contents("a_19" + IntToString(i_19));
                                          }
                                          string serialized_entry;
                                          entry.SerializeToString(&serialized_entry);
                                          store_->Put(a_18_dir, serialized_entry, 0);
                                        }
                                        BTNode *a_19_level = NULL; // 这个就指向该层第一个节点
                                        for (int i_19 = 0; i_19 < a_19; i_19++)
                                        {
                                          string a_19_dir("/" + a18_uid + "a_19" + IntToString(i_19));
                                          string a_19_dir_("a_19" + IntToString(i_19));
                                          BTNode *temp_a = new BTNode;
                                          temp_a->child = NULL;
                                          temp_a->path = a_19_dir_;
                                          temp_a->sibling = NULL;
                                          if (i_19 == 0)
                                          {
                                            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                            a_18_level->child = temp_a;
                                            a_19_level = temp_a; // a_level指针作为上一个兄弟节点
                                          }
                                          else
                                          {
                                            // 如果不是第一个节点，就是上一个节点的兄弟节点
                                            a_19_level->sibling = temp_a;
                                            a_19_level = a_19_level->sibling; // a_level移动到下一个兄弟节点
                                          }
                                          string a19_uid = IntToString(uid_a19++); // 第四层
                                          if (IsLocal(a_19_dir))
                                          {
                                            MetadataEntry entry;
                                            entry.mutable_permissions();
                                            entry.set_type(DIR);
                                            entry.add_dir_contents(a19_uid);
                                            for (int i_20 = 0; i_20 < a_20; i_20++)
                                            {
                                              entry.add_dir_contents("b" + IntToString(i_20));
                                            }
                                            string serialized_entry;
                                            entry.SerializeToString(&serialized_entry);
                                            store_->Put(a_19_dir, serialized_entry, 0);
                                          }
                                          BTNode *a_20_level = NULL; // 这个就指向该层第一个节点
                                          for (int i_20 = 0; i_20 < a_20; i_20++)
                                          {
                                            string a_20_dir("/" + a19_uid + "b" + IntToString(i_20));
                                            string a_20_dir_("b" + IntToString(i_20));
                                            BTNode *temp_a = new BTNode;
                                            temp_a->child = NULL;
                                            temp_a->path = a_20_dir_;
                                            temp_a->sibling = NULL;
                                            if (i_20 == 0)
                                            {
                                              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                              a_19_level->child = temp_a;
                                              a_20_level = temp_a; // a_level指针作为上一个兄弟节点
                                            }
                                            else
                                            {
                                              // 如果不是第一个节点，就是上一个节点的兄弟节点
                                              a_20_level->sibling = temp_a;
                                              a_20_level = a_20_level->sibling; // a_level移动到下一个兄弟节点
                                            }
                                            string a20_uid = IntToString(uid_a20++); // 第四层
                                            if (IsLocal(a_20_dir))
                                            {
                                              MetadataEntry entry;
                                              entry.mutable_permissions();
                                              entry.set_type(DIR);
                                              entry.add_dir_contents(a20_uid);
                                              for (int i_21 = 0; i_21 < a_21; i_21++)
                                              {
                                                entry.add_dir_contents("c_0" + IntToString(i_21));
                                              }
                                              string serialized_entry;
                                              entry.SerializeToString(&serialized_entry);
                                              store_->Put(a_20_dir, serialized_entry, 0);
                                            }
                                            BTNode *a_21_level = NULL; // 这个就指向该层第一个节点
                                            for (int i_21 = 0; i_21 < a_21; i_21++)
                                            {
                                              string a_21_dir(a_20_dir + "/c_0" + IntToString(i_21));
                                              string a_21_dir_("c_0" + IntToString(i_21));
                                              BTNode *temp_a = new BTNode;
                                              temp_a->child = NULL;
                                              temp_a->path = a_21_dir_;
                                              temp_a->sibling = NULL;
                                              if (i_21 == 0)
                                              {
                                                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                a_20_level->child = temp_a;
                                                a_21_level = temp_a; // a_level指针作为上一个兄弟节点
                                              }
                                              else
                                              {
                                                // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                a_21_level->sibling = temp_a;
                                                a_21_level = a_21_level->sibling; // a_level移动到下一个兄弟节点
                                              }
                                              string a21_uid = IntToString(uid_a21++); // 第四层
                                              if (IsLocal(a_21_dir))
                                              {
                                                MetadataEntry entry;
                                                entry.mutable_permissions();
                                                entry.set_type(DIR);
                                                entry.add_dir_contents(a21_uid);
                                                for (int i_22 = 0; i_22 < a_22; i_22++)
                                                {
                                                  entry.add_dir_contents("c_1" + IntToString(i_22));
                                                }
                                                string serialized_entry;
                                                entry.SerializeToString(&serialized_entry);
                                                store_->Put(a_21_dir, serialized_entry, 0);
                                              }
                                              BTNode *a_22_level = NULL; // 这个就指向该层第一个节点
                                              for (int i_22 = 0; i_22 < a_22; i_22++)
                                              {
                                                string a_22_dir(a_21_dir + "/c_1" + IntToString(i_22));
                                                string a_22_dir_("c_1" + IntToString(i_22));
                                                BTNode *temp_a = new BTNode;
                                                temp_a->child = NULL;
                                                temp_a->path = a_22_dir_;
                                                temp_a->sibling = NULL;
                                                if (i_22 == 0)
                                                {
                                                  // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                  a_21_level->child = temp_a;
                                                  a_22_level = temp_a; // a_level指针作为上一个兄弟节点
                                                }
                                                else
                                                {
                                                  // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                  a_22_level->sibling = temp_a;
                                                  a_22_level = a_22_level->sibling; // a_level移动到下一个兄弟节点
                                                }
                                                string a22_uid = IntToString(uid_a22++); // 第四层
                                                if (IsLocal(a_22_dir))
                                                {
                                                  MetadataEntry entry;
                                                  entry.mutable_permissions();
                                                  entry.set_type(DIR);
                                                  entry.add_dir_contents(a22_uid);
                                                  for (int i_23 = 0; i_23 < a_23; i_23++)
                                                  {
                                                    entry.add_dir_contents("c_2" + IntToString(i_23));
                                                  }
                                                  string serialized_entry;
                                                  entry.SerializeToString(&serialized_entry);
                                                  store_->Put(a_22_dir, serialized_entry, 0);
                                                }
                                                BTNode *a_23_level = NULL; // 这个就指向该层第一个节点
                                                for (int i_23 = 0; i_23 < a_23; i_23++)
                                                {
                                                  string a_23_dir(a_22_dir + "/c_2" + IntToString(i_23));
                                                  string a_23_dir_("c_2" + IntToString(i_23));
                                                  BTNode *temp_a = new BTNode;
                                                  temp_a->child = NULL;
                                                  temp_a->path = a_23_dir_;
                                                  temp_a->sibling = NULL;
                                                  if (i_23 == 0)
                                                  {
                                                    // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                    a_22_level->child = temp_a;
                                                    a_23_level = temp_a; // a_level指针作为上一个兄弟节点
                                                  }
                                                  else
                                                  {
                                                    // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                    a_23_level->sibling = temp_a;
                                                    a_23_level = a_23_level->sibling; // a_level移动到下一个兄弟节点
                                                  }
                                                  string a23_uid = IntToString(uid_a23++); // 第四层
                                                  if (IsLocal(a_23_dir))
                                                  {
                                                    MetadataEntry entry;
                                                    entry.mutable_permissions();
                                                    entry.set_type(DIR);
                                                    entry.add_dir_contents(a23_uid);
                                                    for (int i_24 = 0; i_24 < a_24; i_24++)
                                                    {
                                                      entry.add_dir_contents("c_3" + IntToString(i_24));
                                                    }
                                                    string serialized_entry;
                                                    entry.SerializeToString(&serialized_entry);
                                                    store_->Put(a_23_dir, serialized_entry, 0);
                                                  }
                                                  BTNode *a_24_level = NULL; // 这个就指向该层第一个节点
                                                  for (int i_24 = 0; i_24 < a_24; i_24++)
                                                  {
                                                    string a_24_dir(a_23_dir + "/c_3" + IntToString(i_24));
                                                    string a_24_dir_("c_3" + IntToString(i_24));
                                                    BTNode *temp_a = new BTNode;
                                                    temp_a->child = NULL;
                                                    temp_a->path = a_24_dir_;
                                                    temp_a->sibling = NULL;
                                                    if (i_24 == 0)
                                                    {
                                                      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                      a_23_level->child = temp_a;
                                                      a_24_level = temp_a; // a_level指针作为上一个兄弟节点
                                                    }
                                                    else
                                                    {
                                                      // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                      a_24_level->sibling = temp_a;
                                                      a_24_level = a_24_level->sibling; // a_level移动到下一个兄弟节点
                                                    }
                                                    string a24_uid = IntToString(uid_a24++); // 第四层
                                                    if (IsLocal(a_24_dir))
                                                    {
                                                      MetadataEntry entry;
                                                      entry.mutable_permissions();
                                                      entry.set_type(DIR);
                                                      entry.add_dir_contents(a24_uid);
                                                      for (int i_25 = 0; i_25 < a_25; i_25++)
                                                      {
                                                        entry.add_dir_contents("c_4" + IntToString(i_25));
                                                      }
                                                      string serialized_entry;
                                                      entry.SerializeToString(&serialized_entry);
                                                      store_->Put(a_24_dir, serialized_entry, 0);
                                                    }
                                                    BTNode *a_25_level = NULL; // 这个就指向该层第一个节点
                                                    for (int i_25 = 0; i_25 < a_25; i_25++)
                                                    {
                                                      string a_25_dir(a_24_dir + "/c_4" + IntToString(i_25));
                                                      string a_25_dir_("c_4" + IntToString(i_25));
                                                      BTNode *temp_a = new BTNode;
                                                      temp_a->child = NULL;
                                                      temp_a->path = a_25_dir_;
                                                      temp_a->sibling = NULL;
                                                      if (i_25 == 0)
                                                      {
                                                        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                        a_24_level->child = temp_a;
                                                        a_25_level = temp_a; // a_level指针作为上一个兄弟节点
                                                      }
                                                      else
                                                      {
                                                        // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                        a_25_level->sibling = temp_a;
                                                        a_25_level = a_25_level->sibling; // a_level移动到下一个兄弟节点
                                                      }
                                                      string a25_uid = IntToString(uid_a25++); // 第四层
                                                      if (IsLocal(a_25_dir))
                                                      {
                                                        MetadataEntry entry;
                                                        entry.mutable_permissions();
                                                        entry.set_type(DIR);
                                                        entry.add_dir_contents(a25_uid);
                                                        for (int i_26 = 0; i_26 < a_26; i_26++)
                                                        {
                                                          entry.add_dir_contents("c_5" + IntToString(i_26));
                                                        }
                                                        string serialized_entry;
                                                        entry.SerializeToString(&serialized_entry);
                                                        store_->Put(a_25_dir, serialized_entry, 0);
                                                      }
                                                      BTNode *a_26_level = NULL; // 这个就指向该层第一个节点
                                                      for (int i_26 = 0; i_26 < a_26; i_26++)
                                                      {
                                                        string a_26_dir(a_25_dir + "/c_5" + IntToString(i_26));
                                                        string a_26_dir_("c_5" + IntToString(i_26));
                                                        BTNode *temp_a = new BTNode;
                                                        temp_a->child = NULL;
                                                        temp_a->path = a_26_dir_;
                                                        temp_a->sibling = NULL;
                                                        if (i_26 == 0)
                                                        {
                                                          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                          a_25_level->child = temp_a;
                                                          a_26_level = temp_a; // a_level指针作为上一个兄弟节点
                                                        }
                                                        else
                                                        {
                                                          // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                          a_26_level->sibling = temp_a;
                                                          a_26_level = a_26_level->sibling; // a_level移动到下一个兄弟节点
                                                        }
                                                        string a26_uid = IntToString(uid_a26++); // 第四层
                                                        if (IsLocal(a_26_dir))
                                                        {
                                                          MetadataEntry entry;
                                                          entry.mutable_permissions();
                                                          entry.set_type(DIR);
                                                          entry.add_dir_contents(a26_uid);
                                                          for (int i_27 = 0; i_27 < a_27; i_27++)
                                                          {
                                                            entry.add_dir_contents("c_6" + IntToString(i_27));
                                                          }
                                                          string serialized_entry;
                                                          entry.SerializeToString(&serialized_entry);
                                                          store_->Put(a_26_dir, serialized_entry, 0);
                                                        }
                                                        BTNode *a_27_level = NULL; // 这个就指向该层第一个节点
                                                        for (int i_27 = 0; i_27 < a_27; i_27++)
                                                        {
                                                          string a_27_dir(a_26_dir + "/c_6" + IntToString(i_27));
                                                          string a_27_dir_("c_6" + IntToString(i_27));
                                                          BTNode *temp_a = new BTNode;
                                                          temp_a->child = NULL;
                                                          temp_a->path = a_27_dir_;
                                                          temp_a->sibling = NULL;
                                                          if (i_27 == 0)
                                                          {
                                                            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                            a_26_level->child = temp_a;
                                                            a_27_level = temp_a; // a_level指针作为上一个兄弟节点
                                                          }
                                                          else
                                                          {
                                                            // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                            a_27_level->sibling = temp_a;
                                                            a_27_level = a_27_level->sibling; // a_level移动到下一个兄弟节点
                                                          }
                                                          string a27_uid = IntToString(uid_a27++); // 第四层
                                                          if (IsLocal(a_27_dir))
                                                          {
                                                            MetadataEntry entry;
                                                            entry.mutable_permissions();
                                                            entry.set_type(DIR);
                                                            entry.add_dir_contents(a27_uid);
                                                            for (int i_28 = 0; i_28 < a_28; i_28++)
                                                            {
                                                              entry.add_dir_contents("c_7" + IntToString(i_28));
                                                            }
                                                            string serialized_entry;
                                                            entry.SerializeToString(&serialized_entry);
                                                            store_->Put(a_27_dir, serialized_entry, 0);
                                                          }
                                                          BTNode *a_28_level = NULL; // 这个就指向该层第一个节点
                                                          for (int i_28 = 0; i_28 < a_28; i_28++)
                                                          {
                                                            string a_28_dir(a_27_dir + "/c_7" + IntToString(i_28));
                                                            string a_28_dir_("c_7" + IntToString(i_28));
                                                            BTNode *temp_a = new BTNode;
                                                            temp_a->child = NULL;
                                                            temp_a->path = a_28_dir_;
                                                            temp_a->sibling = NULL;
                                                            if (i_28 == 0)
                                                            {
                                                              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                              a_27_level->child = temp_a;
                                                              a_28_level = temp_a; // a_level指针作为上一个兄弟节点
                                                            }
                                                            else
                                                            {
                                                              // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                              a_28_level->sibling = temp_a;
                                                              a_28_level = a_28_level->sibling; // a_level移动到下一个兄弟节点
                                                            }
                                                            string a28_uid = IntToString(uid_a28++); // 第四层
                                                            if (IsLocal(a_28_dir))
                                                            {
                                                              MetadataEntry entry;
                                                              entry.mutable_permissions();
                                                              entry.set_type(DIR);
                                                              entry.add_dir_contents(a28_uid);
                                                              for (int i_29 = 0; i_29 < a_29; i_29++)
                                                              {
                                                                entry.add_dir_contents("c_8" + IntToString(i_29));
                                                              }
                                                              string serialized_entry;
                                                              entry.SerializeToString(&serialized_entry);
                                                              store_->Put(a_28_dir, serialized_entry, 0);
                                                            }
                                                            BTNode *a_29_level = NULL; // 这个就指向该层第一个节点
                                                            for (int i_29 = 0; i_29 < a_29; i_29++)
                                                            {
                                                              string a_29_dir(a_28_dir + "/c_8" + IntToString(i_29));
                                                              string a_29_dir_("c_8" + IntToString(i_29));
                                                              BTNode *temp_a = new BTNode;
                                                              temp_a->child = NULL;
                                                              temp_a->path = a_29_dir_;
                                                              temp_a->sibling = NULL;
                                                              if (i_29 == 0)
                                                              {
                                                                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                                                a_28_level->child = temp_a;
                                                                a_29_level = temp_a; // a_level指针作为上一个兄弟节点
                                                              }
                                                              else
                                                              {
                                                                // 如果不是第一个节点，就是上一个节点的兄弟节点
                                                                a_29_level->sibling = temp_a;
                                                                a_29_level = a_29_level->sibling; // a_level移动到下一个兄弟节点
                                                              }

                                                              //
                                                              string a29_uid = IntToString(uid_a29++); // 第五层
                                                              if (IsLocal(a_29_dir))
                                                              {
                                                                MetadataEntry entry;
                                                                entry.mutable_permissions();
                                                                entry.set_type(DATA);
                                                                entry.add_dir_contents(a29_uid); // 这个用不上，因为分层开始
                                                                FilePart *fp = entry.add_file_parts();
                                                                fp->set_length(RandomSize());
                                                                fp->set_block_id(0);
                                                                fp->set_block_offset(0);
                                                                string serialized_entry;
                                                                entry.SerializeToString(&serialized_entry);
                                                                store_->Put(a_29_dir, serialized_entry, 0);
                                                              }
                                                              //
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::Init() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

// 下面这个函数是20层的增加了uid的只有树的初始化，用于LS深度实验
void MetadataStore::Init_tree_20(BTNode *dir_tree)
{
  // gaoxuan --这里面会涉及到目录树的建立初始化。
  int a_0 = machine_->config().size();
  int a_1 = 2;
  int a_2 = 2;
  int a_3 = 2;
  int a_4 = 2;
  int a_5 = 2;
  int a_6 = 2;
  int a_7 = 2;
  int a_8 = 2;
  int a_9 = 2;
  int a_10 = 2;
  int a_11 = 2;
  int a_12 = 2;
  int a_13 = 2;
  int a_14 = 2;
  int a_15 = 2;
  int a_16 = 2;
  int a_17 = 2;
  int a_18 = 2;
  int a_19 = 2;
  // 下面10层是hash
  int a_20 = 2;
  int a_21 = 2;
  int a_22 = 2;
  int a_23 = 2;
  int a_24 = 2;
  int a_25 = 2;
  int a_26 = 2;
  int a_27 = 2;
  int a_28 = 2;
  int a_29 = 2;

  // 改成5,5测试的时候容易看出来
  // 一种很蠢的方式得到uid
  int a_00 = a_0;
  int a_01 = a_00 * a_1;
  int a_02 = a_01 * a_2;
  int a_03 = a_02 * a_3;
  int a_04 = a_03 * a_4;
  int a_05 = a_04 * a_5;
  int a_06 = a_05 * a_6;
  int a_07 = a_06 * a_7;
  int a_08 = a_07 * a_8;
  int a_09 = a_08 * a_9;
  int a_010 = a_09 * a_10;
  int a_011 = a_010 * a_11;
  int a_012 = a_011 * a_12;
  int a_013 = a_012 * a_13;
  int a_014 = a_013 * a_14;
  int a_015 = a_014 * a_15;
  int a_016 = a_015 * a_16;
  int a_017 = a_016 * a_17;
  int a_018 = a_017 * a_18;
  int a_019 = a_018 * a_19;
  // 下面10层是hash
  int a_020 = a_019 * a_20;
  int a_021 = a_020 * a_21;
  int a_022 = a_021 * a_22;
  int a_023 = a_022 * a_23;
  int a_024 = a_023 * a_24;
  int a_025 = a_024 * a_25;
  int a_026 = a_025 * a_26;
  int a_027 = a_026 * a_27;
  int a_028 = a_027 * a_28;
  int a_029 = a_028 * a_29;

  // 一种很蠢的方式
  int uid_a0 = 1;
  int uid_a1 = a_00 + uid_a0;
  int uid_a2 = a_01 + uid_a1;
  int uid_a3 = a_02 + uid_a2;
  int uid_a4 = a_03 + uid_a3;
  int uid_a5 = a_04 + uid_a4;
  int uid_a6 = a_05 + uid_a5;
  int uid_a7 = a_06 + uid_a6;
  int uid_a8 = a_07 + uid_a7;
  int uid_a9 = a_08 + uid_a8;
  int uid_a10 = a_09 + uid_a9;
  int uid_a11 = a_010 + uid_a10;
  int uid_a12 = a_011 + uid_a11;
  int uid_a13 = a_012 + uid_a12;
  int uid_a14 = a_013 + uid_a13;
  int uid_a15 = a_014 + uid_a14;
  int uid_a16 = a_015 + uid_a15;
  int uid_a17 = a_016 + uid_a16;
  int uid_a18 = a_017 + uid_a17;
  int uid_a19 = a_018 + uid_a18;

  // 下面是分层点的十层
  int uid_a20 = a_019 + uid_a19;
  int uid_a21 = a_020 + uid_a20;
  int uid_a22 = a_021 + uid_a21;
  int uid_a23 = a_022 + uid_a22;
  int uid_a24 = a_023 + uid_a23;
  int uid_a25 = a_024 + uid_a24;
  int uid_a26 = a_025 + uid_a25;
  int uid_a27 = a_026 + uid_a26;
  int uid_a28 = a_027 + uid_a27;
  int uid_a29 = a_028 + uid_a28;
  //

  double start = GetTime();
  // gaoxuan --根节点的指针
  dir_tree->child = NULL;
  dir_tree->sibling = NULL;
  dir_tree->path = "";
  string root_uid = "0";
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    entry.add_dir_contents(root_uid);
    for (int i_0 = 0; i_0 < a_0; i_0++)
    {
      entry.add_dir_contents("a_0" + IntToString(i_0));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }
  BTNode *a_0_level = NULL; // 这个就指向该层第一个节点
  for (int i_0 = 0; i_0 < a_0; i_0++)
  {
    string a_0_dir("/" + root_uid + "a_0" + IntToString(i_0));
    string a_0_dir_("a_0" + IntToString(i_0));
    BTNode *temp_a = new BTNode;
    temp_a->child = NULL;
    temp_a->path = a_0_dir_;
    temp_a->sibling = NULL;
    if (i_0 == 0)
    {
      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
      dir_tree->child = temp_a;
      a_0_level = temp_a; // a_level指针作为上一个兄弟节点
    }
    else
    {
      // 如果不是第一个节点，就是上一个节点的兄弟节点
      a_0_level->sibling = temp_a;
      a_0_level = a_0_level->sibling; // a_level移动到下一个兄弟节点
    }
    string a0_uid = IntToString(uid_a0++); // 第一层的uid，
    if (IsLocal(a_0_dir))
    {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      entry.add_dir_contents(a0_uid);
      for (int i_1 = 0; i_1 < a_1; i_1++)
      {
        entry.add_dir_contents("a_1" + IntToString(i_1));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(a_0_dir, serialized_entry, 0);
    }
    BTNode *a_1_level = NULL; // 这个就指向该层第一个节点

    for (int i_1 = 0; i_1 < a_1; i_1++)
    {
      string a_1_dir("/" + a0_uid + "a_1" + IntToString(i_1));
      string a_1_dir_("a_1" + IntToString(i_1));
      BTNode *temp_a = new BTNode;
      temp_a->child = NULL;
      temp_a->path = a_1_dir_;
      temp_a->sibling = NULL;
      if (i_1 == 0)
      {
        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
        a_0_level->child = temp_a;
        a_1_level = temp_a; // a_level指针作为上一个兄弟节点
      }
      else
      {
        // 如果不是第一个节点，就是上一个节点的兄弟节点
        a_1_level->sibling = temp_a;
        a_1_level = a_1_level->sibling; // a_level移动到下一个兄弟节点
      }
      string a1_uid = IntToString(uid_a1++); // 第二层的uid,这个也没错
      if (IsLocal(a_1_dir))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        entry.add_dir_contents(a1_uid);
        for (int i_2 = 0; i_2 < a_2; i_2++)
        {
          entry.add_dir_contents("a_2" + IntToString(i_2));
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(a_1_dir, serialized_entry, 0);
      }
      BTNode *a_2_level = NULL; // 这个就指向该层第一个节点
      for (int i_2 = 0; i_2 < a_2; i_2++)
      {
        string a_2_dir("/" + a1_uid + "a_2" + IntToString(i_2));
        string a_2_dir_("a_2" + IntToString(i_2));
        BTNode *temp_a = new BTNode;
        temp_a->child = NULL;
        temp_a->path = a_2_dir_;
        temp_a->sibling = NULL;
        if (i_2 == 0)
        {
          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
          a_1_level->child = temp_a;
          a_2_level = temp_a; // a_level指针作为上一个兄弟节点
        }
        else
        {
          // 如果不是第一个节点，就是上一个节点的兄弟节点
          a_2_level->sibling = temp_a;
          a_2_level = a_2_level->sibling; // a_level移动到下一个兄弟节点
        }
        string a2_uid = IntToString(uid_a2++); // 第三层的uid
        if (IsLocal(a_2_dir))
        {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DIR);
          entry.add_dir_contents(a2_uid);
          for (int i_3 = 0; i_3 < a_3; i_3++)
          {
            entry.add_dir_contents("a_3" + IntToString(i_3));
          }
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(a_2_dir, serialized_entry, 0);
        }
        BTNode *a_3_level = NULL; // 这个就指向该层第一个节点
        for (int i_3 = 0; i_3 < a_3; i_3++)
        {
          string a_3_dir("/" + a2_uid + "a_3" + IntToString(i_3));
          string a_3_dir_("a_3" + IntToString(i_3));
          BTNode *temp_a = new BTNode;
          temp_a->child = NULL;
          temp_a->path = a_3_dir_;
          temp_a->sibling = NULL;
          if (i_3 == 0)
          {
            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
            a_2_level->child = temp_a;
            a_3_level = temp_a; // a_level指针作为上一个兄弟节点
          }
          else
          {
            // 如果不是第一个节点，就是上一个节点的兄弟节点
            a_3_level->sibling = temp_a;
            a_3_level = a_3_level->sibling; // a_level移动到下一个兄弟节点
          }

          string a3_uid = IntToString(uid_a3++); // 第四层
          if (IsLocal(a_3_dir))
          {
            MetadataEntry entry;
            entry.mutable_permissions();
            entry.set_type(DIR);
            entry.add_dir_contents(a3_uid);
            for (int i_4 = 0; i_4 < a_4; i_4++)
            {
              entry.add_dir_contents("a_4" + IntToString(i_4));
            }
            string serialized_entry;
            entry.SerializeToString(&serialized_entry);
            store_->Put(a_3_dir, serialized_entry, 0);
          }
          BTNode *a_4_level = NULL; // 这个就指向该层第一个节点
          for (int i_4 = 0; i_4 < a_4; i_4++)
          {
            string a_4_dir("/" + a3_uid + "a_4" + IntToString(i_4));
            string a_4_dir_("a_4" + IntToString(i_4));
            BTNode *temp_a = new BTNode;
            temp_a->child = NULL;
            temp_a->path = a_4_dir_;
            temp_a->sibling = NULL;
            if (i_4 == 0)
            {
              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
              a_3_level->child = temp_a;
              a_4_level = temp_a; // a_level指针作为上一个兄弟节点
            }
            else
            {
              // 如果不是第一个节点，就是上一个节点的兄弟节点
              a_4_level->sibling = temp_a;
              a_4_level = a_4_level->sibling; // a_level移动到下一个兄弟节点
            }
            string a4_uid = IntToString(uid_a4++); // 第四层
            if (IsLocal(a_4_dir))
            {
              MetadataEntry entry;
              entry.mutable_permissions();
              entry.set_type(DIR);
              entry.add_dir_contents(a4_uid);
              for (int i_5 = 0; i_5 < a_5; i_5++)
              {
                entry.add_dir_contents("a_5" + IntToString(i_5));
              }
              string serialized_entry;
              entry.SerializeToString(&serialized_entry);
              store_->Put(a_4_dir, serialized_entry, 0);
            }
            BTNode *a_5_level = NULL; // 这个就指向该层第一个节点
            for (int i_5 = 0; i_5 < a_5; i_5++)
            {
              string a_5_dir("/" + a4_uid + "a_5" + IntToString(i_5));
              string a_5_dir_("a_5" + IntToString(i_5));
              BTNode *temp_a = new BTNode;
              temp_a->child = NULL;
              temp_a->path = a_5_dir_;
              temp_a->sibling = NULL;
              if (i_5 == 0)
              {
                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                a_4_level->child = temp_a;
                a_5_level = temp_a; // a_level指针作为上一个兄弟节点
              }
              else
              {
                // 如果不是第一个节点，就是上一个节点的兄弟节点
                a_5_level->sibling = temp_a;
                a_5_level = a_5_level->sibling; // a_level移动到下一个兄弟节点
              }
              string a5_uid = IntToString(uid_a5++); // 第四层
              if (IsLocal(a_5_dir))
              {
                MetadataEntry entry;
                entry.mutable_permissions();
                entry.set_type(DIR);
                entry.add_dir_contents(a5_uid);
                for (int i_6 = 0; i_6 < a_6; i_6++)
                {
                  entry.add_dir_contents("a_6" + IntToString(i_6));
                }
                string serialized_entry;
                entry.SerializeToString(&serialized_entry);
                store_->Put(a_5_dir, serialized_entry, 0);
              }
              BTNode *a_6_level = NULL; // 这个就指向该层第一个节点
              for (int i_6 = 0; i_6 < a_6; i_6++)
              {
                string a_6_dir("/" + a5_uid + "a_6" + IntToString(i_6));
                string a_6_dir_("a_6" + IntToString(i_6));
                BTNode *temp_a = new BTNode;
                temp_a->child = NULL;
                temp_a->path = a_6_dir_;
                temp_a->sibling = NULL;
                if (i_6 == 0)
                {
                  // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                  a_5_level->child = temp_a;
                  a_6_level = temp_a; // a_level指针作为上一个兄弟节点
                }
                else
                {
                  // 如果不是第一个节点，就是上一个节点的兄弟节点
                  a_6_level->sibling = temp_a;
                  a_6_level = a_6_level->sibling; // a_level移动到下一个兄弟节点
                }
                string a6_uid = IntToString(uid_a6++); // 第四层
                if (IsLocal(a_6_dir))
                {
                  MetadataEntry entry;
                  entry.mutable_permissions();
                  entry.set_type(DIR);
                  entry.add_dir_contents(a6_uid);
                  for (int i_7 = 0; i_7 < a_7; i_7++)
                  {
                    entry.add_dir_contents("a_7" + IntToString(i_7));
                  }
                  string serialized_entry;
                  entry.SerializeToString(&serialized_entry);
                  store_->Put(a_6_dir, serialized_entry, 0);
                }
                BTNode *a_7_level = NULL; // 这个就指向该层第一个节点
                for (int i_7 = 0; i_7 < a_7; i_7++)
                {
                  string a_7_dir("/" + a6_uid + "a_7" + IntToString(i_7));
                  string a_7_dir_("a_7" + IntToString(i_7));
                  BTNode *temp_a = new BTNode;
                  temp_a->child = NULL;
                  temp_a->path = a_7_dir_;
                  temp_a->sibling = NULL;
                  if (i_7 == 0)
                  {
                    // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                    a_6_level->child = temp_a;
                    a_7_level = temp_a; // a_level指针作为上一个兄弟节点
                  }
                  else
                  {
                    // 如果不是第一个节点，就是上一个节点的兄弟节点
                    a_7_level->sibling = temp_a;
                    a_7_level = a_7_level->sibling; // a_level移动到下一个兄弟节点
                  }
                  string a7_uid = IntToString(uid_a7++); // 第四层
                  if (IsLocal(a_7_dir))
                  {
                    MetadataEntry entry;
                    entry.mutable_permissions();
                    entry.set_type(DIR);
                    entry.add_dir_contents(a7_uid);
                    for (int i_8 = 0; i_8 < a_8; i_8++)
                    {
                      entry.add_dir_contents("a_8" + IntToString(i_8));
                    }
                    string serialized_entry;
                    entry.SerializeToString(&serialized_entry);
                    store_->Put(a_7_dir, serialized_entry, 0);
                  }
                  BTNode *a_8_level = NULL; // 这个就指向该层第一个节点
                  for (int i_8 = 0; i_8 < a_8; i_8++)
                  {
                    string a_8_dir("/" + a7_uid + "a_8" + IntToString(i_8));
                    string a_8_dir_("a_8" + IntToString(i_8));
                    BTNode *temp_a = new BTNode;
                    temp_a->child = NULL;
                    temp_a->path = a_8_dir_;
                    temp_a->sibling = NULL;
                    if (i_8 == 0)
                    {
                      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                      a_7_level->child = temp_a;
                      a_8_level = temp_a; // a_level指针作为上一个兄弟节点
                    }
                    else
                    {
                      // 如果不是第一个节点，就是上一个节点的兄弟节点
                      a_8_level->sibling = temp_a;
                      a_8_level = a_8_level->sibling; // a_level移动到下一个兄弟节点
                    }
                    string a8_uid = IntToString(uid_a8++); // 第四层
                    if (IsLocal(a_8_dir))
                    {
                      MetadataEntry entry;
                      entry.mutable_permissions();
                      entry.set_type(DIR);
                      entry.add_dir_contents(a8_uid);
                      for (int i_9 = 0; i_9 < a_9; i_9++)
                      {
                        entry.add_dir_contents("a_9" + IntToString(i_9));
                      }
                      string serialized_entry;
                      entry.SerializeToString(&serialized_entry);
                      store_->Put(a_8_dir, serialized_entry, 0);
                    }
                    BTNode *a_9_level = NULL; // 这个就指向该层第一个节点
                    for (int i_9 = 0; i_9 < a_9; i_9++)
                    {
                      string a_9_dir("/" + a8_uid + "a_9" + IntToString(i_9));
                      string a_9_dir_("a_9" + IntToString(i_9));
                      BTNode *temp_a = new BTNode;
                      temp_a->child = NULL;
                      temp_a->path = a_9_dir_;
                      temp_a->sibling = NULL;
                      if (i_9 == 0)
                      {
                        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                        a_8_level->child = temp_a;
                        a_9_level = temp_a; // a_level指针作为上一个兄弟节点
                      }
                      else
                      {
                        // 如果不是第一个节点，就是上一个节点的兄弟节点
                        a_9_level->sibling = temp_a;
                        a_9_level = a_9_level->sibling; // a_level移动到下一个兄弟节点
                      }
                      string a9_uid = IntToString(uid_a9++); // 第四层
                      if (IsLocal(a_9_dir))
                      {
                        MetadataEntry entry;
                        entry.mutable_permissions();
                        entry.set_type(DIR);
                        entry.add_dir_contents(a9_uid);
                        for (int i_10 = 0; i_10 < a_10; i_10++)
                        {
                          entry.add_dir_contents("a_10" + IntToString(i_10));
                        }
                        string serialized_entry;
                        entry.SerializeToString(&serialized_entry);
                        store_->Put(a_9_dir, serialized_entry, 0);
                      }
                      BTNode *a_10_level = NULL; // 这个就指向该层第一个节点
                      for (int i_10 = 0; i_10 < a_10; i_10++)
                      {
                        string a_10_dir("/" + a9_uid + "a_10" + IntToString(i_10));
                        string a_10_dir_("a_10" + IntToString(i_10));
                        BTNode *temp_a = new BTNode;
                        temp_a->child = NULL;
                        temp_a->path = a_10_dir_;
                        temp_a->sibling = NULL;
                        if (i_10 == 0)
                        {
                          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                          a_9_level->child = temp_a;
                          a_10_level = temp_a; // a_level指针作为上一个兄弟节点
                        }
                        else
                        {
                          // 如果不是第一个节点，就是上一个节点的兄弟节点
                          a_10_level->sibling = temp_a;
                          a_10_level = a_10_level->sibling; // a_level移动到下一个兄弟节点
                        }
                        string a10_uid = IntToString(uid_a10++); // 第四层
                        if (IsLocal(a_10_dir))
                        {
                          MetadataEntry entry;
                          entry.mutable_permissions();
                          entry.set_type(DIR);
                          entry.add_dir_contents(a10_uid);
                          for (int i_11 = 0; i_11 < a_11; i_11++)
                          {
                            entry.add_dir_contents("a_11" + IntToString(i_11));
                          }
                          string serialized_entry;
                          entry.SerializeToString(&serialized_entry);
                          store_->Put(a_10_dir, serialized_entry, 0);
                        }
                        BTNode *a_11_level = NULL; // 这个就指向该层第一个节点
                        for (int i_11 = 0; i_11 < a_11; i_11++)
                        {
                          string a_11_dir("/" + a10_uid + "a_11" + IntToString(i_11));
                          string a_11_dir_("a_11" + IntToString(i_11));
                          BTNode *temp_a = new BTNode;
                          temp_a->child = NULL;
                          temp_a->path = a_11_dir_;
                          temp_a->sibling = NULL;
                          if (i_11 == 0)
                          {
                            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                            a_10_level->child = temp_a;
                            a_11_level = temp_a; // a_level指针作为上一个兄弟节点
                          }
                          else
                          {
                            // 如果不是第一个节点，就是上一个节点的兄弟节点
                            a_11_level->sibling = temp_a;
                            a_11_level = a_11_level->sibling; // a_level移动到下一个兄弟节点
                          }
                          string a11_uid = IntToString(uid_a11++); // 第四层
                          if (IsLocal(a_11_dir))
                          {
                            MetadataEntry entry;
                            entry.mutable_permissions();
                            entry.set_type(DIR);
                            entry.add_dir_contents(a11_uid);
                            for (int i_12 = 0; i_12 < a_12; i_12++)
                            {
                              entry.add_dir_contents("a_12" + IntToString(i_12));
                            }
                            string serialized_entry;
                            entry.SerializeToString(&serialized_entry);
                            store_->Put(a_11_dir, serialized_entry, 0);
                          }
                          BTNode *a_12_level = NULL; // 这个就指向该层第一个节点
                          for (int i_12 = 0; i_12 < a_12; i_12++)
                          {
                            string a_12_dir("/" + a11_uid + "a_12" + IntToString(i_12));
                            string a_12_dir_("a_12" + IntToString(i_12));
                            BTNode *temp_a = new BTNode;
                            temp_a->child = NULL;
                            temp_a->path = a_12_dir_;
                            temp_a->sibling = NULL;
                            if (i_12 == 0)
                            {
                              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                              a_11_level->child = temp_a;
                              a_12_level = temp_a; // a_level指针作为上一个兄弟节点
                            }
                            else
                            {
                              // 如果不是第一个节点，就是上一个节点的兄弟节点
                              a_12_level->sibling = temp_a;
                              a_12_level = a_12_level->sibling; // a_level移动到下一个兄弟节点
                            }
                            string a12_uid = IntToString(uid_a12++); // 第四层
                            if (IsLocal(a_12_dir))
                            {
                              MetadataEntry entry;
                              entry.mutable_permissions();
                              entry.set_type(DIR);
                              entry.add_dir_contents(a12_uid);
                              for (int i_13 = 0; i_13 < a_13; i_13++)
                              {
                                entry.add_dir_contents("a_13" + IntToString(i_13));
                              }
                              string serialized_entry;
                              entry.SerializeToString(&serialized_entry);
                              store_->Put(a_12_dir, serialized_entry, 0);
                            }
                            BTNode *a_13_level = NULL; // 这个就指向该层第一个节点
                            for (int i_13 = 0; i_13 < a_13; i_13++)
                            {
                              string a_13_dir("/" + a12_uid + "a_13" + IntToString(i_13));
                              string a_13_dir_("a_13" + IntToString(i_13));
                              BTNode *temp_a = new BTNode;
                              temp_a->child = NULL;
                              temp_a->path = a_13_dir_;
                              temp_a->sibling = NULL;
                              if (i_13 == 0)
                              {
                                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                a_12_level->child = temp_a;
                                a_13_level = temp_a; // a_level指针作为上一个兄弟节点
                              }
                              else
                              {
                                // 如果不是第一个节点，就是上一个节点的兄弟节点
                                a_13_level->sibling = temp_a;
                                a_13_level = a_13_level->sibling; // a_level移动到下一个兄弟节点
                              }
                              string a13_uid = IntToString(uid_a13++); // 第四层
                              if (IsLocal(a_13_dir))
                              {
                                MetadataEntry entry;
                                entry.mutable_permissions();
                                entry.set_type(DIR);
                                entry.add_dir_contents(a13_uid);
                                for (int i_14 = 0; i_14 < a_14; i_14++)
                                {
                                  entry.add_dir_contents("a_14" + IntToString(i_14));
                                }
                                string serialized_entry;
                                entry.SerializeToString(&serialized_entry);
                                store_->Put(a_13_dir, serialized_entry, 0);
                              }
                              BTNode *a_14_level = NULL; // 这个就指向该层第一个节点
                              for (int i_14 = 0; i_14 < a_14; i_14++)
                              {
                                string a_14_dir("/" + a13_uid + "a_14" + IntToString(i_14));
                                string a_14_dir_("a_14" + IntToString(i_14));
                                BTNode *temp_a = new BTNode;
                                temp_a->child = NULL;
                                temp_a->path = a_14_dir_;
                                temp_a->sibling = NULL;
                                if (i_14 == 0)
                                {
                                  // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                  a_13_level->child = temp_a;
                                  a_14_level = temp_a; // a_level指针作为上一个兄弟节点
                                }
                                else
                                {
                                  // 如果不是第一个节点，就是上一个节点的兄弟节点
                                  a_14_level->sibling = temp_a;
                                  a_14_level = a_14_level->sibling; // a_level移动到下一个兄弟节点
                                }
                                string a14_uid = IntToString(uid_a14++); // 第四层
                                if (IsLocal(a_14_dir))
                                {
                                  MetadataEntry entry;
                                  entry.mutable_permissions();
                                  entry.set_type(DIR);
                                  entry.add_dir_contents(a14_uid);
                                  for (int i_15 = 0; i_15 < a_15; i_15++)
                                  {
                                    entry.add_dir_contents("a_15" + IntToString(i_15));
                                  }
                                  string serialized_entry;
                                  entry.SerializeToString(&serialized_entry);
                                  store_->Put(a_14_dir, serialized_entry, 0);
                                }
                                BTNode *a_15_level = NULL; // 这个就指向该层第一个节点
                                for (int i_15 = 0; i_15 < a_15; i_15++)
                                {
                                  string a_15_dir("/" + a14_uid + "a_15" + IntToString(i_15));
                                  string a_15_dir_("a_15" + IntToString(i_15));
                                  BTNode *temp_a = new BTNode;
                                  temp_a->child = NULL;
                                  temp_a->path = a_15_dir_;
                                  temp_a->sibling = NULL;
                                  if (i_15 == 0)
                                  {
                                    // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                    a_14_level->child = temp_a;
                                    a_15_level = temp_a; // a_level指针作为上一个兄弟节点
                                  }
                                  else
                                  {
                                    // 如果不是第一个节点，就是上一个节点的兄弟节点
                                    a_15_level->sibling = temp_a;
                                    a_15_level = a_15_level->sibling; // a_level移动到下一个兄弟节点
                                  }
                                  string a15_uid = IntToString(uid_a15++); // 第四层
                                  if (IsLocal(a_15_dir))
                                  {
                                    MetadataEntry entry;
                                    entry.mutable_permissions();
                                    entry.set_type(DIR);
                                    entry.add_dir_contents(a15_uid);
                                    for (int i_16 = 0; i_16 < a_16; i_16++)
                                    {
                                      entry.add_dir_contents("a_16" + IntToString(i_16));
                                    }
                                    string serialized_entry;
                                    entry.SerializeToString(&serialized_entry);
                                    store_->Put(a_15_dir, serialized_entry, 0);
                                  }
                                  BTNode *a_16_level = NULL; // 这个就指向该层第一个节点
                                  for (int i_16 = 0; i_16 < a_16; i_16++)
                                  {
                                    string a_16_dir("/" + a15_uid + "a_16" + IntToString(i_16));
                                    string a_16_dir_("a_16" + IntToString(i_16));
                                    BTNode *temp_a = new BTNode;
                                    temp_a->child = NULL;
                                    temp_a->path = a_16_dir_;
                                    temp_a->sibling = NULL;
                                    if (i_16 == 0)
                                    {
                                      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                      a_15_level->child = temp_a;
                                      a_16_level = temp_a; // a_level指针作为上一个兄弟节点
                                    }
                                    else
                                    {
                                      // 如果不是第一个节点，就是上一个节点的兄弟节点
                                      a_16_level->sibling = temp_a;
                                      a_16_level = a_16_level->sibling; // a_level移动到下一个兄弟节点
                                    }
                                    string a16_uid = IntToString(uid_a16++); // 第四层
                                    if (IsLocal(a_16_dir))
                                    {
                                      MetadataEntry entry;
                                      entry.mutable_permissions();
                                      entry.set_type(DIR);
                                      entry.add_dir_contents(a16_uid);
                                      for (int i_17 = 0; i_17 < a_17; i_17++)
                                      {
                                        entry.add_dir_contents("a_17" + IntToString(i_17));
                                      }
                                      string serialized_entry;
                                      entry.SerializeToString(&serialized_entry);
                                      store_->Put(a_16_dir, serialized_entry, 0);
                                    }
                                    BTNode *a_17_level = NULL; // 这个就指向该层第一个节点
                                    for (int i_17 = 0; i_17 < a_17; i_17++)
                                    {
                                      string a_17_dir("/" + a16_uid + "a_17" + IntToString(i_17));
                                      string a_17_dir_("a_17" + IntToString(i_17));
                                      BTNode *temp_a = new BTNode;
                                      temp_a->child = NULL;
                                      temp_a->path = a_17_dir_;
                                      temp_a->sibling = NULL;
                                      if (i_17 == 0)
                                      {
                                        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                        a_16_level->child = temp_a;
                                        a_17_level = temp_a; // a_level指针作为上一个兄弟节点
                                      }
                                      else
                                      {
                                        // 如果不是第一个节点，就是上一个节点的兄弟节点
                                        a_17_level->sibling = temp_a;
                                        a_17_level = a_17_level->sibling; // a_level移动到下一个兄弟节点
                                      }
                                      string a17_uid = IntToString(uid_a17++); // 第四层
                                      if (IsLocal(a_17_dir))
                                      {
                                        MetadataEntry entry;
                                        entry.mutable_permissions();
                                        entry.set_type(DIR);
                                        entry.add_dir_contents(a17_uid);
                                        for (int i_18 = 0; i_18 < a_18; i_18++)
                                        {
                                          entry.add_dir_contents("a_18" + IntToString(i_18));
                                        }
                                        string serialized_entry;
                                        entry.SerializeToString(&serialized_entry);
                                        store_->Put(a_17_dir, serialized_entry, 0);
                                      }
                                      BTNode *a_18_level = NULL; // 这个就指向该层第一个节点
                                      for (int i_18 = 0; i_18 < a_18; i_18++)
                                      {
                                        string a_18_dir("/" + a17_uid + "a_18" + IntToString(i_18));
                                        string a_18_dir_("a_18" + IntToString(i_18));
                                        BTNode *temp_a = new BTNode;
                                        temp_a->child = NULL;
                                        temp_a->path = a_18_dir_;
                                        temp_a->sibling = NULL;
                                        if (i_18 == 0)
                                        {
                                          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                          a_17_level->child = temp_a;
                                          a_18_level = temp_a; // a_level指针作为上一个兄弟节点
                                        }
                                        else
                                        {
                                          // 如果不是第一个节点，就是上一个节点的兄弟节点
                                          a_18_level->sibling = temp_a;
                                          a_18_level = a_18_level->sibling; // a_level移动到下一个兄弟节点
                                        }
                                        string a18_uid = IntToString(uid_a18++); // 第四层
                                        if (IsLocal(a_18_dir))
                                        {
                                          MetadataEntry entry;
                                          entry.mutable_permissions();
                                          entry.set_type(DIR);
                                          entry.add_dir_contents(a18_uid);
                                          for (int i_19 = 0; i_19 < a_19; i_19++)
                                          {
                                            entry.add_dir_contents("a_19" + IntToString(i_19));
                                          }
                                          string serialized_entry;
                                          entry.SerializeToString(&serialized_entry);
                                          store_->Put(a_18_dir, serialized_entry, 0);
                                        }
                                        BTNode *a_19_level = NULL; // 这个就指向该层第一个节点
                                        for (int i_19 = 0; i_19 < a_19; i_19++)
                                        {
                                          string a_19_dir("/" + a18_uid + "a_19" + IntToString(i_19));
                                          string a_19_dir_("a_19" + IntToString(i_19));
                                          BTNode *temp_a = new BTNode;
                                          temp_a->child = NULL;
                                          temp_a->path = a_19_dir_;
                                          temp_a->sibling = NULL;
                                          if (i_19 == 0)
                                          {
                                            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                                            a_18_level->child = temp_a;
                                            a_19_level = temp_a; // a_level指针作为上一个兄弟节点
                                          }
                                          else
                                          {
                                            // 如果不是第一个节点，就是上一个节点的兄弟节点
                                            a_19_level->sibling = temp_a;
                                            a_19_level = a_19_level->sibling; // a_level移动到下一个兄弟节点
                                          }
                                          string a19_uid = IntToString(uid_a19++); // 第四层
                                          if (IsLocal(a_19_dir))
                                          {
                                            MetadataEntry entry;
                                            entry.mutable_permissions();
                                            entry.set_type(DATA);
                                            entry.add_dir_contents(a19_uid);
                                            FilePart *fp = entry.add_file_parts();
                                            fp->set_length(RandomSize());
                                            fp->set_block_id(0);
                                            fp->set_block_offset(0);
                                            string serialized_entry;
                                            entry.SerializeToString(&serialized_entry);
                                            store_->Put(a_19_dir, serialized_entry, 0);
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::Init() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

// 这个初始化函数是增加了分层的，肯定没有问题，上面六层是树，下面4层是hash
void MetadataStore::Init_for_10(BTNode *dir_tree)
{

  // gaoxuan --这里面会涉及到目录树的建立初始化。
  int a_0 = machine_->config().size();
  int a_1 = 2;
  int a_2 = 2;
  int a_3 = 2;
  int a_4 = 2;
  int a_5 = 2;
  int a_6 = 2;
  int a_7 = 2;
  int a_8 = 2;
  int a_9 = 2;

  int a_00 = a_0;
  int a_01 = a_00 * a_1;
  int a_02 = a_01 * a_2;
  int a_03 = a_02 * a_3;
  int a_04 = a_03 * a_4;
  int a_05 = a_04 * a_5;
  int a_06 = a_05 * a_6;
  int a_07 = a_06 * a_7;
  int a_08 = a_07 * a_8;

  // 一种很蠢的方式
  int uid_a0 = 1;
  int uid_a1 = a_00 + 1;
  int uid_a2 = a_00 + a_01 + 1;
  int uid_a3 = a_00 + a_01 + a_02 + 1;
  int uid_a4 = a_00 + a_01 + a_02 + a_03 + 1;
  int uid_a5 = a_00 + a_01 + a_02 + a_03 + a_04 + 1;
  int uid_a6 = a_00 + a_01 + a_02 + a_03 + a_04 + a_05 + 1;
  int uid_a7 = a_00 + a_01 + a_02 + a_03 + a_04 + a_05 + a_06 + 1;
  int uid_a8 = a_00 + a_01 + a_02 + a_03 + a_04 + a_05 + a_06 + a_07 + 1;
  int uid_a9 = a_00 + a_01 + a_02 + a_03 + a_04 + a_05 + a_06 + a_07 + a_08 + 1;
  //

  // 改成5,5测试的时候容易看出来
  double start = GetTime();
  // gaoxuan --根节点的指针
  dir_tree->child = NULL;
  dir_tree->sibling = NULL;
  dir_tree->path = "";
  string root_uid = "0";
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    entry.add_dir_contents(root_uid);
    for (int i_0 = 0; i_0 < a_0; i_0++)
    {
      entry.add_dir_contents("a_0" + IntToString(i_0));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }
  BTNode *a_0_level = NULL; // 这个就指向该层第一个节点
  for (int i_0 = 0; i_0 < a_0; i_0++)
  {
    string a_0_dir("/" + root_uid + "a_0" + IntToString(i_0));
    string a_0_dir_("a_0" + IntToString(i_0));
    BTNode *temp_a = new BTNode;
    temp_a->child = NULL;
    temp_a->path = a_0_dir_;
    temp_a->sibling = NULL;
    if (i_0 == 0)
    {
      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
      dir_tree->child = temp_a;
      a_0_level = temp_a; // a_level指针作为上一个兄弟节点
    }
    else
    {
      // 如果不是第一个节点，就是上一个节点的兄弟节点
      a_0_level->sibling = temp_a;
      a_0_level = a_0_level->sibling; // a_level移动到下一个兄弟节点
    }
    string a0_uid = IntToString(uid_a0++); // 第一层的uid，
    if (IsLocal(a_0_dir))
    {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      entry.add_dir_contents(a0_uid);
      for (int i_1 = 0; i_1 < a_1; i_1++)
      {
        entry.add_dir_contents("a_1" + IntToString(i_1));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(a_0_dir, serialized_entry, 0);
    }
    BTNode *a_1_level = NULL; // 这个就指向该层第一个节点

    for (int i_1 = 0; i_1 < a_1; i_1++)
    {
      string a_1_dir("/" + a0_uid + "a_1" + IntToString(i_1));
      string a_1_dir_("a_1" + IntToString(i_1));
      BTNode *temp_a = new BTNode;
      temp_a->child = NULL;
      temp_a->path = a_1_dir_;
      temp_a->sibling = NULL;
      if (i_1 == 0)
      {
        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
        a_0_level->child = temp_a;
        a_1_level = temp_a; // a_level指针作为上一个兄弟节点
      }
      else
      {
        // 如果不是第一个节点，就是上一个节点的兄弟节点
        a_1_level->sibling = temp_a;
        a_1_level = a_1_level->sibling; // a_level移动到下一个兄弟节点
      }

      string a1_uid = IntToString(uid_a1++); // 第二层的uid,这个也没错
      if (IsLocal(a_1_dir))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        entry.add_dir_contents(a1_uid);
        for (int i_2 = 0; i_2 < a_2; i_2++)
        {
          entry.add_dir_contents("a_2" + IntToString(i_2));
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(a_1_dir, serialized_entry, 0);
      }
      BTNode *a_2_level = NULL; // 这个就指向该层第一个节点
      for (int i_2 = 0; i_2 < a_2; i_2++)
      {
        string a_2_dir("/" + a1_uid + "a_2" + IntToString(i_2));
        string a_2_dir_("a_2" + IntToString(i_2));
        BTNode *temp_a = new BTNode;
        temp_a->child = NULL;
        temp_a->path = a_2_dir_;
        temp_a->sibling = NULL;
        if (i_2 == 0)
        {
          // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
          a_1_level->child = temp_a;
          a_2_level = temp_a; // a_level指针作为上一个兄弟节点
        }
        else
        {
          // 如果不是第一个节点，就是上一个节点的兄弟节点
          a_2_level->sibling = temp_a;
          a_2_level = a_2_level->sibling; // a_level移动到下一个兄弟节点
        }

        // 从这层开始不对劲
        string a2_uid = IntToString(uid_a2++); // 第三层的uid
        if (IsLocal(a_2_dir))
        {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DIR);
          entry.add_dir_contents(a2_uid);
          for (int i_3 = 0; i_3 < a_3; i_3++)
          {
            entry.add_dir_contents("a_3" + IntToString(i_3));
          }
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(a_2_dir, serialized_entry, 0);
        }
        BTNode *a_3_level = NULL; // 这个就指向该层第一个节点
        for (int i_3 = 0; i_3 < a_3; i_3++)
        {
          string a_3_dir("/" + a2_uid + "a_3" + IntToString(i_3));
          string a_3_dir_("a_3" + IntToString(i_3));
          BTNode *temp_a = new BTNode;
          temp_a->child = NULL;
          temp_a->path = a_3_dir_;
          temp_a->sibling = NULL;
          if (i_3 == 0)
          {
            // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
            a_2_level->child = temp_a;
            a_3_level = temp_a; // a_level指针作为上一个兄弟节点
          }
          else
          {
            // 如果不是第一个节点，就是上一个节点的兄弟节点
            a_3_level->sibling = temp_a;
            a_3_level = a_3_level->sibling; // a_level移动到下一个兄弟节点
          }
          string a3_uid = IntToString(uid_a3++); // 第四层
          if (IsLocal(a_3_dir))
          {
            MetadataEntry entry;
            entry.mutable_permissions();
            entry.set_type(DIR);
            entry.add_dir_contents(a3_uid);
            for (int i_4 = 0; i_4 < a_4; i_4++)
            {
              entry.add_dir_contents("a_4" + IntToString(i_4));
            }
            string serialized_entry;
            entry.SerializeToString(&serialized_entry);
            store_->Put(a_3_dir, serialized_entry, 0);
          }
          BTNode *a_4_level = NULL; // 这个就指向该层第一个节点
          for (int i_4 = 0; i_4 < a_4; i_4++)
          {
            string a_4_dir("/" + a3_uid + "a_4" + IntToString(i_4));
            string a_4_dir_("a_4" + IntToString(i_4));
            BTNode *temp_a = new BTNode;
            temp_a->child = NULL;
            temp_a->path = a_4_dir_;
            temp_a->sibling = NULL;
            if (i_4 == 0)
            {
              // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
              a_3_level->child = temp_a;
              a_4_level = temp_a; // a_level指针作为上一个兄弟节点
            }
            else
            {
              // 如果不是第一个节点，就是上一个节点的兄弟节点
              a_4_level->sibling = temp_a;
              a_4_level = a_4_level->sibling; // a_level移动到下一个兄弟节点
            }
            string a4_uid = IntToString(uid_a4++); // 第四层
            if (IsLocal(a_4_dir))
            {
              MetadataEntry entry;
              entry.mutable_permissions();
              entry.set_type(DIR);
              entry.add_dir_contents(a4_uid);
              for (int i_5 = 0; i_5 < a_5; i_5++)
              {
                entry.add_dir_contents("a_5" + IntToString(i_5));
              }
              string serialized_entry;
              entry.SerializeToString(&serialized_entry);
              store_->Put(a_4_dir, serialized_entry, 0);
            }
            BTNode *a_5_level = NULL; // 这个就指向该层第一个节点
            for (int i_5 = 0; i_5 < a_5; i_5++)
            {
              string a_5_dir("/" + a4_uid + "a_5" + IntToString(i_5));
              string a_5_dir_("a_5" + IntToString(i_5));
              BTNode *temp_a = new BTNode;
              temp_a->child = NULL;
              temp_a->path = a_5_dir_;
              temp_a->sibling = NULL;
              if (i_5 == 0)
              {
                // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                a_4_level->child = temp_a;
                a_5_level = temp_a; // a_level指针作为上一个兄弟节点
              }
              else
              {
                // 如果不是第一个节点，就是上一个节点的兄弟节点
                a_5_level->sibling = temp_a;
                a_5_level = a_5_level->sibling; // a_level移动到下一个兄弟节点
              }
              string a5_uid = IntToString(uid_a5++); // 第五层
              if (IsLocal(a_5_dir))
              {
                MetadataEntry entry;
                entry.mutable_permissions();
                entry.set_type(DIR);
                entry.add_dir_contents(a5_uid);
                for (int i_6 = 0; i_6 < a_6; i_6++)
                {
                  entry.add_dir_contents("b" + IntToString(i_6));
                }
                string serialized_entry;
                entry.SerializeToString(&serialized_entry);
                store_->Put(a_5_dir, serialized_entry, 0);
              }
              BTNode *b_level = NULL; // 这个就指向该层第一个节点
              for (int i_6 = 0; i_6 < a_6; i_6++)
              {
                string a_6_dir("/" + a5_uid + "b" + IntToString(i_6));
                string a_6_dir_("b" + IntToString(i_6));

                BTNode *temp_a = new BTNode;
                temp_a->child = NULL;
                temp_a->path = a_6_dir_;
                temp_a->sibling = NULL;
                if (i_6 == 0)
                {
                  // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                  a_5_level->child = temp_a;
                  b_level = temp_a; // a_level指针作为上一个兄弟节点
                }
                else
                {
                  // 如果不是第一个节点，就是上一个节点的兄弟节点
                  b_level->sibling = temp_a;
                  b_level = b_level->sibling; // a_level移动到下一个兄弟节点
                }
                string a6_uid = IntToString(uid_a6++); // 第一层的uid，
                if (IsLocal(a_6_dir))
                {
                  MetadataEntry entry;
                  entry.mutable_permissions();
                  entry.set_type(DIR);
                  entry.add_dir_contents(a6_uid);
                  for (int i_7 = 0; i_7 < a_7; i_7++)
                  {
                    entry.add_dir_contents("c" + IntToString(i_7));
                  }
                  string serialized_entry;
                  entry.SerializeToString(&serialized_entry);
                  store_->Put(a_6_dir, serialized_entry, 0);
                }
                BTNode *a_7_level = NULL; // 这个就指向该层第一个节点

                for (int i_7 = 0; i_7 < a_7; i_7++)
                {
                  string a_7_dir(a_6_dir + "/c" + IntToString(i_7));
                  string a_7_dir_("c" + IntToString(i_7));
                  BTNode *temp_a = new BTNode;
                  temp_a->child = NULL;
                  temp_a->path = a_7_dir_;
                  temp_a->sibling = NULL;
                  if (i_7 == 0)
                  {
                    // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                    b_level->child = temp_a;
                    a_7_level = temp_a; // a_level指针作为上一个兄弟节点
                  }
                  else
                  {
                    // 如果不是第一个节点，就是上一个节点的兄弟节点
                    a_7_level->sibling = temp_a;
                    a_7_level = a_7_level->sibling; // a_level移动到下一个兄弟节点
                  }
                  string a7_uid = IntToString(uid_a7++); // 第一层的uid，
                  if (IsLocal(a_7_dir))
                  {
                    MetadataEntry entry;
                    entry.mutable_permissions();
                    entry.set_type(DIR);
                    entry.add_dir_contents(a7_uid);
                    for (int i_8 = 0; i_8 < a_8; i_8++)
                    {
                      entry.add_dir_contents("d" + IntToString(i_8));
                    }
                    string serialized_entry;
                    entry.SerializeToString(&serialized_entry);
                    store_->Put(a_7_dir, serialized_entry, 0);
                  }
                  BTNode *a_8_level = NULL; // 这个就指向该层第一个节点

                  for (int i_8 = 0; i_8 < a_8; i_8++)
                  {
                    string a_8_dir(a_7_dir + "/d" + IntToString(i_8));
                    string a_8_dir_("d" + IntToString(i_8));
                    BTNode *temp_a = new BTNode;
                    temp_a->child = NULL;
                    temp_a->path = a_8_dir_;
                    temp_a->sibling = NULL;
                    if (i_8 == 0)
                    {
                      // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                      a_7_level->child = temp_a;
                      a_8_level = temp_a; // a_level指针作为上一个兄弟节点
                    }
                    else
                    {
                      // 如果不是第一个节点，就是上一个节点的兄弟节点
                      a_8_level->sibling = temp_a;
                      a_8_level = a_8_level->sibling; // a_level移动到下一个兄弟节点
                    }
                    string a8_uid = IntToString(uid_a8++); // 第一层的uid，
                    if (IsLocal(a_8_dir))
                    {
                      MetadataEntry entry;
                      entry.mutable_permissions();
                      entry.set_type(DIR);
                      entry.add_dir_contents(a8_uid);
                      for (int i_9 = 0; i_9 < a_9; i_9++)
                      {
                        entry.add_dir_contents("e" + IntToString(i_9));
                      }
                      string serialized_entry;
                      entry.SerializeToString(&serialized_entry);
                      store_->Put(a_8_dir, serialized_entry, 0);
                    }
                    BTNode *a_9_level = NULL; // 这个就指向该层第一个节点

                    for (int i_9 = 0; i_9 < a_9; i_9++)
                    {
                      string a_9_dir(a_8_dir + "/e" + IntToString(i_9));
                      string a_9_dir_("e" + IntToString(i_9));
                      BTNode *temp_a = new BTNode;
                      temp_a->child = NULL;
                      temp_a->path = a_9_dir_;
                      temp_a->sibling = NULL;
                      if (i_9 == 0)
                      {
                        // gaoxuan --如果是第一个节点，就将他作为上一层的孩子
                        a_8_level->child = temp_a;
                        a_9_level = temp_a; // a_level指针作为上一个兄弟节点
                      }
                      else
                      {
                        // 如果不是第一个节点，就是上一个节点的兄弟节点
                        a_9_level->sibling = temp_a;
                        a_9_level = a_9_level->sibling; // a_level移动到下一个兄弟节点
                      }

                      string a9_uid = IntToString(uid_a9++); // 第五层
                      if (IsLocal(a_9_dir))
                      {
                        MetadataEntry entry;
                        entry.mutable_permissions();
                        entry.set_type(DATA);
                        entry.add_dir_contents(a9_uid); // 这个用不上，因为分层开始
                        FilePart *fp = entry.add_file_parts();
                        fp->set_length(RandomSize());
                        fp->set_block_id(0);
                        fp->set_block_offset(0);
                        string serialized_entry;
                        entry.SerializeToString(&serialized_entry);
                        store_->Put(a_9_dir, serialized_entry, 0);
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::Init() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

void MetadataStore::Init_from_txt(string filename)
{
  std::ifstream input_file(filename);
  string line;
  if (input_file.is_open())
  {
    // line中第一个字段是路径，第二个字段是类型，第三部分是元数据项
    while (getline(input_file, line))
    {
      line.erase(remove(line.begin(), line.end(), '\r'), line.end());
      string temp = line;
      temp = temp + " ";
      int pos = temp.find(" ");
      string key = temp.substr(0, pos);
      temp = temp.substr(pos + 1);

      pos = temp.find(" ");
      string str_type = temp.substr(0, pos);
      int type;
      if (str_type == "0")
      {
        type = 0;
      }
      else
      {
        type = 1;
      }
      temp = temp.substr(pos + 1);
      path_type[key] = type;
      //LOG(ERROR)<<key<<" in "<<config_->LookupMetadataShard(config_->HashFileName(key), config_->LookupReplica(machine_->machine_id()));
      if (IsLocal(key))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        while ((pos = temp.find(" ")) != std::string::npos)
        {
          string str = temp.substr(0, pos);
          entry.add_dir_contents(str);
          temp = temp.substr(pos + 1);
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(key, serialized_entry, 0);
      }
    }
  }
  else
  {
    LOG(ERROR) << "can't open file!";
  }
  input_file.close();
}
void MetadataStore::InitSmall()
{
  int asize = machine_->config().size();
  int bsize = 1000;

  double start = GetTime();

  // Update root dir.
  if (IsLocal(""))
  {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i = 0; i < 1000; i++)
    {
      entry.add_dir_contents("a" + IntToString(i));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }

  // Add dirs.
  for (int i = 0; i < asize; i++)
  {
    string dir("/a" + IntToString(i));
    if (IsLocal(dir))
    {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int j = 0; j < bsize; j++)
      {
        entry.add_dir_contents("b" + IntToString(j));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(dir, serialized_entry, 0);
    }
    // Add subdirs.
    for (int j = 0; j < bsize; j++)
    {
      string subdir(dir + "/b" + IntToString(j));
      if (IsLocal(subdir))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        entry.add_dir_contents("c");
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(subdir, serialized_entry, 0);
      }
      // Add files.
      string file(subdir + "/c");
      if (IsLocal(file))
      {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DATA);
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(file, serialized_entry, 0);
      }
    }
  }
  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::InitSmall() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

bool MetadataStore::IsLocal(const string &path)
{
  return machine_->machine_id() ==
         config_->LookupMetadataShard(
             config_->HashFileName(path),
             config_->LookupReplica(machine_->machine_id()));
}

void MetadataStore::getLOOKUP(string path)
{
  string from_hash = "";
  std::queue<string> level_queue;
  level_queue.push(from_hash);
  while (!level_queue.empty())
  {
    string front = level_queue.front();
    level_queue.pop();

    if (path_type[front] == 0)
    {

      // add rwset
      LOG(ERROR) << front;
      // get its child
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2); // 标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);

      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));
      // TODO：this part needs to add some parts to lookup request

      MessageBuffer *m = NULL;
      header->set_data_ptr(reinterpret_cast<uint64>(&m));
      machine_->SendMessage(header, new MessageBuffer());
      while (m == NULL)
      {
        usleep(10);
        Noop<MessageBuffer *>(m);
      }
      MessageBuffer *serialized = m;
      Action b;
      b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
      delete serialized;
      /*
      if (b.input() == "switch processed")
      {
        entry.set_type(DIR);
        entry.add_dir_contents("gaoxuan");
        break;
      }*/

      MetadataAction::LookupOutput out;
      out.ParseFromString(b.output());
      if (out.entry().type() == DIR)
      {
        string str = out.entry().dir_contents(0);
        str.erase(remove(str.begin(), str.end(), '\r'), str.end());
        string uid = str;
        for (int j = 1; j < out.entry().dir_contents_size(); j++)
        {
          string filename = out.entry().dir_contents(j);
          // filename.replace("\r","")
          //  push queue
          size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
          while (pos != string::npos)
          {                                     // 如果找到了
            filename.erase(pos, 1);             // 则删除该位置的字符
            pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
          }
          string child = "/" + uid + "/" + filename;
          level_queue.push(child);
        }
      }
    }
    else
    {
      std::queue<string> hash_queue;
      hash_queue.push(front);
      while (!hash_queue.empty())
      {
        string front1 = hash_queue.front();
        hash_queue.pop();
        LOG(ERROR) << front1;
        // get its child
        uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front1)), config_->LookupReplica(machine_->machine_id()));
        Header *header = new Header();
        header->set_flag(2); // 标识
        header->set_from(machine_->machine_id());
        header->set_to(mds_machine);
        header->set_type(Header::RPC);
        header->set_app("client");
        header->set_rpc("LOOKUP");
        header->add_misc_string(front1.c_str(), strlen(front1.c_str()));
        // TODO：this part needs to add some parts to lookup request

        MessageBuffer *m = NULL;
        header->set_data_ptr(reinterpret_cast<uint64>(&m));
        machine_->SendMessage(header, new MessageBuffer());
        while (m == NULL)
        {
          usleep(10);
          Noop<MessageBuffer *>(m);
        }
        MessageBuffer *serialized = m;
        Action b;
        b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
        delete serialized;
        /*
        if (b.input() == "switch processed")
        {
          entry.set_type(DIR);
          entry.add_dir_contents("gaoxuan");
          break;
        }*/
        MetadataAction::LookupOutput out;
        out.ParseFromString(b.output());

        for (int j = 1; j < out.entry().dir_contents_size(); j++)
        {
          string filename = out.entry().dir_contents(j);
          // filename.replace("\r","")
          //  push queue
          size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
          while (pos != string::npos)
          {                                     // 如果找到了
            filename.erase(pos, 1);             // 则删除该位置的字符
            pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
          }
          string child = front1 + "/" + filename;
          hash_queue.push(child);
        }
      }
    }
  }
  LOG(ERROR) << "finished LOOKUP";
}
void MetadataStore::
    GetRWSets(Action *action)
{ // gaoxuan --this function is called by RameFile() for RenameExperiment
  action->clear_readset();
  action->clear_writeset();

  MetadataAction::Type type =
      static_cast<MetadataAction::Type>(action->action_type());

  if (type == MetadataAction::CREATE_FILE)
  {
    MetadataAction::CreateFileInput in;
    in.ParseFromString(action->input());
  //  double start = GetTime();
    string parent_path = ParentDir(in.path());
    // 创建的话，这里需要进行搜索，发出lookup的根请求，别的还是直接等待请求回来，其实和LS里面一模一样啊
    // 只搜到他父亲的元数据项就可以，因为下一级的元数据还没创建，肯定是还不存在
    string front = ""; // 最初的位置只发一个根目录的请求
    uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
    Header *header = new Header();
    header->set_flag(2); // 标识
    header->set_from(machine_->machine_id());
    header->set_original_from(machine_->machine_id()); // 设置最终要返回的机器
    header->set_to(mds_machine);
    header->set_type(Header::RPC);
    header->set_app("client");
    header->set_rpc("LOOKUP");
    header->add_misc_string(front.c_str(), strlen(front.c_str()));
    // TODO：this part needs to add some parts to lookup request
    string s = parent_path;
    if (s != "")
    {
      int flag = 0;
      char pattern = '/';
      string temp_from = parent_path;
      temp_from = temp_from.substr(1, temp_from.size()); // 这一行是为了去除最前面的/
      temp_from = temp_from + pattern;                   // 在最后面添加一个/便于处理
      int pos = temp_from.find(pattern);                 // 找到第一个/的位置
      while (pos != std::string::npos)                   // 循环不断找/，找到一个拆分一次
      {
        string temp1 = temp_from.substr(0, pos); // temp里面就是拆分出来的第一个子串
        string temp = temp1;
        for (int i = temp.size(); i < 4; i++)
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
        string temp = "    ";                // 用四个空格填充一下
        header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
        flag++;                              // 拆分的字符串数量++
      }
    }
    else
    {

      int flag = 0; // 用来标识此时split_string 里面有多少子串
      while (flag != 8)
      {
        string temp = "    ";                // 用四个空格填充一下
        header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
        flag++;                              // 拆分的字符串数量++
      }
      header->set_from_length(0); // 设置长度为0为根目录
    }
    header->set_depth(0); // 初始就为0
    int uid = switch_uid;
    header->set_uid(uid);
    // before this part is split

    MessageBuffer *m = NULL;
    header->set_data_ptr(reinterpret_cast<uint64>(&m));
    machine_->SendMessage(header, new MessageBuffer());
    while (m == NULL)
    {
      usleep(10);
      Noop<MessageBuffer *>(m);
    }
    MessageBuffer *serialized = m;

    Action b;
    b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());

    delete serialized;
    MetadataAction::LookupOutput out;
    out.ParseFromString(b.output());
    // 上面部分就是发出lookup请求，获取其父目录的元数据项的过程
    if(out.success() && out.entry().type() == DIR)
    {
      string entry_path = out.path();
      string child;
      //将父目录添加读写集
      action->add_readset(entry_path);
      action->add_writeset(entry_path);  
      if(path_type[entry_path] == 0)//树
      {
        child = "/" + out.entry().dir_contents(0) + "/" + FileName(in.path());       
        path_type[child] = 0;
      }
      else//hash
      {
        child = entry_path + "/" + FileName(in.path());
        path_type[child] = 1;
      }
      action->add_readset(child);
      action->add_writeset(child);  
      action->set_from_parent(entry_path);//父目录路径
      action->set_from_hash(child);//要创建的目录路径
     // LOG(ERROR)<<"GetRW time: "<<GetTime() - start;
    }
    else
    {
      LOG(ERROR)<<"can't create!";
    }
  }
  else if (type == MetadataAction::ERASE)
  {
    MetadataAction::EraseInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
    action->add_readset(ParentDir(in.path()));
    action->add_writeset(ParentDir(in.path()));
  }
  else if (type == MetadataAction::COPY)
  {
    MetadataAction::CopyInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.from_path());
    action->add_writeset(in.to_path());
    action->add_readset(ParentDir(in.to_path()));
    action->add_writeset(ParentDir(in.to_path()));
  }
  else if (type == MetadataAction::RENAME)
  { // the version of gaoxuan
    // gaoxuan --add read/write set in the way of DFS

    MetadataAction::RenameInput in;
    in.ParseFromString(action->input());
    string from_path = in.from_path();
    string to_path = in.to_path();
    string from_parent;
    // use vector to split from_path and to_path
    std::vector<string> FROM_PATH_SPLIT;
    std::vector<string> TO_PATH_SPLIT;
    char pattern = '/'; // use '/' to split path
    from_path = from_path + pattern;
    to_path = to_path + pattern; // add '/' so that we can deal with is more conveniently

    from_path = from_path.substr(1);
    to_path = to_path.substr(1); // remove the first '/' in from_path and to_path

    FROM_PATH_SPLIT.push_back("");
    TO_PATH_SPLIT.push_back(""); // add root dir to split path

    // the following operation is to split path
    int pos_from, pos_to;
    while ((pos_from = from_path.find(pattern)) != std::string::npos)
    {
      // split from_path
      string split_string = from_path.substr(0, pos_from);
      FROM_PATH_SPLIT.push_back(split_string);
      from_path = from_path.substr(pos_from + 1);
    }
    while ((pos_to = to_path.find(pattern)) != std::string::npos)
    {
      // split to_path
      string split_string = to_path.substr(0, pos_to);
      TO_PATH_SPLIT.push_back(split_string);
      to_path = to_path.substr(pos_to + 1);
    }
    int from_type, to_type;
    // traverse to get from_parent path and from path itself
    string from_uid;
    int i;
    string from_hash;
    bool from_hash_flag = false;
    int from_parent_depth = Dir_depth(ParentDir(in.from_path()));
    int from_depth = Dir_depth(in.from_path());
    for (i = 0; i < FROM_PATH_SPLIT.size(); i++)
    {
      string front;
      if (i == 0)
      {
        // root dir
        front = FROM_PATH_SPLIT[i]; // front = "";
      }
      else
      {
        front = "/" + from_uid + "/" + FROM_PATH_SPLIT[i];
        if (i == FROM_PATH_SPLIT.size() - 1)
        {
          // all path is tree
          from_type = path_type[front];
          from_hash = front;
          break;
        }
      }
      if (i == from_parent_depth)
      {
        // two situation: parent is tree/parent is hash while grandparent is tree
        // add RWset directly
        action->set_from_parent(front);
        from_parent = front;
        action->add_readset(front);
        action->add_writeset(front);
      }
      if (path_type[front] == 1)
      {
        i++; // to traverse next one
        from_hash_flag = true;
        from_hash = front;
        break; // is hash, do not need to traverse
      }
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2); // 标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));
      // TODO：this part needs to add some parts to lookup request

      MessageBuffer *m = NULL;
      header->set_data_ptr(reinterpret_cast<uint64>(&m));
      machine_->SendMessage(header, new MessageBuffer());
      while (m == NULL)
      {
        usleep(10);
        Noop<MessageBuffer *>(m);
      }
      MessageBuffer *serialized = m;
      Action b;
      b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
      delete serialized;
      /*
      if (b.input() == "switch processed")
      {
        entry.set_type(DIR);
        entry.add_dir_contents("gaoxuan");
        break;
      }*/
      MetadataAction::LookupOutput out;
      out.ParseFromString(b.output());
      // remove potential \r
      string str = out.entry().dir_contents(0);
      str.erase(remove(str.begin(), str.end(), '\r'), str.end());
      from_uid = str;
    }
    if (from_hash_flag == true)
    {

      // from_path is hash
      for (; i < FROM_PATH_SPLIT.size(); i++)
      {
        from_hash = from_hash + "/" + FROM_PATH_SPLIT[i];
        if (i == from_parent_depth)
        {
          // two situation: parent is tree/parent is hash while grandparent is tree
          // add RWset directly
          from_parent = from_hash;
          action->set_from_parent(from_parent);
          action->add_readset(from_hash);
          action->add_writeset(from_hash);
        }
      }

      from_type = path_type[from_hash];
    }
    // traverse to get to_parent path and to path itself
    string to_uid;
    string to_hash;
    bool to_hash_flag = false;
    int to_parent_depth = Dir_depth(ParentDir(in.to_path()));
    int to_depth = Dir_depth(in.to_path());
    // this part is used to add ParentDir(to_path) and to_path to RWset
    string to_parent;
    for (i = 0; i < TO_PATH_SPLIT.size(); i++)
    {
      string front;
      if (i == 0)
      {
        // root dir
        front = TO_PATH_SPLIT[i]; // front = "";
      }
      else
      {
        front = "/" + to_uid + "/" + TO_PATH_SPLIT[i];
        if (i == TO_PATH_SPLIT.size() - 1)
        { // all path is tree type
          to_hash = front;
          break; // we get the path we want, dont need to get entry
        }
      }
      // check this part if the parent of to_path with parent_depth
      if (i == to_parent_depth)
      {
        // two situation: parent is tree/parent is hash while grandparent is tree
        // add RWset directly
        action->set_to_parent(front);
        to_parent = front;
        if (from_parent != front)
        {
          action->add_readset(front);
          action->add_writeset(front);
        }
      }
      if (path_type[front] == 1)
      {
        i++; // to traverse next one
        to_hash_flag = true;
        to_hash = front;
        break; // is hash, do not need to traverse
      }
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2); // 标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));
      // TODO：this part needs to add some parts to lookup request

      MessageBuffer *m = NULL;
      header->set_data_ptr(reinterpret_cast<uint64>(&m));
      machine_->SendMessage(header, new MessageBuffer());
      while (m == NULL)
      {
        usleep(10);
        Noop<MessageBuffer *>(m);
      }
      MessageBuffer *serialized = m;
      Action b;
      b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
      delete serialized;
      /*
      if (b.input() == "switch processed")
      {
        entry.set_type(DIR);
        entry.add_dir_contents("gaoxuan");
        break;
      }*/
      MetadataAction::LookupOutput out;
      out.ParseFromString(b.output());
      string str = out.entry().dir_contents(0);
      str.erase(remove(str.begin(), str.end(), '\r'), str.end());
      to_uid = str;
    }
    if (to_hash_flag == true)
    {
      // to_path is hash
      for (; i < TO_PATH_SPLIT.size(); i++)
      {
        to_hash = to_hash + "/" + TO_PATH_SPLIT[i];
        // this situation is grandparent is hash
        if (i == to_parent_depth)
        {
          // two situation: parent is tree/parent is hash while grandparent is tree
          // add RWset directly
          action->set_to_parent(to_hash);
          if (from_parent != to_hash)
          {
            action->add_readset(to_hash);
            action->add_writeset(to_hash);
          }
          to_parent = to_hash;
        }
      }
    }
    path_type[to_hash] = path_type[to_parent];
    to_type = path_type[to_hash];
    action->set_from_hash(from_hash);
    action->set_to_hash(to_hash);

    // now the path is in from_hash and to_hash, type is from_type and to_type;
    if (from_type == 0 && to_type == 0)
    {

      action->add_writeset(to_hash); // only need to add write to target path
      // tree to tree
      std::queue<string> level_queue;
      level_queue.push(from_hash);
      while (!level_queue.empty())
      {
        string front = level_queue.front();
        level_queue.pop();

        if (path_type[front] == 0)
        {

          // add rwset
          action->add_readset(front);
          action->add_writeset(front);
          // get its child
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2); // 标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);

          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
          // TODO：this part needs to add some parts to lookup request

          MessageBuffer *m = NULL;
          header->set_data_ptr(reinterpret_cast<uint64>(&m));
          machine_->SendMessage(header, new MessageBuffer());
          while (m == NULL)
          {
            usleep(10);
            Noop<MessageBuffer *>(m);
          }
          MessageBuffer *serialized = m;
          Action b;
          b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
          delete serialized;
          /*
          if (b.input() == "switch processed")
          {
            entry.set_type(DIR);
            entry.add_dir_contents("gaoxuan");
            break;
          }*/

          MetadataAction::LookupOutput out;
          out.ParseFromString(b.output());
          if (out.entry().type() == DIR && out.entry().dir_contents_size() > 1)
          {
            string str = out.entry().dir_contents(0);
            str.erase(remove(str.begin(), str.end(), '\r'), str.end());
            string uid = str;
            for (int j = 1; j < out.entry().dir_contents_size(); j++)
            {
              string filename = out.entry().dir_contents(j);
              // filename.replace("\r","")
              //  push queue
              size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
              while (pos != string::npos)
              {                                     // 如果找到了
                filename.erase(pos, 1);             // 则删除该位置的字符
                pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
              }
              string child = "/" + uid + "/" + filename;
              level_queue.push(child);
            }
          }
        }
        else
        {

          std::queue<string> hash_queue;
          hash_queue.push(front);
          while (!hash_queue.empty())
          {
            // 直接将所有孩子都用hash的方式加入读写集
            string front1 = hash_queue.front();
            hash_queue.pop();
            action->add_readset(front1);
            action->add_writeset(front1);
            // get its child
            uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front1)), config_->LookupReplica(machine_->machine_id()));
            Header *header = new Header();
            header->set_flag(2); // 标识
            header->set_from(machine_->machine_id());
            header->set_to(mds_machine);
            header->set_type(Header::RPC);
            header->set_app("client");
            header->set_rpc("LOOKUP");
            header->add_misc_string(front1.c_str(), strlen(front1.c_str()));
            // TODO：this part needs to add some parts to lookup request

            MessageBuffer *m = NULL;
            header->set_data_ptr(reinterpret_cast<uint64>(&m));
            machine_->SendMessage(header, new MessageBuffer());
            while (m == NULL)
            {
              usleep(10);
              Noop<MessageBuffer *>(m);
            }
            MessageBuffer *serialized = m;
            Action b;
            b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
            delete serialized;
            /*
            if (b.input() == "switch processed")
            {
              entry.set_type(DIR);
              entry.add_dir_contents("gaoxuan");
              break;
            }*/
            MetadataAction::LookupOutput out;
            out.ParseFromString(b.output());

            for (int j = 1; j < out.entry().dir_contents_size(); j++)
            {
              string filename = out.entry().dir_contents(j);
              // filename.replace("\r","")
              //  push queue
              size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
              while (pos != string::npos)
              {                                     // 如果找到了
                filename.erase(pos, 1);             // 则删除该位置的字符
                pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
              }
              string child = front1 + "/" + filename;
              hash_queue.push(child);
            }
          }
        }
      }
    }
    else if (from_type == 0 && to_type == 1)
    {
      // tree to hash
      std::queue<string> level_queue;
      std::queue<string> target_queue;
      level_queue.push(from_hash);
      target_queue.push(to_hash);
      while (!level_queue.empty())
      {
        string front = level_queue.front();
        level_queue.pop();
        string target_front = target_queue.front();
        target_queue.pop();
        if (path_type[front] == 0)
        {
          // add rwset
          action->add_readset(front);
          action->add_writeset(front);
          action->add_writeset(target_front);
          //          LOG(ERROR) << "writeset: " << target_front;
          path_type[target_front] = 1; // 目的位置是hash，下面全是hash
          // get its child
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2); // 标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
          // TODO：this part needs to add some parts to lookup request

          MessageBuffer *m = NULL;
          header->set_data_ptr(reinterpret_cast<uint64>(&m));
          machine_->SendMessage(header, new MessageBuffer());
          while (m == NULL)
          {
            usleep(10);
            Noop<MessageBuffer *>(m);
          }
          MessageBuffer *serialized = m;
          Action b;
          b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
          delete serialized;
          /*
          if (b.input() == "switch processed")
          {
            entry.set_type(DIR);
            entry.add_dir_contents("gaoxuan");
            break;
          }*/
          MetadataAction::LookupOutput out;
          out.ParseFromString(b.output());
          if (out.entry().type() == DIR)
          {
            string str = out.entry().dir_contents(0);
            str.erase(remove(str.begin(), str.end(), '\r'), str.end());
            string uid = str;
            for (int j = 1; j < out.entry().dir_contents_size(); j++)
            {
              string filename = out.entry().dir_contents(j);
              //  push queue
              size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
              while (pos != string::npos)
              {                                     // 如果找到了
                filename.erase(pos, 1);             // 则删除该位置的字符
                pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
              }
              string child = "/" + uid + "/" + filename;
              string hash_child = target_front + "/" + filename;
              level_queue.push(child);
              target_queue.push(hash_child);
            }
          }
        }
        else
        {

          std::queue<string> hash_queue;
          std::queue<string> target_hash_queue;
          hash_queue.push(front);
          target_hash_queue.push(target_front);
          while (!hash_queue.empty())
          {
            // 直接将所有孩子都用hash的方式加入读写集
            string front1 = hash_queue.front();
            string target_front1 = target_hash_queue.front();
            hash_queue.pop();
            target_hash_queue.pop();
            path_type[target_front1] = 1;
            action->add_readset(front1);
            action->add_writeset(front1);
            action->add_writeset(target_front1);
            // get its child
            uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front1)), config_->LookupReplica(machine_->machine_id()));
            Header *header = new Header();
            header->set_flag(2); // 标识
            header->set_from(machine_->machine_id());
            header->set_to(mds_machine);
            header->set_type(Header::RPC);
            header->set_app("client");
            header->set_rpc("LOOKUP");
            header->add_misc_string(front1.c_str(), strlen(front1.c_str()));
            // TODO：this part needs to add some parts to lookup request

            MessageBuffer *m = NULL;
            header->set_data_ptr(reinterpret_cast<uint64>(&m));
            machine_->SendMessage(header, new MessageBuffer());
            while (m == NULL)
            {
              usleep(10);
              Noop<MessageBuffer *>(m);
            }
            MessageBuffer *serialized = m;
            Action b;
            b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
            delete serialized;
            /*
            if (b.input() == "switch processed")
            {
              entry.set_type(DIR);
              entry.add_dir_contents("gaoxuan");
              break;
            }*/
            MetadataAction::LookupOutput out;
            out.ParseFromString(b.output());

            for (int j = 1; j < out.entry().dir_contents_size(); j++)
            {
              string filename = out.entry().dir_contents(j);
              // filename.replace("\r","")
              //  push queue
              size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
              while (pos != string::npos)
              {                                     // 如果找到了
                filename.erase(pos, 1);             // 则删除该位置的字符
                pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
              }
              // push queue
              string child = front1 + "/" + filename;
              string target_child = target_front1 + "/" + filename;
              hash_queue.push(child);
              target_hash_queue.push(target_child);
            }
          }
        }
      }
    }
    else if (from_type == 1 && to_type == 0)
    {
      // hash to tree
      std::queue<string> level_queue;
      std::queue<string> target_queue;
      target_queue.push(to_hash);
      level_queue.push(from_hash);
      while (!level_queue.empty())
      {
        // don't need to differentiate the tree and hash, because it must be hash
        string front = level_queue.front();
        string target_front = target_queue.front();
        level_queue.pop();
        target_queue.pop();
        // add rwset
        action->add_readset(front);
        action->add_writeset(front);
        action->add_writeset(target_front);
        path_type[target_front] = 0; // rename到树下，全是树
        // get its child
        uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
        Header *header = new Header();
        header->set_flag(2); // 标识
        header->set_from(machine_->machine_id());
        header->set_to(mds_machine);
        header->set_type(Header::RPC);
        header->set_app("client");
        header->set_rpc("LOOKUP");
        header->add_misc_string(front.c_str(), strlen(front.c_str()));
        // TODO：this part needs to add some parts to lookup request

        MessageBuffer *m = NULL;
        header->set_data_ptr(reinterpret_cast<uint64>(&m));
        machine_->SendMessage(header, new MessageBuffer());
        while (m == NULL)
        {
          usleep(10);
          Noop<MessageBuffer *>(m);
        }
        MessageBuffer *serialized = m;
        Action b;
        b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
        delete serialized;
        /*
        if (b.input() == "switch processed")
        {
          entry.set_type(DIR);
          entry.add_dir_contents("gaoxuan");
          break;
        }*/
        MetadataAction::LookupOutput out;
        out.ParseFromString(b.output());

        for (int j = 1; j < out.entry().dir_contents_size(); j++)
        {
          string filename = out.entry().dir_contents(j);
          // filename.replace("\r","")
          //  push queue
          size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
          while (pos != string::npos)
          {                                     // 如果找到了
            filename.erase(pos, 1);             // 则删除该位置的字符
            pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
          }
          string child = front + "/" + filename;
          // target path's child are all tree
          string str = out.entry().dir_contents(0);
          str.erase(remove(str.begin(), str.end(), '\r'), str.end());
          string target_child = "/" + str + "/" + filename;
          level_queue.push(child);
          target_queue.push(target_child);
        }
      }
    }
    else
    {
      // hash to hash
      std::queue<string> level_queue;
      std::queue<string> target_queue;
      target_queue.push(to_hash);
      level_queue.push(from_hash);
      while (!level_queue.empty())
      {
        // don't need to differentiate the tree and hash, because it must be hash
        string front = level_queue.front();
        string target_front = target_queue.front();
        level_queue.pop();
        target_queue.pop();
        // add rwset
        action->add_readset(front);
        action->add_writeset(front);
        action->add_writeset(target_front);
        path_type[target_front] = 1; // hash下全是hash
        // get its child
        uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
        Header *header = new Header();
        header->set_flag(2); // 标识
        header->set_from(machine_->machine_id());
        header->set_to(mds_machine);
        header->set_type(Header::RPC);
        header->set_app("client");
        header->set_rpc("LOOKUP");
        header->add_misc_string(front.c_str(), strlen(front.c_str()));
        // TODO：this part needs to add some parts to lookup request

        MessageBuffer *m = NULL;
        header->set_data_ptr(reinterpret_cast<uint64>(&m));
        machine_->SendMessage(header, new MessageBuffer());
        while (m == NULL)
        {
          usleep(10);
          Noop<MessageBuffer *>(m);
        }
        MessageBuffer *serialized = m;
        Action b;
        b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
        delete serialized;
        /*
        if (b.input() == "switch processed")
        {
          entry.set_type(DIR);
          entry.add_dir_contents("gaoxuan");
          break;
        }*/
        MetadataAction::LookupOutput out;
        out.ParseFromString(b.output());

        for (int j = 1; j < out.entry().dir_contents_size(); j++)
        {
          string filename = out.entry().dir_contents(j);
          // filename.replace("\r","")
          //  push queue
          size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
          while (pos != string::npos)
          {                                     // 如果找到了
            filename.erase(pos, 1);             // 则删除该位置的字符
            pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
          }
          string child = front + "/" + filename;
          string target_child = target_front + "/" + filename;
          level_queue.push(child);
          target_queue.push(target_child);
        }
      }
    }
  }
  else if (type == MetadataAction::LOOKUP)
  {
    MetadataAction::LookupInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
  }
  else if (type == MetadataAction::TREE_LOOKUP)
  {
    MetadataAction::Tree_LookupInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
  }
  else if (type == MetadataAction::RESIZE)
  {
    MetadataAction::ResizeInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
  }
  else if (type == MetadataAction::WRITE)
  {
    MetadataAction::WriteInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
  }
  else if (type == MetadataAction::APPEND)
  {
    MetadataAction::AppendInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
  }
  else if (type == MetadataAction::CHANGE_PERMISSIONS)
  {
    MetadataAction::ChangePermissionsInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
  }
  else
  {
    LOG(FATAL) << "invalid action type";
  }
}

void MetadataStore::Run(Action *action)
{
  // gaoxuan --this part will be executed by scheduler after action has beed append to log
  //  Prepare by performing all reads.
  // double start;
  // if (action->action_type() == MetadataAction::CREATE_FILE)
  // {
  //   start = GetTime();
  // }
  ExecutionContext *context;
  if (machine_ == NULL)
  {
    context = new ExecutionContext(store_, action);
  }
  else
  { // gaoxuan --we can confirm we get all entry we want,because in DistributedExecutionContext has this logic
    context =
        new DistributedExecutionContext(machine_, config_, store_, action);
  }

  if (!context->IsWriter())
  {

    delete context;
    return;
  }

  // Execute action.
  MetadataAction::Type type =
      static_cast<MetadataAction::Type>(action->action_type());

  if (type == MetadataAction::CREATE_FILE)
  {
    MetadataAction::CreateFileInput in;
    MetadataAction::CreateFileOutput out;
    in.ParseFromString(action->input());
    CreateFile_Internal(context, in, &out, action->from_hash(), action->from_parent());
    out.SerializeToString(action->mutable_output());

   // LOG(ERROR)<<"create run : "<<GetTime() - start;
  }
  else if (type == MetadataAction::ERASE)
  {
    MetadataAction::EraseInput in;
    MetadataAction::EraseOutput out;
    in.ParseFromString(action->input());
    Erase_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
  }
  else if (type == MetadataAction::COPY)
  {
    MetadataAction::CopyInput in;
    MetadataAction::CopyOutput out;
    in.ParseFromString(action->input());
    Copy_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
  }
  else if (type == MetadataAction::RENAME)
  {

    MetadataAction::RenameInput in;
    MetadataAction::RenameOutput out;
    in.ParseFromString(action->input());
    // LOG(ERROR)<<"Rename is running";
    Rename_Internal(context, in, &out, action->from_hash(), action->to_hash(), action->from_parent(), action->to_parent());
    out.SerializeToString(action->mutable_output());
  }
  else if (type == MetadataAction::LOOKUP)
  {
    MetadataAction::LookupInput in;
    MetadataAction::LookupOutput out;
    // LOG(ERROR)<<"The logic of LOOKUP in Run ";
    in.ParseFromString(action->input());
    Lookup_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
  }
  else if (type == MetadataAction::TREE_LOOKUP)
  {
    MetadataAction::Tree_LookupInput in;
    MetadataAction::Tree_LookupOutput out;
    in.ParseFromString(action->input());
    Tree_Lookup_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
  }
  else if (type == MetadataAction::RESIZE)
  {
    MetadataAction::ResizeInput in;
    MetadataAction::ResizeOutput out;
    in.ParseFromString(action->input());
    Resize_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
  }
  else if (type == MetadataAction::WRITE)
  {
    MetadataAction::WriteInput in;
    MetadataAction::WriteOutput out;
    in.ParseFromString(action->input());
    Write_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
  }
  else if (type == MetadataAction::APPEND)
  {
    MetadataAction::AppendInput in;
    MetadataAction::AppendOutput out;
    in.ParseFromString(action->input());
    Append_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
  }
  else if (type == MetadataAction::CHANGE_PERMISSIONS)
  {
    MetadataAction::ChangePermissionsInput in;
    MetadataAction::ChangePermissionsOutput out;
    in.ParseFromString(action->input());
    ChangePermissions_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
  }
  else
  {
    LOG(FATAL) << "invalid action type";
  }

  delete context;
}

void MetadataStore::CreateFile_Internal(
    ExecutionContext *context,
    const MetadataAction::CreateFileInput &in,
    MetadataAction::CreateFileOutput *out, string path, string parent_path)
{
  // Don't fuck with the root dir.
  // if (in.path() == "")
  // {
  //   out->set_success(false);
  //   out->add_errors(MetadataAction::PermissionDenied);
  //   return;
  // }

  MetadataEntry parent_entry;
  if (!context->GetEntry(parent_path, &parent_entry))
  {
    // Parent doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string filename = FileName(in.path());
  for (int i = 1; i < parent_entry.dir_contents_size(); i++)
  {
    if (parent_entry.dir_contents(i) == filename)
    {
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }
  // Update parent.
  parent_entry.add_dir_contents(filename);
  context->PutEntry(parent_path, parent_entry);

  // Add entry.
  MetadataEntry entry;
  entry.mutable_permissions()->CopyFrom(in.permissions());
  entry.set_type(in.type());
  entry.add_dir_contents(IntToString(machine_->GetGUID()));
  context->PutEntry(path, entry);
}

void MetadataStore::Erase_Internal(
    ExecutionContext *context,
    const MetadataAction::EraseInput &in,
    MetadataAction::EraseOutput *out)
{
  // Don't fuck with the root dir.
  if (in.path() == "")
  {
    out->set_success(false);
    out->add_errors(MetadataAction::PermissionDenied);
    return;
  }
  // Look up parent dir.
  string parent_path = ParentDir(in.path());
  MetadataEntry parent_entry;
  if (!context->GetEntry(parent_path, &parent_entry))
  {
    // Parent doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }
  // Look up target file.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry))
  {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }
  if (entry.type() == DIR && entry.dir_contents_size() != 0)
  {
    // Trying to delete a non-empty directory!
    out->set_success(false);
    out->add_errors(MetadataAction::DirectoryNotEmpty);
    return;
  }

  // TODO(agt): Check permissions.

  // Delete target file entry.
  context->DeleteEntry(in.path());

  // Find file and remove it from parent directory.
  string filename = FileName(in.path());
  for (int i = 0; i < parent_entry.dir_contents_size(); i++)
  {
    if (parent_entry.dir_contents(i) == filename)
    {
      // Remove reference to target file entry from dir contents.
      parent_entry.mutable_dir_contents()
          ->SwapElements(i, parent_entry.dir_contents_size() - 1);
      parent_entry.mutable_dir_contents()->RemoveLast();

      // Write updated parent entry.
      context->PutEntry(parent_path, parent_entry);
      return;
    }
  }
}

void MetadataStore::Copy_Internal(
    ExecutionContext *context,
    const MetadataAction::CopyInput &in,
    MetadataAction::CopyOutput *out)
{

  // Currently only support Copy: (non-recursive: only succeeds for DATA files and EMPTY directory)
  MetadataEntry from_entry;
  if (!context->GetEntry(in.from_path(), &from_entry))
  {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_to_path = ParentDir(in.to_path());
  MetadataEntry parent_to_entry;
  if (!context->GetEntry(parent_to_path, &parent_to_entry))
  {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // If file already exists, fail.
  string filename = FileName(in.to_path());
  for (int i = 0; i < parent_to_entry.dir_contents_size(); i++)
  {
    if (parent_to_entry.dir_contents(i) == filename)
    {
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }

  // Update parent
  parent_to_entry.add_dir_contents(filename);
  context->PutEntry(parent_to_path, parent_to_entry);

  // Add entry
  MetadataEntry to_entry;
  to_entry.CopyFrom(from_entry);
  context->PutEntry(in.to_path(), to_entry);
}

void MetadataStore::Rename_Internal(
    ExecutionContext *context,
    const MetadataAction::RenameInput &in,
    MetadataAction::RenameOutput *out, string from_hash, string to_hash, string from_parent, string to_parent)
{
  LOG(ERROR) << "rename internal in machine [" << machine_->machine_id();
  MetadataEntry from_parent_entry;
  if (!context->GetEntry(from_parent, &from_parent_entry))
  {
    // File doesn't exist!
    LOG(ERROR) << "From_parent File doesn't exist!";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }
  MetadataEntry to_parent_entry;
  if (!context->GetEntry(to_parent, &to_parent_entry))
  {
    // File doesn't exist!
    LOG(ERROR) << "To_parent File doesn't exist!";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }
  MetadataEntry from_entry;
  if (!context->GetEntry(from_hash, &from_entry))
  {
    // File doesn't exist!
    LOG(ERROR) << "From File doesn't exist!";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string to_filename = FileName(in.to_path());
  for (int i = 1; i < to_parent_entry.dir_contents_size(); i++)
  {
    if (to_parent_entry.dir_contents(i) == to_filename)
    {
      LOG(ERROR) << "file already exists, fail.";
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }
  int from_type = path_type[from_hash];
  int to_type = path_type[to_hash];
  if (from_type == 0 && to_type == 0)
  { // tree to tree

    if (from_parent != to_parent)
    {
      to_parent_entry.add_dir_contents(to_filename);
      context->PutEntry(to_parent, to_parent_entry);
    }
    else
    {

      from_parent_entry.add_dir_contents(to_filename);
    }
    string from_filename = FileName(in.from_path());

    for (int i = 1; i < from_parent_entry.dir_contents_size(); i++)
    {
      string str = from_parent_entry.dir_contents(i);
      str.erase(remove(str.begin(), str.end(), '\r'), str.end());
      if (str == from_filename)
      {
        // Remove reference to target file entry from dir contents.
        from_parent_entry.mutable_dir_contents()
            ->SwapElements(i, from_parent_entry.dir_contents_size() - 1);
        from_parent_entry.mutable_dir_contents()->RemoveLast();
        // Write updated parent entry.
        context->PutEntry(from_parent, from_parent_entry);
        break;
      }
    }
    MetadataEntry to_entry; // gaoxuan --the entry which will be added
    to_entry.CopyFrom(from_entry);
    context->PutEntry(to_hash, to_entry);
    // 删除原位置元数据项
    context->DeleteEntry(from_hash);
  }
  else if (from_type == 0 && to_type == 1)
  { // tree to hash
    if (from_parent != to_parent)
    {
      to_parent_entry.add_dir_contents(to_filename);
      context->PutEntry(to_parent, to_parent_entry);
    }
    else
    {
      from_parent_entry.add_dir_contents(to_filename);
    }
    string from_filename = FileName(in.from_path());
    // 源父目录删除
    for (int i = 1; i < from_parent_entry.dir_contents_size(); i++)
    {
      string str = from_parent_entry.dir_contents(i);
      str.erase(remove(str.begin(), str.end(), '\r'), str.end());
      if (str == from_filename)
      {
        // Remove reference to target file entry from dir contents.
        from_parent_entry.mutable_dir_contents()
            ->SwapElements(i, from_parent_entry.dir_contents_size() - 1);
        from_parent_entry.mutable_dir_contents()->RemoveLast();
        // Write updated parent entry.
        context->PutEntry(from_parent, from_parent_entry);
        break;
      }
    }
    if (from_entry.type() == DIR)
    {
      // gaoxuan --use BFS to add new metadata entry
      std::queue<string> queue1; // 这个用来遍历树
      std::queue<string> queue2; // 这个用来遍历hash
      queue1.push(from_hash);
      queue2.push(to_hash);
      while (!queue1.empty())
      {
        string front = queue1.front();
        queue1.pop();
        string hash_front = queue2.front();
        queue2.pop();

        if (path_type[front] == 0) // 不是hash点
        {
          MetadataEntry to;   // gaoxuan --the entry which will be added
          MetadataEntry from; // gaoxuan --the entry which will be used to copy to to_entry
          if (!context->GetEntry(front, &from))
          {
            LOG(ERROR) << "dont getentry";
          }
          to.CopyFrom(from);

          context->PutEntry(hash_front, to);
          // Erase the from_entry
          //  LOG(ERROR) << hash_front << " and " << to.dir_contents(0);
          if (to.type() == DIR)
          {
            string uid = to.dir_contents(0);
            uid.erase(remove(uid.begin(), uid.end(), '\r'), uid.end());
            for (int i = 1; i < to.dir_contents_size(); i++)
            {
              string filename = to.dir_contents(i);
              //  push queue
              size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
              while (pos != string::npos)
              {                                     // 如果找到了
                filename.erase(pos, 1);             // 则删除该位置的字符
                pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
              }
              string child = "/" + uid + "/" + filename;
              string hash_child = hash_front + "/" + filename;
              queue1.push(child);
              queue2.push(hash_child);
            }
            context->DeleteEntry(front);
          }
        }
        else
        {

          // 这里不太对，如果是hash，需要单独开一个层次遍历，先把下面的都弄上
          std::queue<string> hash_queue;
          std::queue<string> target_hash_queue;
          hash_queue.push(front);
          target_hash_queue.push(hash_front);
          while (!hash_queue.empty())
          {
            // 直接将所有孩子都用hash的方式加入读写集
            string front1 = hash_queue.front();
            string target_front1 = target_hash_queue.front();
            hash_queue.pop();
            target_hash_queue.pop();
            MetadataEntry to;   // gaoxuan --the entry which will be added
            MetadataEntry from; // gaoxuan --the entry which will be used to copy to to_entry
            context->GetEntry(front1, &from);
            to.CopyFrom(from);
            //  LOG(ERROR)<<hash_front<<" and "<<to_entry1.dir_contents(0);
            context->PutEntry(target_front1, to);
            // Erase the from_entry
            context->DeleteEntry(front1);

            for (int j = 1; j < to.dir_contents_size(); j++)
            {
              string filename = to.dir_contents(j);
              // filename.replace("\r","")
              //  push queue
              size_t pos = filename.find_first_of("\r"); // 查找第一个回车符的位置
              while (pos != string::npos)
              {                                     // 如果找到了
                filename.erase(pos, 1);             // 则删除该位置的字符
                pos = filename.find_first_of("\r"); // 继续查找下一个回车符的位置 不知道为什么会引入这个，但确实这个会引起问题
              }
              // push queue
              string child = front1 + "/" + filename;
              string target_child = target_front1 + "/" + filename;
              hash_queue.push(child);
              target_hash_queue.push(target_child);
            }
          }
        }
      }
    }
    else
    {
      // 原目录是个文件
      MetadataEntry to_entry1; // gaoxuan --the entry which will be added
      to_entry1.CopyFrom(from_entry);
      context->PutEntry(to_hash, to_entry1);
      if (from_parent != to_parent)
      {
        to_parent_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, to_parent_entry);
      }
      else
      {
        from_parent_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < from_parent_entry.dir_contents_size(); i++)
      {
        string str = from_parent_entry.dir_contents(i);
        str.erase(remove(str.begin(), str.end(), '\r'), str.end());
        if (str == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          from_parent_entry.mutable_dir_contents()
              ->SwapElements(i, from_parent_entry.dir_contents_size() - 1);
          from_parent_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, from_parent_entry);
          break;
        }
      }
      // Erase the from_entry
      context->DeleteEntry(from_hash);
    }
  }
  else if (from_type == 1 && to_type == 0)
  {                                                                          // hash to tree
    if ((from_entry.type() == DIR) && (from_entry.dir_contents_size() != 1)) // gaoxuan --only if the object we want to rename is DIR we need to loop,if its a file we don't need loop
    {
      std::queue<string> queue1; // 这个用来遍历hash
      std::queue<string> queue2; // 这个用来遍历树
      string from_root = from_hash;
      queue1.push(from_root);
      queue2.push(to_hash);
      while (!queue1.empty())
      {
        string front = queue1.front();
        queue1.pop();
        string tree_front = queue2.front();
        queue2.pop();
        // Add Entry
        MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
        MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
        context->GetEntry(front, &from_entry1);
        to_entry1.CopyFrom(from_entry1);
        context->PutEntry(tree_front, to_entry1);
        // Erase the from_entry
        context->DeleteEntry(front);
        if (from_entry1.type() == DIR)
        {
          for (int i = 1; i < from_entry1.dir_contents_size(); i++)
          {
            string str = from_entry1.dir_contents(i);
            str.erase(remove(str.begin(), str.end(), '\r'), str.end());
            string child_path = front + "/" + str;
            queue1.push(child_path);
            str = from_entry1.dir_contents(0);
            str.erase(remove(str.begin(), str.end(), '\r'), str.end());
            string tree_path = "/" + str + "/" + from_entry1.dir_contents(i);
            queue2.push(tree_path);
          }
        }
      }
      //   Update to_parent (add new dir content)
      if (from_parent != to_parent)
      {
        to_parent_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, to_parent_entry);
      }
      else
      {
        from_parent_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < from_parent_entry.dir_contents_size(); i++)
      {
        string str = from_parent_entry.dir_contents(i);
        str.erase(remove(str.begin(), str.end(), '\r'), str.end());
        if (str == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          from_parent_entry.mutable_dir_contents()
              ->SwapElements(i, from_parent_entry.dir_contents_size() - 1);
          from_parent_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, from_parent_entry);
          break;
        }
      }
    }
    else
    {
      // 原目录是个文件
      MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
      MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
      context->GetEntry(from_hash, &from_entry1);
      to_entry1.CopyFrom(from_entry1);
      context->PutEntry(to_hash, to_entry1);
      // gaoxuan --this part is used to delete the old entry
      //  Update from_parent(Find file and remove it from parent directory.)
      if (from_parent != to_parent)
      {
        to_parent_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, to_parent_entry);
      }
      else
      {
        from_parent_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < from_parent_entry.dir_contents_size(); i++)
      {
        string str = from_parent_entry.dir_contents(i);
        str.erase(remove(str.begin(), str.end(), '\r'), str.end());
        if (str == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          from_parent_entry.mutable_dir_contents()
              ->SwapElements(i, from_parent_entry.dir_contents_size() - 1);
          from_parent_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, from_parent_entry);
          break;
        }
      }
      // Erase the from_entry
      context->DeleteEntry(from_hash);
    }
  }
  else
  {                                                                          // hash to hash
    if ((from_entry.type() == DIR) && (from_entry.dir_contents_size() != 1)) // gaoxuan --only if the object we want to rename is DIR we need to loop,if its a file we don't need loop
    {
      std::queue<string> queue1; // 这个用来遍历hash
      std::queue<string> queue2; // 这个用来遍历目的位置hash
      string from_root = from_hash;
      queue1.push(from_root);
      queue2.push(to_hash);
      while (!queue1.empty())
      {
        string front = queue1.front();
        queue1.pop();
        string tree_front = queue2.front();
        queue2.pop();
        // Add Entry
        MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
        MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
        context->GetEntry(front, &from_entry1);
        to_entry1.CopyFrom(from_entry1);
        context->PutEntry(tree_front, to_entry1);
        // Erase the from_entry
        context->DeleteEntry(front);
        if (from_entry1.type() == DIR)
        {
          for (int i = 1; i < from_entry1.dir_contents_size(); i++)
          {
            string str = from_entry1.dir_contents(i);
            str.erase(remove(str.begin(), str.end(), '\r'), str.end());
            string child_path = front + "/" + str;
            queue1.push(child_path);
            string tree_path = tree_front + "/" + str;
            queue2.push(tree_path);
          }
        }
      }
      //   Update to_parent (add new dir content)
      if (from_parent != to_parent)
      {
        to_parent_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, to_parent_entry);
      }
      else
      {
        from_parent_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < from_parent_entry.dir_contents_size(); i++)
      {
        string str = from_parent_entry.dir_contents(i);
        str.erase(remove(str.begin(), str.end(), '\r'), str.end());
        if (str == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          from_parent_entry.mutable_dir_contents()
              ->SwapElements(i, from_parent_entry.dir_contents_size() - 1);
          from_parent_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, from_parent_entry);
          break;
        }
      }
    }
    else
    {
      // 原目录是个文件
      MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
      MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
      context->GetEntry(from_hash, &from_entry1);
      to_entry1.CopyFrom(from_entry1);
      context->PutEntry(to_hash, to_entry1);
      // gaoxuan --this part is used to delete the old entry
      //  Update from_parent(Find file and remove it from parent directory.)
      if (from_parent != to_parent)
      {
        to_parent_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, to_parent_entry);
      }
      else
      {
        from_parent_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < from_parent_entry.dir_contents_size(); i++)
      {
        string str = from_parent_entry.dir_contents(i);
        str.erase(remove(str.begin(), str.end(), '\r'), str.end());
        if (str == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          from_parent_entry.mutable_dir_contents()
              ->SwapElements(i, from_parent_entry.dir_contents_size() - 1);
          from_parent_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, from_parent_entry);
          break;
        }
      }
      // Erase the from_entry
      context->DeleteEntry(from_hash);
    }
  }
}

void MetadataStore::Lookup_Internal(
    ExecutionContext *context,
    const MetadataAction::LookupInput &in,
    MetadataAction::LookupOutput *out)
{
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry))
  {
    // File doesn't exist!
    LOG(ERROR) << in.path() << " can't lookup";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // TODO(agt): Check permissions.

  // Return entry.
  out->set_path(in.path());
  out->mutable_entry()->CopyFrom(entry);
}

void MetadataStore::Tree_Lookup_Internal(
    ExecutionContext *context,
    const MetadataAction::Tree_LookupInput &in,
    MetadataAction::Tree_LookupOutput *out)
{

  // 原本的遍历代码

  MetadataEntry entry;
  string path = in.path();

  if (path.find("b") != std::string::npos) // 我这里是从b开始才是分层的地方了
  {
    // 要分层
    //   gaoxuan --use BFS to add new metadata entry

    // 先把路径拆分开，根据这个b
    int p = path.find("b");
    string tree_name = path.substr(0, p - 1);
    string hash_name = path.substr(p);
    //
    string root = "";
    string root1 = "";
    // LOG(ERROR)<<"还没进入循环";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));

      if (front != "")
      {
        // gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

        // 第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用空格填充上
        // 拆分的算法，遇到一个/就把之前的字符串放进去
        // 将拆分后的元素添加去的方法：header->add_split_string(拆分的字符串)
        int flag = 0;       // 用来标识此时split_string 里面有多少子串
        char pattern = '/'; // 根据/进行字符串拆分

        string temp_from = front.c_str();
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
      }
      else
      {

        int flag = 0; // 用来标识此时split_string 里面有多少子串
        while (flag != 8)
        {
          string temp = "     ";               // 用五个空格填充一下
          header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
          flag++;                              // 拆分的字符串数量++
        }
        header->set_from_length(flag);
      }

      MessageBuffer *m = NULL;
      header->set_data_ptr(reinterpret_cast<uint64>(&m));
      machine_->SendMessage(header, new MessageBuffer());
      while (m == NULL)
      {
        usleep(10);
        Noop<MessageBuffer *>(m);
      }

      MessageBuffer *serialized = m;
      Action b;
      b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
      delete serialized;
      MetadataAction::LookupOutput out;
      out.ParseFromString(b.output());
      if (front1 == tree_name) // 判断树部分是否搜索完成
      {
        entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 0; i < out.entry().dir_contents_size(); i++)
        {

          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (in.path().find(full_path) == 0)
          { // Todo:这里需要用相对路径
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // LOG(ERROR)<<"跳出了循环";

    // 现在entry中存放的是分层点的元数据项
    hash_name = "/" + entry.dir_contents(0) + hash_name; // 获取需要hash的相对路径
    // 这个路径直接去lookup一下
    uint64 to_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(hash_name)), config_->LookupReplica(machine_->machine_id()));
    Header *header = new Header();
    header->set_from(machine_->machine_id());
    header->set_to(to_machine);
    header->set_type(Header::RPC);
    header->set_app("client");
    header->set_rpc("LOOKUP");
    header->add_misc_string(hash_name.c_str(), strlen(hash_name.c_str()));

    int flag = 0;       // 用来标识此时split_string 里面有多少子串
    char pattern = '/'; // 根据/进行字符串拆分

    string temp_from = hash_name;
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
    machine_->SendMessage(header, new MessageBuffer());
    while (m == NULL)
    {
      usleep(10);
      Noop<MessageBuffer *>(m);
    }
    MessageBuffer *serialized = m;
    Action b;
    b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
    delete serialized;
    MetadataAction::LookupOutput o;
    o.ParseFromString(b.output());
    // TODO(agt): Check permissions.
    MetadataEntry entry1 = o.entry();
    // Return entry.
    out->mutable_entry()->CopyFrom(entry1);
  }
  else
  {
    string root = "";
    string root1 = "";
    // LOG(ERROR)<<"还没进入循环";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      if (mds_machine == machine_->machine_id())
      {
        mds_machine = (mds_machine + 1) % 2;
      }
      Header *header = new Header();
      header->set_flag(2); // 标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));

      if (path != "")
      {
        int flag = 0;       // 用来标识此时split_string 里面有多少子串
        char pattern = '/'; // 根据/进行字符串拆分
        string temp_from = path.c_str();
        temp_from = temp_from.substr(1, temp_from.size()); // 这一行是为了去除最前面的/
        temp_from = temp_from + pattern;                   // 在最后面添加一个/便于处理
        int pos = temp_from.find(pattern);                 // 找到第一个/的位置
        while (pos != std::string::npos)                   // 循环不断找/，找到一个拆分一次
        {
          string temp1 = temp_from.substr(0, pos); // temp里面就是拆分出来的第一个子串
          string temp = temp1;
          for (int i = temp.size(); i < 4; i++)
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
          string temp = "    ";                // 用四个空格填充一下
          header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
          flag++;                              // 拆分的字符串数量++
        }

        // 这一行之前是gaoxuan添加的
      }
      else
      {
        int flag = 0; // 用来标识此时split_string 里面有多少子串
        while (flag != 8)
        {
          string temp = "    ";                // 用四个空格填充一下
          header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
          flag++;                              // 拆分的字符串数量++
        }
        header->set_from_length(flag);
      }
      int depth = Dir_depth(path);
      header->set_depth(depth);
      int uid = 9999;
      header->set_uid(uid);
      MessageBuffer *m = NULL;
      header->set_data_ptr(reinterpret_cast<uint64>(&m));
      machine_->SendMessage(header, new MessageBuffer());
      while (m == NULL)
      {
        usleep(10);
        Noop<MessageBuffer *>(m);
      }

      MessageBuffer *serialized = m;
      Action b;
      b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
      delete serialized;

      if (b.input() == "switch processed")
      {

        entry.set_type(DIR);
        entry.add_dir_contents("gaoxuan");

        break;
      }
      MetadataAction::LookupOutput out;
      out.ParseFromString(b.output());
      if (front1 == in.path()) // 单独用全路径来判断是否搜索完成,可以肯定是这里没执行，才退不出去
      {
        entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 0; i < out.entry().dir_contents_size(); i++)
        {
          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径
          if (in.path().find(full_path) == 0)
          { // Todo:这里需要用相对路径
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    out->mutable_entry()->CopyFrom(entry);
  }

  /*
    MetadataEntry entry;
    string path = in.path();
    string full_path = in.path();
    string front = path;
    uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
    if(mds_machine == machine_->machine_id())
    {
      mds_machine = (mds_machine+1)%2;
    }
    //LOG(ERROR) << path <<"LS: "<<machine_->machine_id() << " to " << mds_machine;
    Header *header = new Header();
    header->set_flag(2); // 标识
    header->set_from(machine_->machine_id());
    header->set_to(mds_machine);
    header->set_type(Header::RPC);
    header->set_app("client");
    header->set_rpc("LOOKUP");
    header->add_misc_string(front.c_str(), strlen(front.c_str()));
    // firstly, we need split full path
    if (full_path != "")
    {
      int flag = 0;       // 用来标识此时split_string 里面有多少子串
      char pattern = '/'; // 根据/进行字符串拆分
      string temp_from = full_path;
      temp_from = temp_from.substr(1, temp_from.size()); // 这一行是为了去除最前面的/
      temp_from = temp_from + pattern;                   // 在最后面添加一个/便于处理
      int pos = temp_from.find(pattern);                 // 找到第一个/的位置
      while (pos != std::string::npos)                   // 循环不断找/，找到一个拆分一次
      {
        string temp1 = temp_from.substr(0, pos); // temp里面就是拆分出来的第一个子串
        string temp = temp1;
        for (int i = temp.size(); i < 4; i++)
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
        string temp = "    ";                // 用四个空格填充一下
        header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
        flag++;                              // 拆分的字符串数量++
      }
    }
    else
    {
      int flag = 0; // 用来标识此时split_string 里面有多少子串
      while (flag != 8)
      {
        string temp = "    ";               // 用四个空格填充一下
        header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
        flag++;                              // 拆分的字符串数量++
      }
      header->set_from_length(flag);
    }
    // secondly, we need to add depth of full_path;
    // need a little function :Dir_depth();
    int depth = Dir_depth(full_path);
    header->set_depth(depth);
    // uid which need to be added in switch
    int uid = 9999;
    header->set_uid(uid);
    // metadataentry part , using " " to fill up
    string empty_str = "0000000000000000";
    for (int i = 0; i < 8; i++)
    {
      header->add_metadatentry(empty_str);
    }
    MessageBuffer *m = NULL;
    header->set_data_ptr(reinterpret_cast<uint64>(&m));
    machine_->SendMessage(header, new MessageBuffer());
    while (m == NULL)
    {
      usleep(10);
      Noop<MessageBuffer *>(m);
    }
    MessageBuffer *serialized = m;
    Action b;
    b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
    delete serialized;

    if(b.input() == "switch processed")
    {

      entry.set_type(DIR);
      entry.add_dir_contents("gaoxuan");
      //LOG(ERROR) << "switch modified! opreation finished!";
    }
    else
    {
      MetadataAction::LookupOutput out;
      out.ParseFromString(b.output());
      entry = out.entry();
    }
    out->mutable_entry()->CopyFrom(entry);

  */
}
void MetadataStore::Resize_Internal(
    ExecutionContext *context,
    const MetadataAction::ResizeInput &in,
    MetadataAction::ResizeOutput *out)
{
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry))
  {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // Only resize DATA files.
  if (entry.type() != DATA)
  {
    out->set_success(false);
    out->add_errors(MetadataAction::WrongFileType);
    return;
  }

  // TODO(agt): Check permissions.

  // If we're resizing to size 0, just clear all file part.
  if (in.size() == 0)
  {
    entry.clear_file_parts();
    return;
  }

  // Truncate/remove entries that go past the target size.
  uint64 total = 0;
  for (int i = 0; i < entry.file_parts_size(); i++)
  {
    total += entry.file_parts(i).length();
    if (total >= in.size())
    {
      // Discard all following file parts.
      entry.mutable_file_parts()->DeleteSubrange(
          i + 1,
          entry.file_parts_size() - i - 1);

      // Truncate current file part by (total - in.size()) bytes.
      entry.mutable_file_parts(i)->set_length(
          entry.file_parts(i).length() - (total - in.size()));
      break;
    }
  }

  // If resize operation INCREASES file size, append a new default file part.
  if (total < in.size())
  {
    entry.add_file_parts()->set_length(in.size() - total);
  }

  // Write out new version of entry.
  context->PutEntry(in.path(), entry);
}

void MetadataStore::Write_Internal(
    ExecutionContext *context,
    const MetadataAction::WriteInput &in,
    MetadataAction::WriteOutput *out)
{
  LOG(FATAL) << "not implemented";
}

void MetadataStore::Append_Internal(
    ExecutionContext *context,
    const MetadataAction::AppendInput &in,
    MetadataAction::AppendOutput *out)
{
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry))
  {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // Only append to DATA files.
  if (entry.type() != DATA)
  {
    out->set_success(false);
    out->add_errors(MetadataAction::WrongFileType);
    return;
  }

  // TODO(agt): Check permissions.

  // Append data to end of file.
  for (int i = 0; i < in.data_size(); i++)
  {
    entry.add_file_parts()->CopyFrom(in.data(i));
  }

  // Write out new version of entry.
  context->PutEntry(in.path(), entry);
}

void MetadataStore::ChangePermissions_Internal(
    ExecutionContext *context,
    const MetadataAction::ChangePermissionsInput &in,
    MetadataAction::ChangePermissionsOutput *out)
{
  LOG(FATAL) << "not implemented";
}
