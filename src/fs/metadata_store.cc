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
{ // 输出整棵树
  // 如果要使用这个函数，需要将GetMetadataentry这个函数里面拆分屏蔽掉，因为大于八层
  string root = "";
  string root1 = "";
  std::queue<string> queue1;
  queue1.push(root);
  std::queue<string> queue2;
  queue2.push(root1);
  while (!queue1.empty())
  {
    string front = queue1.front();
    string front1 = queue2.front();
    queue1.pop();
    queue2.pop();
    LOG(ERROR) << front1;
    uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
    Header *header = new Header();
    header->set_flag(2);//标识
    header->set_from(machine_->machine_id());
    header->set_to(mds_machine);
    header->set_type(Header::RPC);
    header->set_app("client");
    header->set_rpc("LOOKUP");
    header->add_misc_string(front.c_str(), strlen(front.c_str()));
    /*
    //为了输出大于八层的树，暂时先屏蔽掉拆分
        // 下面是路径拆分
        if (front != "")
        {
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

          // 这一行之前是gaoxuan添加的
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
    */

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
    if (front1.find("b") == std::string::npos)
    { // 这是树部分
      for (int i = 1; i < out.entry().dir_contents_size(); i++)
      {
        string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

        root1 = full_path;
        root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
        queue1.push(root);
        queue2.push(root1);
      }
    }
    else
    {
      for (int i = 1; i < out.entry().dir_contents_size(); i++)
      {
        string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

        root1 = full_path;
        root = front + "/" + out.entry().dir_contents(i);
        queue1.push(root);
        queue2.push(root1);
      }
    }
  }
  LOG(ERROR) << "finished LOOKUP";
}
void MetadataStore::GetRWSets(Action *action)
{ // gaoxuan --this function is called by RameFile() for RenameExperiment
  action->clear_readset();
  action->clear_writeset();

  MetadataAction::Type type =
      static_cast<MetadataAction::Type>(action->action_type());

  if (type == MetadataAction::CREATE_FILE)
  {
    MetadataAction::CreateFileInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
    action->add_readset(ParentDir(in.path()));
    action->add_writeset(ParentDir(in.path()));
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

    if (in.from_path().find("b") == std::string::npos)
    {
      if (in.to_path().find("b") == std::string::npos)
      {
        // 这一部分是从树rename到树的过程
        //  1.1 找到原位置的父目录以及爷爷目录
        MetadataEntry Parent_from_entry;
        MetadataEntry PParent_from_entry;
        string parent_from_path = ParentDir(from_path);
        string root = "";
        string root1 = "";
        while (1)
        {
          string front = root;
          string front1 = root1;
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
          // 下面是路径拆分
          if (front != "")
          {
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

            // 这一行之前是gaoxuan添加的
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
          if (front1 == ParentDir(parent_from_path)) // 找到了from_path父目录的父目录的元数据项
          {
            PParent_from_entry = out.entry(); // 这里爷爷目录用来确定父目录的
          }
          if (front1 == parent_from_path) // 找到了from_path父目录的元数据项
          {
            Parent_from_entry = out.entry();
            break;
          }
          else
          { // gaoxuan --还没有找到
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {
              string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

              if (parent_from_path.find(full_path) == 0)
              {
                // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
                root1 = full_path;
                root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
                break;
              }
            }
          }
        }
        // PParent_from_entry中存放原位置的爷爷的元数据项，Parent_from_entry存放父亲的元数据项

        // 1.1 找到目的位置的父目录以及父父目录
        MetadataEntry Parent_to_entry;
        MetadataEntry PParent_to_entry;
        string parent_to_path = ParentDir(in.to_path());
        root = "";
        root1 = "";
        // LOG(ERROR)<<"还没进入循环";
        while (1)
        {
          string front = root;
          string front1 = root1;
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
          // 下面是路径拆分
          if (front != "")
          {
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

            // 这一行之前是gaoxuan添加的
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
          if (front1 == ParentDir(parent_to_path)) // 找到了to_path父目录的父目录的元数据项
          {
            PParent_to_entry = out.entry(); // 这里爷爷目录用来确定父目录的
          }
          if (front1 == parent_to_path) // 找到了from_path父目录的元数据项
          {
            Parent_to_entry = out.entry();
            break;
          }
          else
          { // gaoxuan --还没有找到
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {
              string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

              if (parent_to_path.find(full_path) == 0)
              {
                // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
                root1 = full_path;
                root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
                break;
              }
            }
          }
        }
        // PParent_to_entry中存放目的位置的爷爷的元数据项，Parent_to_entry存放父亲的元数据项

        // 2、将父目录的元数据的键加入读写集
        string from_parent = "/" + PParent_from_entry.dir_contents(0) + FileName(parent_from_path);
        string to_parent = "/" + PParent_to_entry.dir_contents(0) + FileName(parent_to_path);
        if (parent_from_path == parent_to_path) // 父目录相同
        {
          action->add_readset(from_parent);
          action->add_writeset(to_parent);
        }
        else
        {
          action->add_readset(from_parent);
          action->add_writeset(from_parent);

          action->add_readset(to_parent);
          action->add_writeset(to_parent);
        }
        // 3、用广度优先遍历，将原位置向下所有子目录都做路径解析拼接，添加读写集

        string origin_path = "/" + Parent_from_entry.dir_contents(0) + FileName(from_path);
        string desti_path = "/" + Parent_to_entry.dir_contents(0) + FileName(to_path);
        action->add_writeset(desti_path); // gaoxuan --这里是我觉得涉及分层的一个不同点，如果是树，那么rename的目的地只需要加入目的的父亲和目的路径这两个即可
                                          // 也就是说这里目的地的写集只需要加目的路径这一个路径即可（分层点之下则不同，需要对之下都要修改）
                                          // 做法
        std::queue<string> queue1;
        string from_root = origin_path;
        queue1.push(from_root);
        while (!queue1.empty())
        {
          string front = queue1.front();
          queue1.pop();
          // 一系列操作
          // 给from这个子树节点添加读写集
          action->add_readset(front);
          action->add_writeset(front);
          // 下面是获取这个路径的元数据项的过程
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app(getAPPname());
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
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
            string temp = "     ";               // 用空格填充一下
            header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
            flag++;                              // 拆分的字符串数量++
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

          if (front.find("b") == std::string::npos)
          {
            // 不是分层点
            if (out.success() && out.entry().type() == DIR)
            {
              string uid = out.entry().dir_contents(0);
              for (int i = 1; i < out.entry().dir_contents_size(); i++)
              {
                string child_path = "/" + uid + out.entry().dir_contents(i);
                queue1.push(child_path);
              }
            }
          }
          else
          {
            if (out.success() && out.entry().type() == DIR)
            {
              // 相比树状，也就是路径加入方式换一下
              for (int i = 1; i < out.entry().dir_contents_size(); i++)
              {
                string child_path = front + "/" + out.entry().dir_contents(i);
                queue1.push(child_path);
              }
            }
          }
        }
        // 这之前是广度优先遍历的代码
      }
      else
      {
        // 上面是从树rename到树,这里是树rename到hash
        // 相比树到树，读写集的变化如下
        /*
        1、搜索原目录的父目录没有变化
        2、搜索目的目录的父目录需要先搜索到分层点，然后再用hash拿
        3、将父目录放入读写集没什么变化
        4、这里遍历添加子树进入读写集有很大不同
          从原目录位置开始向下层次遍历，树这边和以前一样，获得一个id，就将其和其他元数据项拼接放入读写集
          目的位置，则需要使用目的位置的元数据项，和子树这边拿的路径做个拼接，加入写集（这一点是最大区别，好好思考再写）
        */
        // 1、原目录及其父目录搜索
        MetadataEntry Parent_from_entry;
        MetadataEntry PParent_from_entry;
        string parent_from_path = ParentDir(in.from_path());
        string root = "";
        string root1 = "";
        while (1)
        {
          string front = root;
          string front1 = root1;
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
          // 下面是路径拆分
          if (front != "")
          {
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

            // 这一行之前是gaoxuan添加的
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
          if (front1 == ParentDir(parent_from_path)) // 找到了from_path父目录的父目录的元数据项
          {
            PParent_from_entry = out.entry(); // 这里爷爷目录用来确定父目录的
          }
          if (front1 == parent_from_path) // 找到了from_path父目录的元数据项
          {
            Parent_from_entry = out.entry();
            break;
          }
          else
          { // gaoxuan --还没有找到
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {
              string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

              if (parent_from_path.find(full_path) == 0)
              {
                // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
                root1 = full_path;
                root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
                break;
              }
            }
          }
        }
        // PParent_from_entry中存放原位置的爷爷的元数据项，Parent_from_entry存放父亲的元数据项

        // 2、目的位置，先找到分层点，再确定父目录
        int p1 = to_path.find("b");
        string tree_name1 = to_path.substr(0, p1 - 1);
        string hash_name1 = to_path.substr(p1);
        //
        root = "";
        root1 = "";
        MetadataEntry to_split_entry;
        while (1)
        {
          string front = root;
          string front1 = root1;
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));

          if (front != "")
          {
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
          if (front1 == tree_name1) // 判断树部分是否搜索完成
          {
            to_split_entry = out.entry();
            break;
          }
          else
          { // gaoxuan --还没有找到
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {

              string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

              if (to_path.find(full_path) == 0)
              { // Todo:这里需要用相对路径
                // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
                root1 = full_path;
                root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
                break;
              }
            }
          }
        }
        // 目的位置的分层点元数据项就在to_split_entry这个里面

        // 下面获得原位置的父目录信息
        string from_parent = "/" + PParent_from_entry.dir_contents(0) + FileName(parent_from_path);

        // 下面获得目的位置的父目录信息
        string to_uid = to_split_entry.dir_contents(0);
        string to_parent = ParentDir(to_path);
        int pos_to_parent = to_parent.find('b');
        to_parent = "/" + to_uid + to_parent.substr(pos_to_parent);

        // 将父目录放入读写集
        if (from_parent == to_parent) // 父目录相同
        {
          action->add_readset(from_parent);
          action->add_writeset(to_parent);
        }
        else
        {
          action->add_readset(from_parent);
          action->add_writeset(from_parent);

          action->add_readset(to_parent);
          action->add_writeset(to_parent);
        }

        // 下面是从原位置开始向下层次遍历，唯一要注意的点就是确定目的位置的写集路径
        string origin_path = "/" + Parent_from_entry.dir_contents(0) + FileName(from_path);
        string desti_path = "/" + to_uid + to_path.substr(p1);
        // 先把目的目录加入写集
        // action->add_writeset(desti_path);

        std::queue<string> queue1; // 这个用来遍历树
        std::queue<string> queue2; // 这个用来遍历hash
        string from_root = origin_path;
        queue1.push(from_root);
        queue2.push(desti_path);
        while (!queue1.empty())
        {
          string front = queue1.front();
          queue1.pop();
          string writeset = queue2.front();
          queue2.pop();
          // 一系列操作
          // 给from这个子树节点添加读写集
          action->add_readset(front);
          action->add_writeset(front);
          action->add_writeset(writeset);
          // 这里需要一点修改，怎么添加写集
          // 想法来了，在这个遍历里面，对树的也弄一个全路径？，不行，这必须再弄一个队列，感觉不可行

          // 下面是获取这个路径的元数据项的过程
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app(getAPPname());
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
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
            string temp = "     ";               // 用空格填充一下
            header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
            flag++;                              // 拆分的字符串数量++
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
          if (front.find("b") == std::string::npos)
          { // 从树一直向下搜，目前还是树
            // 不是分层点
            if (out.success() && out.entry().type() == DIR)
            {
              string uid = out.entry().dir_contents(0);
              for (int i = 1; i < out.entry().dir_contents_size(); i++)
              {
                string child_path = "/" + uid + out.entry().dir_contents(i);     // 树的遍历路径
                string full_path = writeset + "/" + out.entry().dir_contents(i); // 目的地hash的拼接路径
                queue1.push(child_path);
                queue2.push(full_path);
              }
            }
          }
          else
          {
            if (out.success() && out.entry().type() == DIR)
            {
              // 搜到了hash位置，那现在front里面已经是 /uid+bx,只需要在后面继续拼接，不再需要uid了
              //  相比树状，也就是路径加入方式换一下
              for (int i = 1; i < out.entry().dir_contents_size(); i++)
              {
                string child_path = front + "/" + out.entry().dir_contents(i);
                string full_path = writeset + "/" + out.entry().dir_contents(i);
                queue1.push(child_path);
                queue2.push(full_path);
              }
            }
          }
        }
      }
    }
    else
    {

      if (in.to_path().find("b") != std::string::npos)
      {
        // 这部分是需要分层的代码
        // 1.1搜到原位置的分层点
        int p = from_path.find("b");
        string tree_name = from_path.substr(0, p - 1);
        string hash_name = from_path.substr(p);
        //
        string root = "";
        string root1 = "";
        MetadataEntry from_split_entry;
        // LOG(ERROR)<<"还没进入循环";
        while (1)
        {
          string front = root;
          string front1 = root1;
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));

          if (front != "")
          {
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
            from_split_entry = out.entry();
            break;
          }
          else
          { // gaoxuan --还没有找到
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {

              string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

              if (from_path.find(full_path) == 0)
              { // Todo:这里需要用相对路径
                // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
                root1 = full_path;
                root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
                break;
              }
            }
          }
        }
        // 原位置的分层点元数据项就在from_split_entry这个里面
        // 1.2搜到目的位置的分层点
        int p1 = to_path.find("b");
        string tree_name1 = to_path.substr(0, p1 - 1);
        string hash_name1 = to_path.substr(p1);
        //
        root = "";
        root1 = "";
        MetadataEntry to_split_entry;
        // LOG(ERROR)<<"还没进入循环";
        while (1)
        {
          string front = root;
          string front1 = root1;
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));

          if (front != "")
          {
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
          if (front1 == tree_name1) // 判断树部分是否搜索完成
          {
            to_split_entry = out.entry();
            break;
          }
          else
          { // gaoxuan --还没有找到
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {

              string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

              if (to_path.find(full_path) == 0)
              { // Todo:这里需要用相对路径
                // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
                root1 = full_path;
                root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
                break;
              }
            }
          }
        }
        // 目的位置的分层点元数据项就在to_split_entry这个里面

        string from_uid = from_split_entry.dir_contents(0);
        string to_uid = to_split_entry.dir_contents(0); // 注意注意，这两个id很大可能相同嗷，别大意，到时候测试也是要基于这一点再思考
        // 下面要获得父目录
        string from_parent = ParentDir(from_path);
        string to_parent = ParentDir(to_path);
        int pos_from_parent = from_parent.find('b');
        int pos_to_parent = to_parent.find('b');

        from_parent = "/" + from_uid + from_parent.substr(pos_from_parent);
        to_parent = "/" + to_uid + to_parent.substr(pos_to_parent);
        if (from_parent == to_parent)
        {
          action->add_readset(from_parent);
          action->add_writeset(to_parent);
        }
        else
        {
          action->add_readset(from_parent);
          action->add_writeset(from_parent);

          action->add_readset(to_parent);
          action->add_writeset(to_parent);
        }

        // 这里和纯树状的存在一些不同之处，因为要将所有的目的位置的孩子同样是要加入写集
        string origin_path = "/" + from_uid + from_path.substr(p);
        string desti_path = "/" + to_uid + to_path.substr(p1);
        string To_path = desti_path;
        std::queue<string> queue1;
        string from_root = origin_path;
        queue1.push(from_root);
        while (!queue1.empty())
        {
          string front = queue1.front();
          queue1.pop();
          // 一系列操作
          // 给from这个子树节点添加读写集
          action->add_readset(front);
          action->add_writeset(front);
          string s = front.substr(origin_path.size());
          To_path = desti_path + s;
          action->add_writeset(To_path);
          // 下面是获取这个路径的元数据项的过程
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app(getAPPname());
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
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
            string temp = "     ";               // 用空格填充一下
            header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
            flag++;                              // 拆分的字符串数量++
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
          // 上面是获取这个路径的元数据项的过程
          // 当前路径的元数据项就在out.entry()里面
          // 将这一层的元数据项都拼接一下放入队列
          if (out.success() && out.entry().type() == DIR)
          {
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {
              string child_path = front + "/" + out.entry().dir_contents(i);
              queue1.push(child_path);
            }
          }
        }
      }
      else
      {
        // 上面是hash到hash  ，这里是hash到树
        // 和上面hash到hash的区别
        /*
        1、搜索父目录这个过程，和上面树到hash换个位置就行，然后添加进入父目录
        2、遍历添加子树进入读写集
          2.1 对于原位置，因为是hash，所以放入读写集的应该是拼接的相对路径
          2.2 目的位置，因为是树，所以添加写集的时候，还需要做更改，重新弄成id+path加入写集

        */

        // 原位置是hash，先搜到分层点，再
        // 1.1搜到原位置的分层点
        int p = from_path.find("b");
        string tree_name = from_path.substr(0, p - 1);
        string hash_name = from_path.substr(p);
        //
        string root = "";
        string root1 = "";
        MetadataEntry from_split_entry;
        // LOG(ERROR)<<"还没进入循环";
        while (1)
        {
          string front = root;
          string front1 = root1;
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));

          if (front != "")
          {
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
            from_split_entry = out.entry();
            break;
          }
          else
          { // gaoxuan --还没有找到
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {

              string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

              if (from_path.find(full_path) == 0)
              { // Todo:这里需要用相对路径
                // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
                root1 = full_path;
                root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
                break;
              }
            }
          }
        }
        // 原位置的分层点元数据项就在from_split_entry这个里面

        // 搜索目的位置的父目录
        // 1.1 找到目的位置的父目录以及父父目录
        MetadataEntry Parent_to_entry;
        MetadataEntry PParent_to_entry;
        string parent_to_path = ParentDir(in.to_path());
        root = "";
        root1 = "";
        // LOG(ERROR)<<"还没进入循环";
        while (1)
        {
          string front = root;
          string front1 = root1;
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app("client");
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
          // 下面是路径拆分
          if (front != "")
          {
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

            // 这一行之前是gaoxuan添加的
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
          if (front1 == ParentDir(parent_to_path)) // 找到了to_path父目录的父目录的元数据项
          {
            PParent_to_entry = out.entry(); // 这里爷爷目录用来确定父目录的
          }
          if (front1 == parent_to_path) // 找到了from_path父目录的元数据项
          {
            Parent_to_entry = out.entry();
            break;
          }
          else
          { // gaoxuan --还没有找到
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {
              string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

              if (parent_to_path.find(full_path) == 0)
              {
                // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
                root1 = full_path;
                root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
                break;
              }
            }
          }
        }
        // PParent_to_entry中存放目的位置的爷爷的元数据项，Parent_to_entry存放父亲的元数据项

        // PParent_to_entry中存放目的位置的爷爷的元数据项，Parent_to_entry存放父亲的元数据项
        // 获得原位置的父目录
        string from_uid = from_split_entry.dir_contents(0);
        string from_parent = ParentDir(from_path);
        int pos_from_parent = from_parent.find('b');
        from_parent = "/" + from_uid + from_parent.substr(pos_from_parent);

        // 获得目的位置的父目录
        string to_parent = "/" + PParent_to_entry.dir_contents(0) + FileName(parent_to_path);
        if (from_parent == to_parent) // 父目录相同
        {
          action->add_readset(from_parent);
          action->add_writeset(to_parent);
        }
        else
        {
          action->add_readset(from_parent);
          action->add_writeset(from_parent);

          action->add_readset(to_parent);
          action->add_writeset(to_parent);
        }

        // 下面是要将原本的hash改成树的部分
        // 这个的难点是什么；对原位置之下的子树都是直接用相对路径，
        // 但是这里我们还是要对子树，得到uid，和后面拼接得到树状的？是的，这也是必须的，因为还是会出现冲突的可能
        // 在里面改的还是一个路径拼接修改的过程
        // ！因为我们这里是根据b这一层来判断是不是分层点，那这里只要我们rename b之下的层就暂时没错
        string origin_path = "/" + from_uid + from_path.substr(p);
        string desti_path = "/" + Parent_to_entry.dir_contents(0) + FileName(to_path);
        string To_path = desti_path;
        std::queue<string> queue1;
        std::queue<string> queue2; // 这个用来遍历目的树

        string from_root = origin_path;
        queue1.push(origin_path);
        queue2.push(desti_path);
        while (!queue1.empty())
        {
          string front = queue1.front();
          queue1.pop();
          string writeset = queue2.front();
          queue2.pop();
          // 一系列操作
          // 给from这个子树节点添加读写集
          action->add_readset(front);
          action->add_writeset(front);
          // 这块没写，To_path怎么更改的
          action->add_writeset(writeset);
          // 下面是获取这个路径的元数据项的过程
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
          Header *header = new Header();
          header->set_flag(2);//标识
          header->set_from(machine_->machine_id());
          header->set_to(mds_machine);
          header->set_type(Header::RPC);
          header->set_app(getAPPname());
          header->set_rpc("LOOKUP");
          header->add_misc_string(front.c_str(), strlen(front.c_str()));
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
            string temp = "     ";               // 用空格填充一下
            header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
            flag++;                              // 拆分的字符串数量++
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
          if (out.success() && out.entry().type() == DIR)
          {
            for (int i = 1; i < out.entry().dir_contents_size(); i++)
            {
              string child_path = front + "/" + out.entry().dir_contents(i);
              queue1.push(child_path);
              string tree_path = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
              queue2.push(tree_path);
            }
          }
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
    CreateFile_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
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
    // LOG(ERROR)<<"In Run :: "<<in.from_path()<<" and "<<in.to_path();
    Rename_Internal(context, in, &out);
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
    MetadataAction::CreateFileOutput *out)
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

  // TODO(agt): Check permissions.

  // If file already exists, fail.
  // TODO(agt): Look up file directly instead of looking through parent dir?
  string filename = FileName(in.path());
  for (int i = 0; i < parent_entry.dir_contents_size(); i++)
  {
    if (parent_entry.dir_contents(i) == filename)
    {
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }

  // Update parent.
  // TODO(agt): Keep dir_contents sorted?
  parent_entry.add_dir_contents(filename);
  context->PutEntry(parent_path, parent_entry);

  // Add entry.
  MetadataEntry entry;
  entry.mutable_permissions()->CopyFrom(in.permissions());
  entry.set_type(in.type());
  context->PutEntry(in.path(), entry);
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
    MetadataAction::RenameOutput *out)
{
  string from_path = in.from_path();
  string to_path = in.to_path();

  // TODO1:树到树，写出来，测一下，顺便改一下读写集可能的隐含bug
  //
  if (from_path.find("b") == std::string::npos && to_path.find("b") == std::string::npos)
  {
    // 3.1 找到源和目的的两个位置，还是从""开始向下搜索
    //   1、首先要用循环，找到源和目的两个位置的父目录的和父亲的父亲的元数据项（因为父目录也要加读写集）
    //   1.1 找到原位置的父目录，以及父父目录
    MetadataEntry Parent_from_entry;
    MetadataEntry PParent_from_entry;
    string parent_from_path = ParentDir(in.from_path());
    string root = "";
    string root1 = "";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));
      // 下面是路径拆分
      if (front != "")
      {
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

        // 这一行之前是gaoxuan添加的
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
      if (front1 == ParentDir(parent_from_path)) // 找到了from_path父目录的父目录的元数据项
      {
        PParent_from_entry = out.entry(); // 这里爷爷目录用来确定父目录的
      }
      if (front1 == parent_from_path) // 找到了from_path父目录的元数据项
      {
        Parent_from_entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 1; i < out.entry().dir_contents_size(); i++)
        {
          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (parent_from_path.find(full_path) == 0)
          {
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // PParent_from_entry中存放原位置的爷爷的元数据项，Parent_from_entry存放父亲的元数据项

    // 1.1 找到目的位置的父目录以及父父目录
    MetadataEntry Parent_to_entry;
    MetadataEntry PParent_to_entry;
    string parent_to_path = ParentDir(in.to_path());
    root = "";
    root1 = "";
    // LOG(ERROR)<<"还没进入循环";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));
      // 下面是路径拆分
      if (front != "")
      {
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

        // 这一行之前是gaoxuan添加的
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
      if (front1 == ParentDir(parent_to_path)) // 找到了to_path父目录的父目录的元数据项
      {
        PParent_to_entry = out.entry(); // 这里爷爷目录用来确定父目录的
      }
      if (front1 == parent_to_path) // 找到了from_path父目录的元数据项
      {
        Parent_to_entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 1; i < out.entry().dir_contents_size(); i++)
        {
          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (parent_to_path.find(full_path) == 0)
          {
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // PParent_to_entry中存放目的位置的爷爷的元数据项，Parent_to_entry存放父亲的元数据项
    // 下面两个是获取父目录元数据项的路径
    string from_parent = "/" + PParent_from_entry.dir_contents(0) + FileName(parent_from_path);
    string to_parent = "/" + PParent_to_entry.dir_contents(0) + FileName(parent_to_path);
    MetadataEntry parent_from_entry;
    if (!context->GetEntry(from_parent, &parent_from_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "From_Parent File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    MetadataEntry parent_to_entry;
    if (!context->GetEntry(to_parent, &parent_to_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "To_Parent File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    // 注意在这里，如果父目录完全一样，parent_from_entry和parent_to_entry是一样的
    //  现在原位置和目的位置的父目录的元数据项都拿到了
    //  判断目的路径的父目录下是不是重复
    string to_filename = FileName(in.to_path());
    for (int i = 1; i < parent_to_entry.dir_contents_size(); i++)
    { // 这一步不会有问题，
      if (parent_to_entry.dir_contents(i) == to_filename)
      {
        LOG(ERROR) << "file already exists, fail.";
        out->set_success(false);
        out->add_errors(MetadataAction::FileAlreadyExists);
        return;
      }
    }
    if (from_parent != to_parent)
    {
      // 目的父目录添加到最后的元数据项
      // 所以问题出现在这一步了，写回没写回去，原因呢
      parent_to_entry.add_dir_contents(to_filename);
      context->PutEntry(to_parent, parent_to_entry);
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < parent_from_entry.dir_contents_size(); i++)
      { // 这一步正常进行了，反而是上面一步没正常进行，猜测是这一步给上一步覆盖了，那么就区分开
        if (parent_from_entry.dir_contents(i) == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          parent_from_entry.mutable_dir_contents()
              ->SwapElements(i, parent_from_entry.dir_contents_size() - 1);
          parent_from_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, parent_from_entry);
          break;
        }
      }
    }
    else
    {

      parent_from_entry.add_dir_contents(to_filename);
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < parent_from_entry.dir_contents_size(); i++)
      { // 这一步正常进行了，反而是上面一步没正常进行，猜测是这一步给上一步覆盖了，那么就区分开
        if (parent_from_entry.dir_contents(i) == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          parent_from_entry.mutable_dir_contents()
              ->SwapElements(i, parent_from_entry.dir_contents_size() - 1);
          parent_from_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, parent_from_entry);
          break;
        }
      }
    }

    // 将原位置的拷贝过来，创建新的目的目录的元数据项
    string origin_path = "/" + Parent_from_entry.dir_contents(0) + FileName(from_path);
    string desti_path = "/" + Parent_to_entry.dir_contents(0) + FileName(to_path);
    MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
    MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
    if (!context->GetEntry(origin_path, &from_entry1))
    {
      // File doesn't exist!
      LOG(ERROR) << "Original File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    to_entry1.CopyFrom(from_entry1);
    context->PutEntry(desti_path, to_entry1);

    // 删除原位置元数据项
    context->DeleteEntry(origin_path);
  }
  else if (from_path.find("b") != std::string::npos && to_path.find("b") != std::string::npos)
  { // TODO2：hash到hash
    // 1.1搜到原位置的分层点
    int p = from_path.find("b");
    string tree_name = from_path.substr(0, p - 1);
    string hash_name = from_path.substr(p);
    //
    string root = "";
    string root1 = "";
    MetadataEntry from_split_entry;
    // LOG(ERROR)<<"还没进入循环";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));

      if (front != "")
      {
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
        from_split_entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 1; i < out.entry().dir_contents_size(); i++)
        {

          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (from_path.find(full_path) == 0)
          { // Todo:这里需要用相对路径
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // 原位置的分层点元数据项就在from_split_entry这个里面
    // 1.2搜到目的位置的分层点
    int p1 = to_path.find("b");
    string tree_name1 = to_path.substr(0, p1 - 1);
    string hash_name1 = to_path.substr(p1);
    //
    root = "";
    root1 = "";
    MetadataEntry to_split_entry;
    // LOG(ERROR)<<"还没进入循环";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));

      if (front != "")
      {
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
      if (front1 == tree_name1) // 判断树部分是否搜索完成
      {
        to_split_entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 1; i < out.entry().dir_contents_size(); i++)
        {

          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (to_path.find(full_path) == 0)
          { // Todo:这里需要用相对路径
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // 目的位置的分层点元数据项就在to_split_entry这个里面

    string from_uid = from_split_entry.dir_contents(0);
    string to_uid = to_split_entry.dir_contents(0);
    // 上面就找到了原位置和目的位置的分层点uid了

    // 获取原位置和目的位置的父目录的元数据项
    // 获得路径
    string from_parent = ParentDir(from_path);
    string to_parent = ParentDir(to_path);
    int pos_from_parent = from_parent.find('b');
    int pos_to_parent = to_parent.find('b');

    from_parent = "/" + from_uid + from_parent.substr(pos_from_parent);
    to_parent = "/" + to_uid + to_parent.substr(pos_to_parent);
    // 取出父目录的元数据项
    MetadataEntry parent_from_entry;
    if (!context->GetEntry(from_parent, &parent_from_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "From_Parent File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    MetadataEntry parent_to_entry;
    if (!context->GetEntry(to_parent, &parent_to_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "To_Parent File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    // If file already exists, fail.
    // gaoxuan --check if exist a file with same name in the Parent dir of to_path
    string to_filename = FileName(in.to_path());
    for (int i = 1; i < parent_to_entry.dir_contents_size(); i++)
    {
      if (parent_to_entry.dir_contents(i) == to_filename)
      {
        LOG(ERROR) << "file already exists, fail.";
        out->set_success(false);
        out->add_errors(MetadataAction::FileAlreadyExists);
        return;
      }
    }
    // gaoxuan --the part above is used to check if we can rename
    // gaoxuan --in the following part we should change it to a loop
    //  Update to_parent (add new dir content)
    // 这块可能有错误
    int flag = 0; // 用来标记父目录是不是会一样
    if (to_parent != from_parent)
    {
      parent_to_entry.add_dir_contents(to_filename);
      context->PutEntry(to_parent, parent_to_entry);
    }
    else
    {
      flag = true;
    }

    string origin_path = "/" + from_uid + from_path.substr(p);
    string desti_path = "/" + to_uid + to_path.substr(p1);
    MetadataEntry from_entry;
    if (!context->GetEntry(origin_path, &from_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }

    // 下面开始遍历
    if ((from_entry.type() == DIR) && (from_entry.dir_contents_size() != 0)) // gaoxuan --only if the object we want to rename is DIR we need to loop,if its a file we don't need loop
    {
      // gaoxuan --use BFS to add new metadata entry
      std::queue<string> queue1;
      string root = desti_path;
      queue1.push(root);
      string from_path = origin_path; // gaoxuan --the path used to copyEntry
      while (!queue1.empty())
      {
        string front = queue1.front(); // gaoxuan --get the front in queue
        queue1.pop();
        // Add Entry
        MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
        MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
        context->GetEntry(from_path, &from_entry1);
        to_entry1.CopyFrom(from_entry1);
        context->PutEntry(front, to_entry1);
        // gaoxuan --this part is used to delete the old entry
        //  Update from_parent(Find file and remove it from parent directory.)
        // 注意这里可能有个bug，rename分层第一层会出错，因为有uid在路径里
        string from_filename = FileName(from_path);
        // get the entry of parent of from_path
        string parent_from_path1 = ParentDir(from_path);
        MetadataEntry parent_from_entry1;
        context->GetEntry(parent_from_path1, &parent_from_entry1);
        // 下面实现将原位置的父目录内对应项删除
        if (flag == true && parent_from_path1 == from_parent)
        {
          //
          parent_from_entry1.add_dir_contents(to_filename);
        }

        for (int i = 1; i < parent_from_entry1.dir_contents_size(); i++)
        {
          if (parent_from_entry1.dir_contents(i) == from_filename)
          {
            // Remove reference to target file entry from dir contents.
            parent_from_entry1.mutable_dir_contents()
                ->SwapElements(i, parent_from_entry1.dir_contents_size() - 1);
            parent_from_entry1.mutable_dir_contents()->RemoveLast();

            // Write updated parent entry.
            context->PutEntry(parent_from_path1, parent_from_entry1);
            break;
          }
        }
        // Erase the from_entry
        context->DeleteEntry(from_path);
        // gaoxuan --this part is used to delete the old entry

        if (to_entry1.type() == DIR)
        {

          for (int i = 1; i < to_entry1.dir_contents_size(); i++)
          {

            string full_path = front + "/" + to_entry1.dir_contents(i);
            queue1.push(full_path);
          }
        }

        if (!queue1.empty())
        {
          from_path = origin_path + queue1.front().substr(desti_path.size());
        }
      }
    }
    else
    {                            // gaoxuan --empty dir or file RENAME opretaion
      MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
      MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
      context->GetEntry(origin_path, &from_entry1);
      to_entry1.CopyFrom(from_entry1);
      context->PutEntry(desti_path, to_entry1);
      // gaoxuan --this part is used to delete the old entry
      //  Update from_parent(Find file and remove it from parent directory.)
      string from_filename = FileName(origin_path);
      // get the entry of parent of from_path
      string parent_from_path1 = ParentDir(origin_path);
      MetadataEntry parent_from_entry1;
      context->GetEntry(parent_from_path1, &parent_from_entry1);

      if (flag == true)
      {
        // 原目录和目的目录都是一个父亲
        parent_from_entry1.add_dir_contents(to_filename);
      }
      for (int i = 1; i < parent_from_entry1.dir_contents_size(); i++)
      {
        if (parent_from_entry1.dir_contents(i) == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          parent_from_entry1.mutable_dir_contents()
              ->SwapElements(i, parent_from_entry1.dir_contents_size() - 1);
          parent_from_entry1.mutable_dir_contents()->RemoveLast();

          // Write updated parent entry.
          context->PutEntry(parent_from_path1, parent_from_entry1);
          break;
        }
      }
      // Erase the from_entry
      context->DeleteEntry(origin_path);
    }
  }
  else if (from_path.find("b") == std::string::npos && to_path.find("b") != std::string::npos)
  { // TODO3：树到hash
    // 找到原位置的父目录和目的目录的分层点
    // 1、原目录及其父目录搜索
    MetadataEntry Parent_from_entry;
    MetadataEntry PParent_from_entry;
    string parent_from_path = ParentDir(in.from_path());
    string root = "";
    string root1 = "";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));
      // 下面是路径拆分
      if (front != "")
      {
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

        // 这一行之前是gaoxuan添加的
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
      if (front1 == ParentDir(parent_from_path)) // 找到了from_path父目录的父目录的元数据项
      {
        PParent_from_entry = out.entry(); // 这里爷爷目录用来确定父目录的
      }
      if (front1 == parent_from_path) // 找到了from_path父目录的元数据项
      {
        Parent_from_entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 1; i < out.entry().dir_contents_size(); i++)
        {
          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (parent_from_path.find(full_path) == 0)
          {
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // PParent_from_entry中存放原位置的爷爷的元数据项，Parent_from_entry存放父亲的元数据项

    // 2、目的位置，先找到分层点，再确定父目录
    int p1 = to_path.find("b");
    string tree_name1 = to_path.substr(0, p1 - 1);
    string hash_name1 = to_path.substr(p1);
    //
    root = "";
    root1 = "";
    MetadataEntry to_split_entry;
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));

      if (front != "")
      {
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
      if (front1 == tree_name1) // 判断树部分是否搜索完成
      {
        to_split_entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 1; i < out.entry().dir_contents_size(); i++)
        {

          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (to_path.find(full_path) == 0)
          { // Todo:这里需要用相对路径
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // 目的位置的分层点元数据项就在to_split_entry这个里面

    // 下面获得原位置的父目录信息
    string from_parent = "/" + PParent_from_entry.dir_contents(0) + FileName(parent_from_path);

    // 下面获得目的位置的父目录信息
    string to_uid = to_split_entry.dir_contents(0);
    string to_parent = ParentDir(to_path);
    int pos_to_parent = to_parent.find('b');
    to_parent = "/" + to_uid + to_parent.substr(pos_to_parent);
    // 下面是从原位置开始向下层次遍历，唯一要注意的点就是确定目的位置的写集路径
    string origin_path = "/" + Parent_from_entry.dir_contents(0) + FileName(from_path);
    string desti_path = "/" + to_uid + to_path.substr(p1);
    MetadataEntry from_entry;
    if (!context->GetEntry(origin_path, &from_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    MetadataEntry parent_from_entry;
    if (!context->GetEntry(from_parent, &parent_from_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "From_Parent File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    MetadataEntry parent_to_entry;
    if (!context->GetEntry(to_parent, &parent_to_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "To_Parent File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    // If file already exists, fail.
    // gaoxuan --check if exist a file with same name in the Parent dir of to_path
    string to_filename = FileName(in.to_path());
    for (int i = 1; i < parent_to_entry.dir_contents_size(); i++)
    {
      if (parent_to_entry.dir_contents(i) == to_filename)
      {
        LOG(ERROR) << "file already exists, fail.";
        out->set_success(false);
        out->add_errors(MetadataAction::FileAlreadyExists);
        return;
      }
    }

    if ((from_entry.type() == DIR) && (from_entry.dir_contents_size() != 0)) // gaoxuan --only if the object we want to rename is DIR we need to loop,if its a file we don't need loop
    {
      // gaoxuan --use BFS to add new metadata entry
      std::queue<string> queue1; // 这个用来遍历树
      std::queue<string> queue2; // 这个用来遍历hash
      string from_root = origin_path;
      queue1.push(from_root);
      queue2.push(desti_path);
      while (!queue1.empty())
      {
        string front = queue1.front();
        queue1.pop();
        string hash_front = queue2.front();
        queue2.pop();
        // Add Entry
        MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
        MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
        context->GetEntry(front, &from_entry1);
        to_entry1.CopyFrom(from_entry1);
        context->PutEntry(hash_front, to_entry1);

        // Erase the from_entry
        context->DeleteEntry(front);
        // gaoxuan --this part is used to delete the old entry
        if (front.find("b") == std::string::npos)
        {
          // 不是分层点
          if (from_entry1.type() == DIR)
          {
            string uid = from_entry1.dir_contents(0);
            for (int i = 1; i < from_entry1.dir_contents_size(); i++)
            {
              string child_path = "/" + uid + from_entry1.dir_contents(i);
              string full_path = hash_front + "/" + from_entry1.dir_contents(i);
              queue1.push(child_path);
              queue2.push(full_path);
            }
          }
        }
        else
        {
          if (from_entry1.type() == DIR)
          {
            // 相比树状，也就是路径加入方式换一下
            for (int i = 1; i < from_entry1.dir_contents_size(); i++)
            {
              string child_path = front + "/" + from_entry1.dir_contents(i);
              string full_path = hash_front + "/" + from_entry1.dir_contents(i);
              queue1.push(child_path);
              queue2.push(full_path);
            }
          }
        }
      }

      if (from_parent != to_parent)
      {
        parent_to_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, parent_to_entry);
      }
      else
      {
        parent_from_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < parent_from_entry.dir_contents_size(); i++)
      {
        if (parent_from_entry.dir_contents(i) == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          parent_from_entry.mutable_dir_contents()
              ->SwapElements(i, parent_from_entry.dir_contents_size() - 1);
          parent_from_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, parent_from_entry);
          break;
        }
      }
    }
    else
    {
      // 原目录是个文件
      MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
      MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
      context->GetEntry(origin_path, &from_entry1);
      to_entry1.CopyFrom(from_entry1);
      context->PutEntry(desti_path, to_entry1);
      // gaoxuan --this part is used to delete the old entry
      //  Update from_parent(Find file and remove it from parent directory.)
      if (from_parent != to_parent)
      {
        parent_to_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, parent_to_entry);
      }
      else
      {
        parent_from_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < parent_from_entry.dir_contents_size(); i++)
      {
        if (parent_from_entry.dir_contents(i) == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          parent_from_entry.mutable_dir_contents()
              ->SwapElements(i, parent_from_entry.dir_contents_size() - 1);
          parent_from_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, parent_from_entry);
          break;
        }
      }
      // Erase the from_entry
      context->DeleteEntry(origin_path);
    }
  }
  else
  { // TODO4: hash到树
    // 原位置是hash，先搜到分层点，再
    //  1.1搜到原位置的分层点

    int p = from_path.find("b");
    string tree_name = from_path.substr(0, p - 1);
    string hash_name = from_path.substr(p);
    //
    string root = "";
    string root1 = "";
    MetadataEntry from_split_entry;
    // LOG(ERROR)<<"还没进入循环";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));

      if (front != "")
      {
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
        from_split_entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 1; i < out.entry().dir_contents_size(); i++)
        {

          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (from_path.find(full_path) == 0)
          { // Todo:这里需要用相对路径
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // 原位置的分层点元数据项就在from_split_entry这个里面

    // 搜索目的位置的父目录
    //  1.1 找到目的位置的父目录以及父父目录
    MetadataEntry Parent_to_entry;
    MetadataEntry PParent_to_entry;
    string parent_to_path = ParentDir(in.to_path());
    root = "";
    root1 = "";
    // LOG(ERROR)<<"还没进入循环";
    while (1)
    {
      string front = root;
      string front1 = root1;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));
      // 下面是路径拆分
      if (front != "")
      {
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

        // 这一行之前是gaoxuan添加的
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
      if (front1 == ParentDir(parent_to_path)) // 找到了to_path父目录的父目录的元数据项
      {
        PParent_to_entry = out.entry(); // 这里爷爷目录用来确定父目录的
      }
      if (front1 == parent_to_path) // 找到了from_path父目录的元数据项
      {
        Parent_to_entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 1; i < out.entry().dir_contents_size(); i++)
        {
          string full_path = front1 + "/" + out.entry().dir_contents(i); // 拼接获取全路径

          if (parent_to_path.find(full_path) == 0)
          {
            // 进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    // PParent_to_entry中存放目的位置的爷爷的元数据项，Parent_to_entry存放父亲的元数据项
    // 获得原位置的父目录
    string from_uid = from_split_entry.dir_contents(0);
    string from_parent = ParentDir(from_path);
    int pos_from_parent = from_parent.find('b');
    from_parent = "/" + from_uid + from_parent.substr(pos_from_parent);
    // 获得目的位置的父目录
    string to_parent = "/" + PParent_to_entry.dir_contents(0) + FileName(parent_to_path);
    string origin_path = "/" + from_uid + from_path.substr(p);
    string desti_path = "/" + Parent_to_entry.dir_contents(0) + FileName(to_path);

    MetadataEntry from_entry;
    if (!context->GetEntry(origin_path, &from_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    MetadataEntry parent_from_entry;
    if (!context->GetEntry(from_parent, &parent_from_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "From_Parent File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }
    MetadataEntry parent_to_entry;
    if (!context->GetEntry(to_parent, &parent_to_entry))
    {
      // File doesn't exist!
      LOG(ERROR) << "To_Parent File doesn't exist!";
      out->set_success(false);
      out->add_errors(MetadataAction::FileDoesNotExist);
      return;
    }

    // 现在原位置和目的位置的父目录的元数据项都拿到了
    // 判断目的路径的父目录下是不是重复
    string to_filename = FileName(in.to_path());
    for (int i = 1; i < parent_to_entry.dir_contents_size(); i++)
    {
      if (parent_to_entry.dir_contents(i) == to_filename)
      {
        LOG(ERROR) << "file already exists, fail.";
        out->set_success(false);
        out->add_errors(MetadataAction::FileAlreadyExists);
        return;
      }
    }
    // hash到树元数据项要怎么放置

    // 里面进行一下修改
    if ((from_entry.type() == DIR) && (from_entry.dir_contents_size() != 0)) // gaoxuan --only if the object we want to rename is DIR we need to loop,if its a file we don't need loop
    {
      // gaoxuan --use BFS to add new metadata entry
      std::queue<string> queue1; // 这个用来遍历hash
      std::queue<string> queue2; // 这个用来遍历树
      string from_root = origin_path;
      queue1.push(from_root);
      queue2.push(desti_path);
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
        // gaoxuan --this part is used to delete the old entry
        if (from_entry1.type() == DIR)
        {
          for (int i = 1; i < from_entry1.dir_contents_size(); i++)
          {
            string child_path = front + "/" + from_entry1.dir_contents(i);
            queue1.push(child_path);
            string tree_path = "/" + from_entry1.dir_contents(0) + from_entry1.dir_contents(i);
            queue2.push(tree_path);
          }
        }
      }

      //   Update to_parent (add new dir content)
      if (from_parent != to_parent)
      {
        parent_to_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, parent_to_entry);
      }
      else
      {
        parent_from_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < parent_from_entry.dir_contents_size(); i++)
      {
        if (parent_from_entry.dir_contents(i) == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          parent_from_entry.mutable_dir_contents()
              ->SwapElements(i, parent_from_entry.dir_contents_size() - 1);
          parent_from_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, parent_from_entry);
          break;
        }
      }
    }
    else
    {
      // 原目录是个文件
      MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
      MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
      context->GetEntry(origin_path, &from_entry1);
      to_entry1.CopyFrom(from_entry1);
      context->PutEntry(desti_path, to_entry1);
      // gaoxuan --this part is used to delete the old entry
      //  Update from_parent(Find file and remove it from parent directory.)
      if (from_parent != to_parent)
      {
        parent_to_entry.add_dir_contents(to_filename);
        context->PutEntry(to_parent, parent_to_entry);
      }
      else
      {
        parent_from_entry.add_dir_contents(to_filename);
      }
      string from_filename = FileName(in.from_path());
      // 源父目录删除
      for (int i = 1; i < parent_from_entry.dir_contents_size(); i++)
      {
        if (parent_from_entry.dir_contents(i) == from_filename)
        {
          // Remove reference to target file entry from dir contents.
          parent_from_entry.mutable_dir_contents()
              ->SwapElements(i, parent_from_entry.dir_contents_size() - 1);
          parent_from_entry.mutable_dir_contents()->RemoveLast();
          // Write updated parent entry.
          context->PutEntry(from_parent, parent_from_entry);
          break;
        }
      }
      // Erase the from_entry
      context->DeleteEntry(origin_path);
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
  out->mutable_entry()->CopyFrom(entry);
}

void MetadataStore::Tree_Lookup_Internal(
    ExecutionContext *context,
    const MetadataAction::Tree_LookupInput &in,
    MetadataAction::Tree_LookupOutput *out)
{

  // Look up existing entry.
  MetadataEntry entry;

  string front = in.path();


      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_flag(2);//标识
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::DATA);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));



      MessageBuffer *m = NULL;
      header->set_data_ptr(reinterpret_cast<uint64>(&m));
      machine_->SendMessage(header, new MessageBuffer());
      while (m == NULL)
      {
        usleep(10);
        Noop<MessageBuffer *>(m);
      }

      MessageBuffer *serialized = m;
      Header b;
      b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
      delete serialized;


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
