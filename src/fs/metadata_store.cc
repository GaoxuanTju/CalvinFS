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
      char pattern = 'e';
      string hash_name;
      if (path.find(pattern) != std::string::npos)
      {
        // TODO：获取相对路径

        hash_name = "";
      }
      else
      { // 不涉及分层，这半部分就是上面那样直接获取即可
        hash_name = path;
      }
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
      char pattern = 'e';
      string hash_name;
      if (path.find(pattern) != std::string::npos)
      {
        // TODO:获取相对路径
        hash_name = "";
      }
      else
      { // 不涉及分层，这半部分就是上面那样直接获取即可
        hash_name = path;
      }
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
      // 判断涉不涉及分层，目前是看有没有b，如果有b就分层
      char pattern = 'e';
      string hash_name;
      if (path.find(pattern) != std::string::npos)
      {
        // TODO：获取相对路径

        hash_name = "";
      }
      else
      { // 不涉及分层，这半部分就是上面那样直接获取即可
        hash_name = path;
      }

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

// 这个初始化函数是增加了分层的
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

// 这个初始化函数是增加了分层的
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
void MetadataStore::Init_for_8(BTNode *dir_tree)
{
  // gaoxuan --这里面会涉及到目录树的建立初始化。
  int a_0 = machine_->config().size();
  int a_1 = 2;
  int a_2 = 2;
  int a_3 = 2;
  int a_4 = 2;
  int a_5 = 2;

  int bsize = 2;
  int csize = 2;
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
    string a0_uid = IntToString(i_0 +1); //第一层的uid，
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
      string a1_uid = IntToString(a_0 + i_0*a_1 + i_1 + 1);//第二层的uid
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
        string a2_uid = IntToString(a_0 + a_0*a_1 + a_2 * i_1 + i_2 + 1);//第三层的uid
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
          string a3_uid = IntToString(a_0 + a_0*a_1 +  a_0*a_1*a_2 + a_3*i_2 +i_3 +1);//第四层
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
            string a4_uid = IntToString(a_0 + a_0*a_1 +  a_0*a_1*a_2 +a_0*a_1*a_2*a_3 + a_4*i_3 +i_4 +1);//第四层
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
              string a5_uid = IntToString(a_0 + a_0*a_1 +  a_0*a_1*a_2 +a_0*a_1*a_2*a_3 + a_0*a_1*a_2 +a_0*a_1*a_2*a_3*a_4+ a_5*i_4 +i_5 +1);//第五层
              if (IsLocal(a_5_dir))
              {
                MetadataEntry entry;
                entry.mutable_permissions();
                entry.set_type(DIR);
                entry.add_dir_contents(a5_uid);
                for (int i_6 = 0; i_6 < bsize; i_6++)
                {
                  entry.add_dir_contents("b" + IntToString(i_6));
                }
                string serialized_entry;
                entry.SerializeToString(&serialized_entry);
                store_->Put(a_5_dir, serialized_entry, 0);
              }
              BTNode *b_level = NULL; // 这个就指向该层第一个节点
              for (int i_6 = 0; i_6 < bsize; i_6++)
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
                string a6_uid = IntToString(a_0 + a_0*a_1 +  a_0*a_1*a_2 +a_0*a_1*a_2*a_3 + a_0*a_1*a_2 +a_0*a_1*a_2*a_3*a_4+a_0*a_1*a_2 +a_0*a_1*a_2*a_3*a_4*a_5+ bsize*i_5 +i_6 +1);//第五层 
                if (IsLocal(a_6_dir))
                {
                  MetadataEntry entry;
                  entry.mutable_permissions();
                  entry.set_type(DIR);
                  entry.add_dir_contents(a6_uid);//这个用不上，因为分层开始
                  for (int k = 0; k < csize; k++)
                  {
                    entry.add_dir_contents("c" + IntToString(k));
                  }
                  string serialized_entry;
                  entry.SerializeToString(&serialized_entry);
                  store_->Put(a_6_dir, serialized_entry, 0);
                }
                // Add files.
                BTNode *c_level = NULL; // 这个就指向该层第三个节点
                for (int k = 0; k < csize; k++)
                {
                  string file(a_6_dir + "/c" + IntToString(k));//从b开始分层，下面的只需要相对路径就好
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
                  string a7_uid = IntToString(a_0 + a_0*a_1 +  a_0*a_1*a_2 +a_0*a_1*a_2*a_3 + a_0*a_1*a_2 +a_0*a_1*a_2*a_3*a_4+a_0*a_1*a_2 +a_0*a_1*a_2*a_3*a_4*a_5+a_0*a_1*a_2 +a_0*a_1*a_2*a_3*a_4*a_5*bsize+ csize*i_6 +k +1);//第五层 
                  if (IsLocal(file))
                  {
                    MetadataEntry entry;
                    entry.mutable_permissions();
                    entry.set_type(DATA);
                    entry.add_dir_contents(a7_uid);//这个用不上，因为分层开始
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
{
  std::stack<string> stack1; // gaoxuan --the stack is used for tranversing  the file tree
  stack1.push(path);         // gaoxuan --we want to tranverse all children of this path to add read/write set

  while (!stack1.empty())
  {
    string top = stack1.top();              // gaoxuan --get the top
    stack1.pop();                           // gaoxuan --pop the top
    if (top.find("d") != std::string::npos) // gaoxuan --this 10 is used to limit output.Because it will be too many output without limitation
    {
      LOG(ERROR) << "renamed file is: " << top;
    }
    uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(top)), config_->LookupReplica(machine_->machine_id()));
    Header *header = new Header();
    header->set_from(machine_->machine_id());
    header->set_to(mds_machine);
    header->set_type(Header::RPC);
    header->set_app("client");
    header->set_rpc("LOOKUP");
    header->add_misc_string(top.c_str(), strlen(top.c_str()));
    // gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

    // 第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用空格填充上
    // 拆分的算法，遇到一个/就把之前的字符串放进去
    // 将拆分后的元素添加去的方法：header->add_split_string(拆分的字符串)
    int flag = 0;       // 用来标识此时split_string 里面有多少子串
    char pattern = '/'; // 根据/进行字符串拆分

    string temp_from = top.c_str();
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
    MetadataAction::LookupOutput out;
    out.ParseFromString(b.output());
    if (out.success() && out.entry().type() == DIR)
    {

      for (int i = 0; i < out.entry().dir_contents_size(); i++)
      {

        string full_path = top + "/" + out.entry().dir_contents(i);
        stack1.push(full_path);
      }
    }
  }
  LOG(ERROR) << "finished LOOKUP!";
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
    // gaoxuan --this part is rewrited by gaoxuan
    // gaoxuan --add read/write set in the way of DFS
    MetadataAction::RenameInput in;
    in.ParseFromString(action->input());
    std::stack<string> stack1;   // gaoxuan --the stack is used for tranversing  the file tree
    stack1.push(in.from_path()); // gaoxuan --we want to tranverse all children of this path to add read/write set
    string To_path = in.to_path();
    while (!stack1.empty())
    {
      string top = stack1.top(); // get the top
      stack1.pop();              // pop the top
      action->add_readset(top);
      action->add_writeset(top);

      string s = top.substr(in.from_path().size()); // gaoxuan --s is used to get the path without from_path
      To_path = in.to_path() + s;                   // gaoxuan --To_path is the path that needed to add writeset
      action->add_writeset(To_path);

      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(top)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app(getAPPname());
      header->set_rpc("LOOKUP");
      header->add_misc_string(top.c_str(), strlen(top.c_str()));
      // gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

      // 第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用空格填充上
      // 拆分的算法，遇到一个/就把之前的字符串放进去
      // 将拆分后的元素添加去的方法：header->add_split_string(拆分的字符串)
      int flag = 0;       // 用来标识此时split_string 里面有多少子串
      char pattern = '/'; // 根据/进行字符串拆分

      string temp_from = top.c_str();
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
      MetadataAction::LookupOutput out;
      out.ParseFromString(b.output());
      if (out.success() && out.entry().type() == DIR)
      {

        for (int i = 0; i < out.entry().dir_contents_size(); i++)
        {
          string full_path = top + "/" + out.entry().dir_contents(i);
          stack1.push(full_path);
        }
      }
    }
    // gaoxuan --in case that add read/write set repeatedly when form_path and to_path have same parent dir
    if (ParentDir(in.from_path()) == ParentDir(in.to_path()))
    {
      action->add_readset(ParentDir(in.from_path()));
      action->add_writeset(ParentDir(in.from_path()));
    }
    else
    {
      action->add_readset(ParentDir(in.from_path()));
      action->add_writeset(ParentDir(in.from_path()));

      action->add_readset(ParentDir(in.to_path()));
      action->add_writeset(ParentDir(in.to_path()));
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
    // LOG(ERROR)<<"Run is executing!";
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
{ // gaoxuan --this function wiil be executed when running RenameExperiment
  // Currently only support Copy: (non-recursive: only succeeds for DATA files and EMPTY directory)

  // gaoxuan --now consider how to modify the logic of rename with correct context
  // LOG(ERROR)<<"Rename_internal is Executing!";
  MetadataEntry from_entry; // gaoxuan --get from_path's entry to check if it's existed,put it into from_entry
  if (!context->GetEntry(in.from_path(), &from_entry))
  {
    // File doesn't exist!
    LOG(ERROR) << "File doesn't exist!";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_from_path = ParentDir(in.from_path());
  MetadataEntry parent_from_entry;
  if (!context->GetEntry(parent_from_path, &parent_from_entry))
  {
    // File doesn't exist!
    LOG(ERROR) << "From_Parent File doesn't exist!";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_to_path = ParentDir(in.to_path());
  MetadataEntry parent_to_entry;
  if (!context->GetEntry(parent_to_path, &parent_to_entry))
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
  for (int i = 0; i < parent_to_entry.dir_contents_size(); i++)
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
  parent_to_entry.add_dir_contents(to_filename);
  context->PutEntry(parent_to_path, parent_to_entry);

  if ((from_entry.type() == DIR) && (from_entry.dir_contents_size() != 0)) // gaoxuan --only if the object we want to rename is DIR we need to loop,if its a file we don't need loop
  {
    // gaoxuan --use BFS to add new metadata entry
    std::queue<string> queue1;
    string root = in.to_path();
    queue1.push(root);
    string from_path = in.from_path(); // gaoxuan --the path used to copyEntry
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
      string from_filename = FileName(from_path);
      // get the entry of parent of from_path
      string parent_from_path1 = ParentDir(from_path);
      MetadataEntry parent_from_entry1;
      context->GetEntry(parent_from_path1, &parent_from_entry1);

      for (int i = 0; i < parent_from_entry1.dir_contents_size(); i++)
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

        for (int i = 0; i < to_entry1.dir_contents_size(); i++)
        {

          string full_path = front + "/" + to_entry1.dir_contents(i);
          queue1.push(full_path);
        }
      }

      if (!queue1.empty())
      {
        from_path = in.from_path() + queue1.front().substr(in.to_path().size());
      }
    }
  }
  else
  {                            // gaoxuan --empty dir or file RENAME opretaion
    MetadataEntry to_entry1;   // gaoxuan --the entry which will be added
    MetadataEntry from_entry1; // gaoxuan --the entry which will be used to copy to to_entry
    context->GetEntry(in.from_path(), &from_entry1);
    to_entry1.CopyFrom(from_entry1);
    context->PutEntry(in.to_path(), to_entry1);
    // gaoxuan --this part is used to delete the old entry
    //  Update from_parent(Find file and remove it from parent directory.)
    string from_filename = FileName(in.from_path());
    // get the entry of parent of from_path
    string parent_from_path1 = ParentDir(in.from_path());
    MetadataEntry parent_from_entry1;
    context->GetEntry(parent_from_path1, &parent_from_entry1);

    for (int i = 0; i < parent_from_entry1.dir_contents_size(); i++)
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
    context->DeleteEntry(in.from_path());
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

  string path = in.path();
  string hash_name;

  if (path.find("e") != std::string::npos)
  {

    // 要分层
    // Todo:获取分层后的相对路径
    hash_name = "";
    //

    string root = "";

    while (1)
    {
      string front = root;
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(front)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(front.c_str(), strlen(front.c_str()));

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
      MetadataAction::LookupOutput out;
      out.ParseFromString(b.output());

      // TODO：//找到分层点
      if (1) // 找到了分层点
      {

        break;
      }
      for (int i = 0; i < out.entry().dir_contents_size(); i++)
      {
        string full_path = front + "/" + out.entry().dir_contents(i);
        if (in.path().find(full_path) == 0)
        {
          root = full_path;
          break;
        }
      }
    }

    if (context->EntryExists(hash_name))
    { // 存在这个路径的元数据项，证明就是他
      context->GetEntry(hash_name, &entry);
      LOG(ERROR) << hash_name << " 's entry!";
    }
    else
    {
      LOG(ERROR) << hash_name << " dont's exists!";
    }
    // TODO(agt): Check permissions.

    // Return entry.
    out->mutable_entry()->CopyFrom(entry);
  }
  else
  {

    // 不分层，只是树
    //  gaoxuan --use BFS to add new metadata entry

    string root = "";
    string root1 = "";
    LOG(ERROR)<<"还没进入循环";
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
      if (front1 == in.path())//单独用全路径来判断是否搜索完成,可以肯定是这里没执行，才退不出去
      {
        entry = out.entry();
        break;
      }
      else
      { // gaoxuan --还没有找到
        for (int i = 0; i < out.entry().dir_contents_size(); i++)
        {

          string full_path = front + "/" + out.entry().dir_contents(i);//拼接获取全路径

          if (in.path().find(full_path) == 0)
          { // Todo:这里需要用相对路径
          //进入这个分支就代表此时，恰好搜到了，此时i代表的就是所需的相对路径，我们只需要用0位置的id拼一下就好
            root1 = full_path;
            root = "/" + out.entry().dir_contents(0) + out.entry().dir_contents(i);
            break;
          }
        }
      }
    }
    LOG(ERROR)<<"还没出循环";
    // TODO(agt): Check permissions.
    // Return entry.
    out->mutable_entry()->CopyFrom(entry);
  }
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
