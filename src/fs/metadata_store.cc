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
#include <typeinfo> //gaoxuan --用于确定数据类型
#include <stack> //gaoxuan --用于深搜时的栈
#include <list> //gaoxuan --用于深搜时的列表
using std::map;
using std::set;
using std::string;

REGISTER_APP(MetadataStoreApp) {
  return new StoreApp(new MetadataStore(new HybridVersionedKVStore()));
}

///////////////////////        ExecutionContext        ////////////////////////
//
// TODO(agt): The implementation below is a LOCAL execution context.
//            Extend this to be a DISTRIBUTED one.
// TODO(agt): Generalize and move to components/store/store.{h,cc}.
//
class ExecutionContext {
 public:
  // Constructor performs all reads.
  ExecutionContext(VersionedKVStore* store, Action* action)
      : store_(store), version_(action->version()), aborted_(false) {
    for (int i = 0; i < action->readset_size(); i++) {
      if (!store_->Get(action->readset(i),
                       version_,
                       &reads_[action->readset(i)])) {
        reads_.erase(action->readset(i));
      }
    }

    if (action->readset_size() > 0) {
      reader_ = true;
    } else {
      reader_ = false;
    }

    if (action->writeset_size() > 0) {
      writer_ = true;
    } else {
      writer_ = false;
    }

  }

  // Destructor installs all writes.
  ~ExecutionContext() {
    if (!aborted_) {
      for (auto it = writes_.begin(); it != writes_.end(); ++it) {
        store_->Put(it->first, it->second, version_);
      }
      for (auto it = deletions_.begin(); it != deletions_.end(); ++it) {
        store_->Delete(*it, version_);
      }
    }
  }

  bool EntryExists(const string& path) {
    return reads_.count(path) != 0;
  }

  bool GetEntry(const string& path, MetadataEntry* entry) {
    entry->Clear();
    if (reads_.count(path) != 0) {
      entry->ParseFromString(reads_[path]);
      return true;
    }
    return false;
  }

  void PutEntry(const string& path, const MetadataEntry& entry) {
    deletions_.erase(path);
    entry.SerializeToString(&writes_[path]);
    if (reads_.count(path) != 0) {
      entry.SerializeToString(&reads_[path]);
    }
  }

  void DeleteEntry(const string& path) {
    reads_.erase(path);
    writes_.erase(path);
    deletions_.insert(path);
  }

  bool IsWriter() {
    if (writer_)
      return true;
    else
      return false;
  }

  void Abort() {
    aborted_ = true;
  }

 protected:
  ExecutionContext() {}
  VersionedKVStore* store_;
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
class DistributedExecutionContext : public ExecutionContext {
 public:
  // Constructor performs all reads.
  DistributedExecutionContext(
      Machine* machine,
      CalvinFSConfigMap* config,
      VersionedKVStore* store,
      Action* action)
        : machine_(machine), config_(config) {
    // Initialize parent class variables.
    store_ = store;
    version_ = action->version();
    aborted_ = false;

    // Look up what replica we're at.
    replica_ = config_->LookupReplica(machine_->machine_id());

    // Figure out what machines are readers (and perform local reads).
    reader_ = false;
    set<uint64> remote_readers;
    for (int i = 0; i < action->readset_size(); i++) {
      uint64 mds = config_->HashFileName(action->readset(i));
      uint64 machine = config_->LookupMetadataShard(mds, replica_);
      if (machine == machine_->machine_id()) {
        // Local read.
        if (!store_->Get(action->readset(i),
                         version_,
                         &reads_[action->readset(i)])) {
          reads_.erase(action->readset(i));
        }
        reader_ = true;
      } else {
        remote_readers.insert(machine);
      }
    }

    // Figure out what machines are writers.
    writer_ = false;
    set<uint64> remote_writers;
    for (int i = 0; i < action->writeset_size(); i++) {
      uint64 mds = config_->HashFileName(action->writeset(i));
      uint64 machine = config_->LookupMetadataShard(mds, replica_);
      if (machine == machine_->machine_id()) {
        writer_ = true;
      } else {
        remote_writers.insert(machine);
      }
    }

    // If any reads were performed locally, broadcast them to writers.
    if (reader_) {
      MapProto local_reads;
      for (auto it = reads_.begin(); it != reads_.end(); ++it) {
        MapProto::Entry* e = local_reads.add_entries();
        e->set_key(it->first);
        e->set_value(it->second);
      }
      for (auto it = remote_writers.begin(); it != remote_writers.end(); ++it) {
        Header* header = new Header();
        header->set_from(machine_->machine_id());
        header->set_to(*it);
        header->set_type(Header::DATA);
        header->set_data_channel("action-" + UInt64ToString(version_));
        machine_->SendMessage(header, new MessageBuffer(local_reads));
      }
    }

    // If any writes will be performed locally, wait for all remote reads.
    if (writer_) {
      // Get channel.
      AtomicQueue<MessageBuffer*>* channel =
          machine_->DataChannel("action-" + UInt64ToString(version_));
      for (uint32 i = 0; i < remote_readers.size(); i++) {
        MessageBuffer* m = NULL;
        // Get results.
        while (!channel->Pop(&m)) {
          usleep(10);
        }
        MapProto remote_read;
        remote_read.ParseFromArray((*m)[0].data(), (*m)[0].size());
        for (int j = 0; j < remote_read.entries_size(); j++) {
          CHECK(reads_.count(remote_read.entries(j).key()) == 0);
          reads_[remote_read.entries(j).key()] = remote_read.entries(j).value();
        }
      }
      // Close channel.
      machine_->CloseDataChannel("action-" + UInt64ToString(version_));
    }
  }

  // Destructor installs all LOCAL writes.
  ~DistributedExecutionContext() {
    if (!aborted_) {
      for (auto it = writes_.begin(); it != writes_.end(); ++it) {
        uint64 mds = config_->HashFileName(it->first);
        uint64 machine = config_->LookupMetadataShard(mds, replica_);
        if (machine == machine_->machine_id()) {
          store_->Put(it->first, it->second, version_);
        }
      }
      for (auto it = deletions_.begin(); it != deletions_.end(); ++it) {
        uint64 mds = config_->HashFileName(*it);
        uint64 machine = config_->LookupMetadataShard(mds, replica_);
        if (machine == machine_->machine_id()) {
          store_->Delete(*it, version_);
        }
      }
    }
  }

 private:
  // Local machine.
  Machine* machine_;

  // Deployment configuration.
  CalvinFSConfigMap* config_;

  // Local replica id.
  uint64 replica_;

};

///////////////////////          MetadataStore          ///////////////////////
//找父目录的方法是直接对这个文件路径使用rfind逆向搜索，确定最后一个/的位置，那么从位置0到最后一个之间的这个字符串就是它的父目录了，秒哇
string ParentDir(const string& path) {
  // Root dir is a special case.
  if (path.empty()) {
    LOG(FATAL) << "root dir has no parent";
  }
  uint32 offset = path.rfind('/');
  CHECK_NE(string::npos, offset);     // at least 1 slash required
  CHECK_NE(path.size() - 1, offset);  // filename cannot be empty
  return string(path, 0, offset);
}
//获取文件名字也是同样的方法，不同点就在于这次是从最后一个/到最后的位置就是文件名字了
string FileName(const string& path) {
  // Root dir is a special case.
  if (path.empty()) {
    return path;
  }
  uint32 offset = path.rfind('/');
  CHECK_NE(string::npos, offset);     // at least 1 slash required
  CHECK_NE(path.size() - 1, offset);  // filename cannot be empty
  return string(path, offset + 1);
}

MetadataStore::MetadataStore(VersionedKVStore* store)
    : store_(store), machine_(NULL), config_(NULL) {
}

MetadataStore::~MetadataStore() {
  delete store_;
}

void MetadataStore::SetMachine(Machine* m) {
  machine_ = m;
  config_ = new CalvinFSConfigMap(machine_);

  // Initialize by inserting an entry for the root directory "/" (actual
  // representation is "" since trailing slashes are always removed).
  if (IsLocal("")) {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }
}

int RandomSize() {
  return 1 + rand() % 2047;
}

void MetadataStore::Init() {
  LOG(ERROR)<< "Execution step2 ：in metadata_store.cc's Init";//gaoxuan --
  int asize = machine_->config().size();
  int bsize = 1000;
  int csize = 500;
  
  double start = GetTime();

  // Update root dir.
  if (IsLocal("")) {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i = 0; i < 1000; i++) {
      entry.add_dir_contents("a" + IntToString(i));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }

  // Add dirs.
  for (int i = 0; i < asize; i++) {
    string dir("/a" + IntToString(i));
    if (IsLocal(dir)) {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int j = 0; j < bsize; j++) {
        entry.add_dir_contents("b" + IntToString(j));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(dir, serialized_entry, 0);
    }
    // Add subdirs.
    for (int j = 0; j < bsize; j++) {
      string subdir(dir + "/b" + IntToString(j));
      if (IsLocal(subdir)) {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        for (int k = 0; k < csize; k++) {
          entry.add_dir_contents("c" + IntToString(k));
        }
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(subdir, serialized_entry, 0);
      }
      // Add files.
      for (int k = 0; k < csize; k++) {
        string file(subdir + "/c" + IntToString(k));
        if (IsLocal(file)) {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DATA);
          FilePart* fp = entry.add_file_parts();
          fp->set_length(RandomSize());
          fp->set_block_id(0);
          fp->set_block_offset(0);
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(file, serialized_entry, 0);
        }
      }
      if (j % 100 == 0) {
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

void MetadataStore::InitSmall() {
  int asize = machine_->config().size();
  int bsize = 1000;

  double start = GetTime();

  // Update root dir.
  if (IsLocal("")) {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i = 0; i < 1000; i++) {
      entry.add_dir_contents("a" + IntToString(i));
    }
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry, 0);
  }

  // Add dirs.
  for (int i = 0; i < asize; i++) {
    string dir("/a" + IntToString(i));
    if (IsLocal(dir)) {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int j = 0; j < bsize; j++) {
        entry.add_dir_contents("b" + IntToString(j));
      }
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(dir, serialized_entry, 0);
    }
    // Add subdirs.
    for (int j = 0; j < bsize; j++) {
      string subdir(dir + "/b" + IntToString(j));
      if (IsLocal(subdir)) {
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
      if (IsLocal(file)) {
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

bool MetadataStore::IsLocal(const string& path) {
  return machine_->machine_id() ==
         config_->LookupMetadataShard(
            config_->HashFileName(path),
            config_->LookupReplica(machine_->machine_id()));
}


/*gaoxuan --这里是一些碎碎念，想要做的事情
1 要在GetRWSets函数里面的RENAME部分改写一下，
  改写成的样子是这个目录，它的父目录，它的所有子目录，都要获取读写集，目的是加锁

  使用的算法呢？应当是一个树的搜索算法，这部分深搜就好
  同时这里，代码里面只弄了一个获取父目录的方法，对照着看看是不是能先写一个函数，获取所有子目录，这个应该是不难的，因为CalvinFS元数据的键值存储中
  值的部分的第三个字段，contents里面是会包含所有的子目录的

  小Tips：content是字段：如果是目录的话，包含的是所有文件和子目录的列表
  所以说在进行遍历时，判断有没有叶子的方式就是，这个目录的contents里面如果是空，代表这个目录就是一个叶子
  如果不空，文件时叶子，若有子目录，再去查看子目录的contents的内容。




综合版的深度优先遍历
改写措施

    MetadataAction::RenameInput in;
    in.ParseFromString(action->input());
    //姑且当作string来处理
    stack<string> stack;//建立用于遍历的栈
    stack.push(in.from_path());//把要更改的这个当作根放入栈里面
    action->add_readset(ParentDir(in.from_path()));//先把原位置的那个父目录读写集加入
    action->add_writeset(ParentDir(in.from_path()));
    while (!stack.empty()) 
    {//栈还不空的时候，对子目录进行处理

            string top = stack.top();//获取顶部的文件路径
            stack.pop();//出栈节点

            //执行对这个目录的获取读写集的逻辑
            
            action->add_readset(top);
            action->add_writeset(top);
              
            //下面将top目录的所有孩子都放在list中
            list<string> children = top.getChildren();//要写一个获取所有孩子的逻辑
            for (list<string>::iterator it = children.begin(); it != children.end(); ++it)
            {
                  stack.push(*it);
            }
            
     }
    //把目标位置的读写集获取
    action->add_writeset(in.to_path());
    action->add_readset(ParentDir(in.to_path()));
    action->add_writeset(ParentDir(in.to_path()));

上面就是改写后的大体逻辑了，需要让他能跑，要做的事
（1）查一下c++栈的头文件以及如何使用栈，进栈出栈的函数
#include <stack>
创建栈    stack<T> path;
添加元素  path.push(file_path);
获取栈顶（不删除） path.top();
删除栈顶（不获取） path.pop();
是否为空          path.empty();

（2）找一下in.from_path这是什么类型，好确定栈是什么类型
姑且是当作string来使用，还没有确定

（3）看一下c++有没有列表，如果有就简单一些，没有就用循环
#include <list>
创建一个列表  list<string> list;
//使用迭代器依次输出list容器中的元素
    for (list<string>::iterator it = values.begin(); it != values.end(); ++it) {
        cout << *it << " ";
    }

（4）获取目录所有子目录的逻辑

//如果这个对象在这里能够直接用的话，Run里面是直接用的，不知道这里能吗？


map<string, string> reads_;
MetadataEntry* entry;
entry->Clear();
if (reads_.count(path) != 0) 
{
      entry->ParseFromString(reads_[path]);
}
//这样就把path的元数据拿过来了！
string contents = entry.dir_contents();

//首先明确一点就是，直接使用reads[in.from_path()]是拿不出来的

2 ok上面的工作做完之后/做的同时，在新的位置先创建一个新的目录树。这个是不是可以在遍历的时候就直接干了呢？
  就是说我从要修改的部分开始，遍历到一个目录，就获取读写集，同时将这个目录对应元数据拷贝到新位置的目录树下，这不就相当于逻辑上把文件挪到了新的位置上
3 上面的工作完成之后，新的RENAME之后的结果我们已经获取到了。需要解决残留工作
  也就是说把原来的元数据删除掉，这里应该从下往上删除，按道理来说应该是从最底层开始，一层一层的删除，这应该是使用广度优先遍历的逆遍历吧？
OK 上面的就是我当前所需要做的工作啦！
*/
//gaoxuan --这里是要修改的第一个地方，需要将要Rename的目录以及其所有子目录都获取读写集

void MetadataStore::GetRWSets(Action* action) {//gaoxuan --这个函数被RameFile调用了一下
  action->clear_readset();
  action->clear_writeset();

  MetadataAction::Type type =
      static_cast<MetadataAction::Type>(action->action_type());

  if (type == MetadataAction::CREATE_FILE) {
    MetadataAction::CreateFileInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
    action->add_readset(ParentDir(in.path()));
    action->add_writeset(ParentDir(in.path()));

  } else if (type == MetadataAction::ERASE) {
    MetadataAction::EraseInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
    action->add_readset(ParentDir(in.path()));
    action->add_writeset(ParentDir(in.path()));

  } else if (type == MetadataAction::COPY) {
    MetadataAction::CopyInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.from_path());
    action->add_writeset(in.to_path());
    action->add_readset(ParentDir(in.to_path()));
    action->add_writeset(ParentDir(in.to_path()));

  } else if (type == MetadataAction::RENAME) {//gaoxuan --这里会被调用
  //gaoxuan --先看懂下面这个是什么逻辑
    MetadataAction::RenameInput in;
    in.ParseFromString(action->input());
    
    action->add_readset(in.from_path());
    action->add_writeset(in.from_path());
    action->add_readset(ParentDir(in.from_path()));
    action->add_writeset(ParentDir(in.from_path()));
    action->add_writeset(in.to_path());
    action->add_readset(ParentDir(in.to_path()));
    action->add_writeset(ParentDir(in.to_path()));
    //gaoxuan --测试一下能不能行
    
    map<string, string> reads_gaoxuan;
    //
    MetadataEntry* entry;
    entry->Clear();
    entry->ParseFromString(reads_gaoxuan[ParentDir(in.from_path())]);//照常理来说，现在entry里面就是in.from_path的元数据了
    //
    LOG(ERROR)<<in.from_path()<<":";
    for(int i =0;i<entry.dir_contents_size();i++)
    {
      LOG(ERROR)<<entry.dir_contents(i)<<;
    }
    //<<reads_gaoxuan[ParentDir(in.from_path())];

      

    //gaoxuan --这里是终止
  } else if (type == MetadataAction::LOOKUP) {
    MetadataAction::LookupInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());

  } else if (type == MetadataAction::RESIZE) {
    MetadataAction::ResizeInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

  } else if (type == MetadataAction::WRITE) {
    MetadataAction::WriteInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

  } else if (type == MetadataAction::APPEND) {
    MetadataAction::AppendInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

  } else if (type == MetadataAction::CHANGE_PERMISSIONS) {
    MetadataAction::ChangePermissionsInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

  } else {
    LOG(FATAL) << "invalid action type";
  }
}

void MetadataStore::Run(Action* action) {
  //gaoxuan --事实上这里又运行了一个函数
  //LOG(ERROR) << "Run in metadata_store.cc--gaoxuan";//gaoxuan --这个地方会被schduler执行，在RenameExperiment()中Test progress开始之后
  // Prepare by performing all reads.
  ExecutionContext* context;
  if (machine_ == NULL) {
    context = new ExecutionContext(store_, action);
  } else {
    context =
        new DistributedExecutionContext(machine_, config_, store_, action);
  }

  if (!context->IsWriter()) {
    delete context;
    return;
  }

  // Execute action.
  MetadataAction::Type type =
      static_cast<MetadataAction::Type>(action->action_type());

  if (type == MetadataAction::CREATE_FILE) {
    MetadataAction::CreateFileInput in;
    MetadataAction::CreateFileOutput out;
    in.ParseFromString(action->input());
    CreateFile_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::ERASE) {
    MetadataAction::EraseInput in;
    MetadataAction::EraseOutput out;
    in.ParseFromString(action->input());
    Erase_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::COPY) {
    MetadataAction::CopyInput in;
    MetadataAction::CopyOutput out;
    in.ParseFromString(action->input());
    Copy_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::RENAME) {
    //gaoxuan --肯定这里也执行了
    MetadataAction::RenameInput in;
    MetadataAction::RenameOutput out;
    in.ParseFromString(action->input());
    Rename_Internal(context, in, &out);//gaoxuan --调用了这个函数,再过去查一下
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::LOOKUP) {
    MetadataAction::LookupInput in;
    MetadataAction::LookupOutput out;
    in.ParseFromString(action->input());
    Lookup_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::RESIZE) {
    MetadataAction::ResizeInput in;
    MetadataAction::ResizeOutput out;
    in.ParseFromString(action->input());
    Resize_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::WRITE) {
    MetadataAction::WriteInput in;
    MetadataAction::WriteOutput out;
    in.ParseFromString(action->input());
    Write_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::APPEND) {
    MetadataAction::AppendInput in;
    MetadataAction::AppendOutput out;
    in.ParseFromString(action->input());
    Append_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::CHANGE_PERMISSIONS) {
    MetadataAction::ChangePermissionsInput in;
    MetadataAction::ChangePermissionsOutput out;
    in.ParseFromString(action->input());
    ChangePermissions_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else {
    LOG(FATAL) << "invalid action type";
  }

  delete context;
}

void MetadataStore::CreateFile_Internal(
    ExecutionContext* context,
    const MetadataAction::CreateFileInput& in,
    MetadataAction::CreateFileOutput* out) {
  // Don't fuck with the root dir.
  if (in.path() == "") {
    out->set_success(false);
    out->add_errors(MetadataAction::PermissionDenied);
    return;
  }

  // Look up parent dir.
  string parent_path = ParentDir(in.path());
  MetadataEntry parent_entry;
  if (!context->GetEntry(parent_path, &parent_entry)) {
    // Parent doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // TODO(agt): Check permissions.

  // If file already exists, fail.
  // TODO(agt): Look up file directly instead of looking through parent dir?
  string filename = FileName(in.path());
  for (int i = 0; i < parent_entry.dir_contents_size(); i++) {
    if (parent_entry.dir_contents(i) == filename) {
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
    ExecutionContext* context,
    const MetadataAction::EraseInput& in,
    MetadataAction::EraseOutput* out) {
  // Don't fuck with the root dir.
  if (in.path() == "") {
    out->set_success(false);
    out->add_errors(MetadataAction::PermissionDenied);
    return;
  }

  // Look up parent dir.
  string parent_path = ParentDir(in.path());
  MetadataEntry parent_entry;
  if (!context->GetEntry(parent_path, &parent_entry)) {
    // Parent doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // Look up target file.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }
  if (entry.type() == DIR && entry.dir_contents_size() != 0) {
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
  for (int i = 0; i < parent_entry.dir_contents_size(); i++) {
    if (parent_entry.dir_contents(i) == filename) {
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
    ExecutionContext* context,
    const MetadataAction::CopyInput& in,
    MetadataAction::CopyOutput* out) {

  // Currently only support Copy: (non-recursive: only succeeds for DATA files and EMPTY directory)
  MetadataEntry from_entry;
  if (!context->GetEntry(in.from_path(), &from_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_to_path = ParentDir(in.to_path());
  MetadataEntry parent_to_entry;
  if (!context->GetEntry(parent_to_path, &parent_to_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // If file already exists, fail.
  string filename = FileName(in.to_path());
  for (int i = 0; i < parent_to_entry.dir_contents_size(); i++) {
    if (parent_to_entry.dir_contents(i) == filename) {
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
    ExecutionContext* context,
    const MetadataAction::RenameInput& in,
    MetadataAction::RenameOutput* out) {//gaoxuan --这个函数肯定也会被执行
  // Currently only support Copy: (non-recursive: only succeeds for DATA files and EMPTY directory)
  MetadataEntry from_entry;
  if (!context->GetEntry(in.from_path(), &from_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_from_path = ParentDir(in.from_path());
  MetadataEntry parent_from_entry;
  if (!context->GetEntry(parent_from_path, &parent_from_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_to_path = ParentDir(in.to_path());
  MetadataEntry parent_to_entry;
  if (!context->GetEntry(parent_to_path, &parent_to_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // If file already exists, fail.
  string to_filename = FileName(in.to_path());
  for (int i = 0; i < parent_to_entry.dir_contents_size(); i++) {
    if (parent_to_entry.dir_contents(i) == to_filename) {
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }

  // Update to_parent (add new dir content)
  parent_to_entry.add_dir_contents(to_filename);
  context->PutEntry(parent_to_path, parent_to_entry);
  
  // Add to_entry
  MetadataEntry to_entry;
  to_entry.CopyFrom(from_entry);
  context->PutEntry(in.to_path(), to_entry);

  // Update from_parent(Find file and remove it from parent directory.)
  string from_filename = FileName(in.from_path());
  for (int i = 0; i < parent_from_entry.dir_contents_size(); i++) {
    if (parent_from_entry.dir_contents(i) == from_filename) {
      // Remove reference to target file entry from dir contents.
      parent_from_entry.mutable_dir_contents()
          ->SwapElements(i, parent_from_entry.dir_contents_size() - 1);
      parent_from_entry.mutable_dir_contents()->RemoveLast();

      // Write updated parent entry.
      context->PutEntry(parent_from_path, parent_from_entry);
      break;
    }
  }

  // Erase the from_entry
  context->DeleteEntry(in.from_path());
}

void MetadataStore::Lookup_Internal(
    ExecutionContext* context,
    const MetadataAction::LookupInput& in,
    MetadataAction::LookupOutput* out) {
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // TODO(agt): Check permissions.

  // Return entry.
  out->mutable_entry()->CopyFrom(entry);
}

void MetadataStore::Resize_Internal(
    ExecutionContext* context,
    const MetadataAction::ResizeInput& in,
    MetadataAction::ResizeOutput* out) {
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // Only resize DATA files.
  if (entry.type() != DATA) {
    out->set_success(false);
    out->add_errors(MetadataAction::WrongFileType);
    return;
  }

  // TODO(agt): Check permissions.

  // If we're resizing to size 0, just clear all file part.
  if (in.size() == 0) {
    entry.clear_file_parts();
    return;
  }

  // Truncate/remove entries that go past the target size.
  uint64 total = 0;
  for (int i = 0; i < entry.file_parts_size(); i++) {
    total += entry.file_parts(i).length();
    if (total >= in.size()) {
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
  if (total < in.size()) {
    entry.add_file_parts()->set_length(in.size() - total);
  }

  // Write out new version of entry.
  context->PutEntry(in.path(), entry);
}

void MetadataStore::Write_Internal(
    ExecutionContext* context,
    const MetadataAction::WriteInput& in,
    MetadataAction::WriteOutput* out) {
  LOG(FATAL) << "not implemented";
}

void MetadataStore::Append_Internal(
    ExecutionContext* context,
    const MetadataAction::AppendInput& in,
    MetadataAction::AppendOutput* out) {
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // Only append to DATA files.
  if (entry.type() != DATA) {
    out->set_success(false);
    out->add_errors(MetadataAction::WrongFileType);
    return;
  }

  // TODO(agt): Check permissions.

  // Append data to end of file.
  for (int i = 0; i < in.data_size(); i++) {
    entry.add_file_parts()->CopyFrom(in.data(i));
  }

  // Write out new version of entry.
  context->PutEntry(in.path(), entry);
}

void MetadataStore::ChangePermissions_Internal(
    ExecutionContext* context,
    const MetadataAction::ChangePermissionsInput& in,
    MetadataAction::ChangePermissionsOutput* out) {
  LOG(FATAL) << "not implemented";
}

