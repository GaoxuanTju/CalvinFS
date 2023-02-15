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
    for (int i = 0; i < action->readset_size(); i++) {//gaoxuan --now handle the read/write set passed from GetRWs
      uint64 mds = config_->HashFileName(action->readset(i));
      uint64 machine = config_->LookupMetadataShard(mds, replica_);//gaoxuan --get right machine of the path
      if (machine == machine_->machine_id()) {//gaoxuan --if the path in read/write set is local,put it's metadataentry into map reads_,key is path,value is entry
        // Local read.
        if (!store_->Get(action->readset(i),
                         version_,
                         &reads_[action->readset(i)])) {
          reads_.erase(action->readset(i));
        }
        reader_ = true;
      } else {//gaoxuan --this machine is that we want to read. Put it into set remote_readers
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
      //gaoxuan --why do we want to read but we need to broadcast to writers?
      for (auto it = remote_writers.begin(); it != remote_writers.end(); ++it) {
        Header* header = new Header();
        header->set_from(machine_->machine_id());
        header->set_to(*it);
        header->set_type(Header::DATA);//gaoxuan --deliver packets directly,and the specific logic refers to lock
        header->set_data_channel("action-" + UInt64ToString(version_));//gaoxuan --Can this version make a difference ? 
        machine_->SendMessage(header, new MessageBuffer(local_reads));
      }
    }

    // If any writes will be performed locally, wait for all remote reads.
    if (writer_) {
      // Get channel.
      AtomicQueue<MessageBuffer*>* channel =
          machine_->DataChannel("action-" + UInt64ToString(version_));
      for (uint32 i = 0; i < remote_readers.size(); i++) {//gaoxuan --in this part we get remote entry
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
//use rfind to get the index of last '/',so the parentdir is for index 0 to index of last '/'
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
//get the name of file, without the absolute path
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


void MetadataStore::getLOOKUP(string path)
{
    std::stack <string> stack1;//gaoxuan --the stack is used for tranversing  the file tree
    stack1.push(path);//gaoxuan --we want to tranverse all children of this path to add read/write set
  
    while (!stack1.empty()) 
    {
      string top = stack1.top(); // gaoxuan --get the top
      stack1.pop();              // gaoxuan --pop the top
      if(top.find("d") != std::string::npos)//gaoxuan --this 10 is used to limit output.Because it will be too many output without limitation 
      {
            LOG(ERROR)<<"renamed file is: "<<top;
      }
      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(top)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app("client");
      header->set_rpc("LOOKUP");
      header->add_misc_string(top.c_str(), strlen(top.c_str()));
    //gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

    //第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用四个空格填充上
    //拆分的算法，遇到一个/就把之前的字符串放进去
    //将拆分后的元素添加去的方法：header->add_split_string(拆分的字符串)
    int flag = 0 ;//用来标识此时split_string 里面有多少子串
    char pattern = '/' ;//根据/进行字符串拆分

    string temp_from = top.c_str(); 
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
          
          string full_path =top + "/" + out.entry().dir_contents(i);
          stack1.push(full_path);
          
          
        }
      }     
    }
    LOG(ERROR)<<"finished LOOKUP!";
}
void MetadataStore::GetRWSets(Action* action) {//gaoxuan --this function is called by RameFile() for RenameExperiment
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

  }else if (type == MetadataAction::RENAME) {// the version of gaoxuan
    //gaoxuan --this part is rewrited by gaoxuan
    //gaoxuan --add read/write set in the way of DFS
    MetadataAction::RenameInput in;
    in.ParseFromString(action->input());
    std::stack <string> stack1;//gaoxuan --the stack is used for tranversing  the file tree
    stack1.push(in.from_path());//gaoxuan --we want to tranverse all children of this path to add read/write set
   string To_path = in.to_path();
    while (!stack1.empty()) 
    {
      string top = stack1.top(); // get the top
      stack1.pop();              // pop the top
      action->add_readset(top);
      action->add_writeset(top);
      
      string s = top.substr(in.from_path().size());//gaoxuan --s is used to get the path without from_path 
      To_path = in.to_path()+s;//gaoxuan --To_path is the path that needed to add writeset
      action->add_writeset(To_path);

      uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(top)), config_->LookupReplica(machine_->machine_id()));
      Header *header = new Header();
      header->set_from(machine_->machine_id());
      header->set_to(mds_machine);
      header->set_type(Header::RPC);
      header->set_app(getAPPname());
      header->set_rpc("LOOKUP");
      header->add_misc_string(top.c_str(), strlen(top.c_str()));
    //gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

    //第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用四个空格填充上
    //拆分的算法，遇到一个/就把之前的字符串放进去
    //将拆分后的元素添加去的方法：header->add_split_string(拆分的字符串)
    int flag = 0 ;//用来标识此时split_string 里面有多少子串
    char pattern = '/' ;//根据/进行字符串拆分

    string temp_from = top.c_str(); 
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
          string full_path =top + "/" + out.entry().dir_contents(i);
          stack1.push(full_path);
        
        }
      }     
    }
    //gaoxuan --in case that add read/write set repeatedly when form_path and to_path have same parent dir
    if(ParentDir(in.from_path())==ParentDir(in.to_path()))
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
  else if (type == MetadataAction::LOOKUP) {
    MetadataAction::LookupInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

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
  //gaoxuan --this part will be executed by scheduler after action has beed append to log
  // Prepare by performing all reads.
  ExecutionContext* context;
  if (machine_ == NULL) {
    context = new ExecutionContext(store_, action);
  } else {//gaoxuan --we can confirm we get all entry we want,because in DistributedExecutionContext has this logic
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
    //LOG(ERROR)<<"Run is executing!";
    MetadataAction::RenameInput in;
    MetadataAction::RenameOutput out;
    in.ParseFromString(action->input());
    //LOG(ERROR)<<"In Run :: "<<in.from_path()<<" and "<<in.to_path();
    Rename_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::LOOKUP) {
    MetadataAction::LookupInput in;
    MetadataAction::LookupOutput out;
   // LOG(ERROR)<<"The logic of LOOKUP in Run ";
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
    MetadataAction::RenameOutput* out) {//gaoxuan --this function wiil be executed when running RenameExperiment
  // Currently only support Copy: (non-recursive: only succeeds for DATA files and EMPTY directory)

//gaoxuan --now consider how to modify the logic of rename with correct context
  //LOG(ERROR)<<"Rename_internal is Executing!";
  MetadataEntry from_entry;//gaoxuan --get from_path's entry to check if it's existed,put it into from_entry
  if (!context->GetEntry(in.from_path(), &from_entry)) {
    // File doesn't exist!
    LOG(ERROR)<<"File doesn't exist!";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_from_path = ParentDir(in.from_path());
  MetadataEntry parent_from_entry;
  if (!context->GetEntry(parent_from_path, &parent_from_entry)) {
    // File doesn't exist!
    LOG(ERROR)<<"From_Parent File doesn't exist!";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_to_path = ParentDir(in.to_path());
  MetadataEntry parent_to_entry;
  if (!context->GetEntry(parent_to_path, &parent_to_entry)) {
    // File doesn't exist!
    LOG(ERROR)<<"To_Parent File doesn't exist!";
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }
 
  // If file already exists, fail.
  //gaoxuan --check if exist a file with same name in the Parent dir of to_path 
  string to_filename = FileName(in.to_path());
  for (int i = 0; i < parent_to_entry.dir_contents_size(); i++) {
    if (parent_to_entry.dir_contents(i) == to_filename) {
      LOG(ERROR)<<"file already exists, fail.";
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }
//gaoxuan --the part above is used to check if we can rename
//gaoxuan --in the following part we should change it to a loop
  // Update to_parent (add new dir content)
  parent_to_entry.add_dir_contents(to_filename);
  context->PutEntry(parent_to_path, parent_to_entry);
  
  if((from_entry.type()==DIR)&&(from_entry.dir_contents_size()!=0))//gaoxuan --only if the object we want to rename is DIR we need to loop,if its a file we don't need loop
  {
  //gaoxuan --use BFS to add new metadata entry 
      std::queue<string> queue1; 
      string root = in.to_path();
      queue1.push(root); 
      string from_path =in.from_path();//gaoxuan --the path used to copyEntry
      while (!queue1.empty()) { 
          string front = queue1.front();//gaoxuan --get the front in queue
          queue1.pop();         
          //Add Entry
          MetadataEntry to_entry1;//gaoxuan --the entry which will be added
          MetadataEntry from_entry1;//gaoxuan --the entry which will be used to copy to to_entry
          context->GetEntry(from_path, &from_entry1);
          to_entry1.CopyFrom(from_entry1);
          context->PutEntry(front, to_entry1);
          //gaoxuan --this part is used to delete the old entry
          // Update from_parent(Find file and remove it from parent directory.)
          string from_filename = FileName(from_path);
          //get the entry of parent of from_path
          string parent_from_path1 = ParentDir(from_path);
          MetadataEntry parent_from_entry1;
          context->GetEntry(parent_from_path1, &parent_from_entry1);

          for (int i = 0; i < parent_from_entry1.dir_contents_size(); i++) {
            if (parent_from_entry1.dir_contents(i) == from_filename) {
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
          //gaoxuan --this part is used to delete the old entry

          if (to_entry1.type() == DIR) {
            
            for (int i = 0; i < to_entry1.dir_contents_size(); i++) {
              
              string full_path = front+"/"+to_entry1.dir_contents(i);
              queue1.push(full_path);
            }  
          }
          
          if(!queue1.empty())
          {
            from_path = in.from_path() + queue1.front().substr(in.to_path().size());
          } 
      }

  } else
  {//gaoxuan --empty dir or file RENAME opretaion
          MetadataEntry to_entry1;//gaoxuan --the entry which will be added
          MetadataEntry from_entry1;//gaoxuan --the entry which will be used to copy to to_entry
          context->GetEntry(in.from_path(), &from_entry1);
          to_entry1.CopyFrom(from_entry1);
          context->PutEntry(in.to_path(), to_entry1);
          //gaoxuan --this part is used to delete the old entry
          // Update from_parent(Find file and remove it from parent directory.)
          string from_filename = FileName(in.from_path());
          //get the entry of parent of from_path
          string parent_from_path1 = ParentDir(in.from_path());
          MetadataEntry parent_from_entry1;
          context->GetEntry(parent_from_path1, &parent_from_entry1);

          for (int i = 0; i < parent_from_entry1.dir_contents_size(); i++) {
            if (parent_from_entry1.dir_contents(i) == from_filename) {
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

