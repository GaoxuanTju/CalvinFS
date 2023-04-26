// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Kun  Ren  <kun.ren@yale.edu>
//

#ifndef CALVIN_FS_CALVINFS_CLIENT_APP_H_
#define CALVIN_FS_CALVINFS_CLIENT_APP_H_
#define switch_uid 9999
#define operation_num 5000
#include <leveldb/env.h>
#include <iomanip>

#include "components/scheduler/scheduler.h"
#include "components/store/store.h"
#include "components/store/store_app.h"
#include "fs/block_log.h"
#include "fs/calvinfs.h"
#include "fs/metadata.pb.h"
#include "fs/metadata_store.h"
#include "machine/app/app.h"
#include "machine/machine.h"
#include <stack>
// add for file op
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <vector>
//
using std::make_pair;

class CalvinFSClientApp : public App
{
public:
  CalvinFSClientApp()
      : go_(true), going_(false), reporting_(false)
  {
  }
  virtual ~CalvinFSClientApp()
  {
    go_ = false;
    while (going_.load())
    {
    }
  }

  virtual void Start()
  { // gaoxuan --the function for start RenameExperiment

    action_count_ = 0;

    for (int i = 0; i < 20000000; i++)
    {
      random_data_.push_back(rand() % 256);
    }

    latencies_["touch"] = new AtomicQueue<double>();
    latencies_["mkdir"] = new AtomicQueue<double>();
    latencies_["append"] = new AtomicQueue<double>();
    latencies_["copy"] = new AtomicQueue<double>();
    latencies_["rename"] = new AtomicQueue<double>();
    latencies_["ls"] = new AtomicQueue<double>();
    latencies_["cat0"] = new AtomicQueue<double>();
    latencies_["cat1"] = new AtomicQueue<double>();
    latencies_["cat10"] = new AtomicQueue<double>();
    latencies_["cat100"] = new AtomicQueue<double>();

    config_ = new CalvinFSConfigMap(machine());
    replica_ = config_->LookupReplica(machine()->machine_id());
    blocks_ = reinterpret_cast<DistributedBlockStoreApp *>(
        machine()->GetApp("blockstore"));
    log_ = reinterpret_cast<BlockLogApp *>(machine()->GetApp("blocklog"));
    scheduler_ = reinterpret_cast<Scheduler *>(machine()->GetApp("scheduler"));
    metadata_ =
        reinterpret_cast<MetadataStore *>(
            reinterpret_cast<StoreApp *>(machine()->GetApp("metadata"))->store());
    // gaoxuan --metadata_is a MetadataStoreApp that App name is metadata
    Spin(1);

    capacity_ = kMaxCapacity; // gaoxuan --the amount of client

    switch (experiment)
    {
    case 0:
      FillExperiment(); // create实验
      break;

    case 1:
      ConflictingAppendExperiment();
      break;

    case 2:
      RandomAppendExperiment();
      break;

    case 3:
      CopyExperiment(); // copy实验
      break;

    case 4:

      RenameExperiment(); // rename实验
      break;

    case 5:
      LatencyExperimentReadFile();
      break;

    case 6:
      LatencyExperimentCreateFile();
      break;

    case 7:
      LatencyExperimentAppend();
      break;

    case 8:
      LatencyExperimentMix();
      break;

    case 9:
      LatencyExperimentRenameFile();
      break;

    case 10:
      CrashExperiment();
      break;

    case 11:
      DeleteExperiment();
      break;
    case 12:
      LsExperiment();
      break;
    }
  }

  virtual void HandleMessage(Header *header, MessageBuffer *message)
  {

    if (header->rpc() == "LOOKUP")
    {
      int id = header->uid(); // 看看uid是不是被交换机修改了
      if (id != switch_uid) // 这种情况是交换机要修改
      {
        header->set_data_ptr(header->data_ptr());
        int depth;                                              // 用来记录当前遍历到那个深度了
        string uid = IntToString(id);                           // 获取修改之后的uid
        depth = header->depth() - 1;                        // 获得交换机修改之后的深度
        string filename = header->split_string_from(depth); // 交换机是采用++的方式的
        string new_str;
        // 这里要将字符串中的空格删除
        for (int i = 0; i < filename.size(); i++)
        {
          if (filename[i] != ' ')
          {
            new_str += filename[i];
          }
        }
        string path = "/" + uid + "/" + new_str; // 要发出的结果
        MessageBuffer *serialized = GetMetadataEntry(path);      
        if (depth + 1 == header->from_length() || Dir_dep(path) > 2)
        { // 交换机匹配到了，而且一直匹配到最后一段位置
          // 要做的是获取直接获取元数据项，然后返回即可

          header->set_from(header->original_from()); //
          machine()->SendReplyMessage(header, serialized);
        }
        else
        {
           // 解析一下，没有结束，拼接一下路径，继续发出请求
          Action b;
          b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
          MetadataAction::LookupOutput out;
          out.ParseFromString(b.output());          
          if (path == "")
          {
            depth = 0;
          }
          else
          {
            depth = depth + 1;
          }
          string uid1 = out.entry().dir_contents(0);
          string LS_path;
          if(metadata_->path_type[path] == 0)
          {
            //父目录是树
            string file = header->split_string_from(depth);
            string str;
             // 这里要将字符串中的空格删除
            for (int i = 0; i < file.size(); i++)
            {
              if (file[i] != ' ')
              {
                str += file[i];
              }
            } 
            LS_path = "/" + uid1 + "/" + str;
            header->set_depth(depth); // 当前所在深度                      
          }
          else
          {
            //父目录是hash
            LS_path = path;
            for (int i = depth-1; i < header->from_length(); i++)
            {
              string filename = header->split_string_from(i);
              string new_str;
              // 这里要将字符串中的空格删除
              for (int i = 0; i < filename.size(); i++)
              {
                if (filename[i] != ' ')
                {
                  new_str += filename[i];
                }
              }
              LS_path = LS_path + "/" + new_str;
            }
            header->set_depth(header->from_length() - 1); // 为了统一上面深度，depth = depth + 1            
          }
          // 下面要对LS_path发lookup请求
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(LS_path)), config_->LookupReplica(machine()->machine_id()));
          // 这之前是发送lookup请求
          // 还是之前的header，只需要改路径，from, to就行
          // 第一次被修改过后，重新设置回来
          header->set_uid(switch_uid);
          header->set_from(machine()->machine_id());
          header->set_to(mds_machine);
          header->clear_misc_string();
          header->add_misc_string(LS_path.c_str(), strlen(LS_path.c_str()));
          Header* h = new Header();
          h->CopyFrom(*header);
          machine()->SendMessage(h, new MessageBuffer());         
        }
      }
      else
      {
        string path;
        MessageBuffer *serialized = GetMetadataEntry(path = header->misc_string(0));
        header->set_data_ptr(header->data_ptr());
        int depth; // 用来记录当前遍历到那个深度了
        if (path == "")
        {
          depth = 0;
        }
        else
        {
          depth = header->depth() + 1;
        }
        if (depth == header->from_length() || Dir_dep(path) > 2) // 是最后一段,将最后结果发回
        {
          header->set_from(header->original_from()); 
          machine()->SendReplyMessage(header, serialized);
        }
        else
        { 
          Action b;
          b.ParseFromArray((*serialized)[0].data(), (*serialized)[0].size());
          delete serialized;
          MetadataAction::LookupOutput out;
          out.ParseFromString(b.output());
          MetadataEntry entry = out.entry();
          string LS_path;
          string uid = entry.dir_contents(0);
          if (metadata_->path_type[path] == 0)
          {
            // 树类型
            string filename = header->split_string_from(depth);
            string new_str;
            // 这里要将字符串中的空格删除
            for (int i = 0; i < filename.size(); i++)
            {
              if (filename[i] != ' ')
              {
                new_str += filename[i];
              }
            }
            LS_path = "/" + uid + "/" + new_str;
            header->set_depth(depth); // 当前所在深度
          }
          else
          {
            LS_path = path;
            for (int i = depth; i < header->from_length(); i++)
            {
              string filename = header->split_string_from(i);
              string new_str;
              // 这里要将字符串中的空格删除
              for (int i = 0; i < filename.size(); i++)
              {
                if (filename[i] != ' ')
                {
                  new_str += filename[i];
                }
              }
              LS_path = LS_path + "/" + new_str;
            }
            header->set_depth(header->from_length() - 1); // 为了统一上面深度，depth = depth + 1
          }
          uint64 mds_machine = config_->LookupMetadataShard(config_->HashFileName(Slice(LS_path)), config_->LookupReplica(machine()->machine_id()));
          header->set_from(machine()->machine_id());
          header->set_to(mds_machine);
          header->clear_misc_string();
          header->add_misc_string(LS_path.c_str(), strlen(LS_path.c_str()));
          Header * h = new Header();
          h->CopyFrom(*header);
          delete header;
          machine()->SendMessage(h, new MessageBuffer());
        }
      }
    }
    // if (header->rpc() == "LOOKUP")
    // {
    //   machine()->SendReplyMessage(
    //       header,
    //       GetMetadataEntry(header->misc_string(0)));

    //   // EXTERNAL LS
    // }    
    else if (header->rpc() == "LS")
    {
      machine()->SendReplyMessage(header, LS(header->misc_string(0)));
      // EXTERNAL read file
    }
    else if (header->rpc() == "READ_FILE")
    {
      machine()->SendReplyMessage(header, ReadFile(header->misc_string(0)));

      // EXTERNAL file/dir creation
    }
    else if (header->rpc() == "CREATE_FILE")
    {

      machine()->SendReplyMessage(header, CreateFile(header->misc_string(0), header->misc_bool(0) ? DIR : DATA));
      // EXTERNAL file append
    }
    else if (header->rpc() == "DELETE_FILE")
    {
      string s1;
      machine()->SendReplyMessage(header, DeleteFile(
                                              s1 = header->misc_string(0),
                                              header->misc_bool(0) ? DIR : DATA));
      // 用于发送汇总请求的地方
      Header *temp = new Header();
      temp->set_from(header->from());
      temp->set_to(0);
      temp->set_type(Header::RPC);
      temp->set_app(name());
      temp->set_rpc("SUMMARY_DELETE");

      temp->add_misc_string(s1);
      machine()->SendMessage(temp, new MessageBuffer());

      // EXTERNAL file append
    }
    else if (header->rpc() == "APPEND")
    {
      machine()->SendReplyMessage(header, AppendStringToFile(
                                              (*message)[0],
                                              header->misc_string(0)));
      // EXTERNAL file copy
    }
    else if (header->rpc() == "COPY_FILE")
    {
      string s1, s2;
      machine()->SendReplyMessage(header, CopyFile(
                                              s1 = header->misc_string(0),
                                              s2 = header->misc_string(1)));
      // 用于发送汇总请求的地方
      Header *temp = new Header();
      temp->set_from(header->from());
      temp->set_to(0);
      temp->set_type(Header::RPC);
      temp->set_app(name());
      temp->set_rpc("SUMMARY_COPY");

      temp->add_misc_string(s1);
      temp->add_misc_string(s2);
      machine()->SendMessage(temp, new MessageBuffer());
      // EXTERNAL file copy
    }
    else if (header->rpc() == "RENAME_FILE")
    {

      string s1, s2;
      machine()->SendReplyMessage(header, RenameFile(
                                              s1 = header->misc_string(0),
                                              s2 = header->misc_string(1)));
      /*
        // 用于发送汇总请求的地方
        Header *temp = new Header();
        temp->set_from(header->from());
        temp->set_to(0);
        temp->set_type(Header::RPC);
        temp->set_app(name());
        temp->set_rpc("SUMMARY_RENAME");

        temp->add_misc_string(s1);
        temp->add_misc_string(s2);

        machine()->SendMessage(temp, new MessageBuffer());
  */
      // Callback for recording latency stats
    }
    else if (header->rpc() == "CB")
    {
      double end = GetTime();
      int misc_size = header->misc_string_size();
      string category = header->misc_string(misc_size - 1);
      if (category == "cat")
      {
        if ((*message)[0] == "metadata lookup error\n")
        {
          latencies_["cat0"]->Push(
              end - header->misc_double(0));
          delete header;
          delete message;
          return;
        }
        MetadataEntry result;
        result.ParseFromArray((*message)[0].data(), (*message)[0].size());
        CHECK(result.has_type());

        if (result.file_parts_size() == 0)
        {
          category.append("0");
        }
        else if (result.file_parts_size() == 1)
        {
          category.append("1");
        }
        else if (result.file_parts_size() <= 10)
        {
          category.append("10");
        }
        else
        {
          category.append("100");
        }
      }

      latencies_[category]->Push(end - header->misc_double(0));

      delete header;
      delete message;
    }
    else if (header->rpc() == "SUMMARY_CREATE")
    {                                       // gaoxuan --这里是我后添加的，为了汇总创建请求的内容
      string temp = header->misc_string(0); // 这个是需要的路径
      create_dir_tree(dir_tree, temp);
    }
    else if (header->rpc() == "SUMMARY_RENAME")
    {                                            // gaoxuan --这里是我后添加的，为了汇总创建请求的内容
      string temp_from = header->misc_string(0); // 这个是需要的源路径
      string temp_to = header->misc_string(1);
      rename_dir_tree(dir_tree, temp_from, temp_to);
    }
    else if (header->rpc() == "SUMMARY_COPY")
    {                                            // gaoxuan --这里是我后添加的，为了汇总创建请求的内容
      string temp_from = header->misc_string(0); // 这个是需要的源路径
      string temp_to = header->misc_string(1);
      copy_dir_tree(dir_tree, temp_from, temp_to);
    }
    else if (header->rpc() == "SUMMARY_DELETE")
    {                                       // gaoxuan --这里是我后添加的，为了汇总创建请求的内容
      string temp = header->misc_string(0); // 这个是需要的路径
      delete_dir_tree(dir_tree, temp);
    }
    else
    {
      LOG(FATAL) << "unknown RPC: " << header->rpc();
    }
  }

  uint64 RandomBlockSize()
  {
    return 1000000 / (1 + rand() % 9999);
  }

  void FillExperiment()
  {
    Spin(1);
    metadata_->Init_from_txt("/home/wenxin/CalvinFS/src/fs/Init.txt");
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);
    double start = GetTime();

    // LOG(ERROR)<<"create dir "<<path;
    for (int i = 0; i < operation_num; i++)
    {
      string path = "/z" + IntToString(i);
      BackgroundCreateFile(path);
    }

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Created " << operation_num << " files. Elapsed time: "
               << (GetTime() - start) << " seconds";
  }

  void ConflictingAppendExperiment()
  {
    int files = 2;
    // Create 1k top-level files.
    if (machine()->machine_id() == 0)
    {
      for (int i = 0; i < files; i++)
      {
        BackgroundCreateFile("/f" + IntToString(i));
      }
      // Wait for all operations to finish.
      while (capacity_.load() < kMaxCapacity)
      {
        usleep(10);
      }
    }

    // 1k appends to random files.
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);
    double start = GetTime();
    int iterations = 5;
    for (int a = 0; a < iterations; a++)
    {
      for (int i = 0; i < 1000; i++)
      {
        // Append.
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            "/f" + IntToString(rand() % files));
      }
      LOG(ERROR) << "[" << machine()->machine_id() << "] "
                 << "CAppendExperiment progress: " << a + 1 << "/" << iterations;
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << iterations << "k conflicting appends. Elapsed time: "
               << (GetTime() - start) << " seconds";
  }
  void RandomAppendExperiment()
  {
    // Create M * 1k top-level files.
    for (int i = 0; i < 100; i++)
    {
      BackgroundCreateFile(
          "/f" + UInt64ToString(machine()->machine_id()) + "." + IntToString(i));
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }

    // 1k appends to random files.
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);
    double start = GetTime();
    int iterations = 5;
    for (int a = 0; a < iterations; a++)
    {
      for (int i = 0; i < 1000; i++)
      {
        // Append.
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            "/f" + IntToString(rand() % 100));
      }
      LOG(ERROR) << "[" << machine()->machine_id() << "] "
                 << "RAppendExperiment progress: " << a + 1 << "/" << iterations;
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << iterations << "k random appends. Elapsed time: "
               << (GetTime() - start) << " seconds";
  }

  string RandomFile()
  {
    return "/a" + IntToString(rand() % machine()->config().size()) +
           "/b" + IntToString(rand() % 100) + "/c";
  }

  void LatencyExperimentSetup()
  {
    Spin(1);
    metadata_->InitSmall();
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    string tld("/a" + IntToString(machine()->machine_id()));

    // Append to some files.
    for (int i = 0; i < 1000; i++)
    {
      while (rand() % 3 == 0)
      {
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            tld + "/b" + IntToString(i) + "/c");
      }
      if (i % 100 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE prep progress C: " << i / 100 << "/" << 10;
      }
    }
    Spin(1);

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    LOG(ERROR) << "[" << machine()->machine_id() << "] LE prep complete";
  }

  void LatencyExperimentReadFile()
  {
    // Setup.
    LatencyExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Begin mix of operations.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < 1000; i++)
    {
      BackgroundReadFile(RandomFile());

      if (i % 10 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE test progress: " << i / 10 << "/" << 100;
      }
    }

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "ReadFile workload completed. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }

  void LatencyExperimentCreateFile()
  {
    // Setup.
    /**LatencyExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Begin mix of operations.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < 10000; i++) {
      BackgroundCreateFile(
            "/a" + IntToString(machine()->machine_id()) +
            "/b" + IntToString(rand() % 1000) +
            "/x" + UInt64ToString(1000 + machine()->GetGUID()));

      if (i % 100 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE test progress: " << i / 100 << "/" << 100;
      }
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "CreateFile workload completed. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();**/

    Spin(1);
    metadata_->Init();
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    string tld("/a" + UInt64ToString(machine()->machine_id()));
    int dirs = 1000;
    int files = 10;

    // Put files into second-level dir.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < files; i++)
    {
      string file = "/d" + IntToString(i);
      for (int j = 0; j < dirs; j++)
      {
        BackgroundCreateFile(tld + "/b" + IntToString(j) + file);
      }
      LOG(ERROR) << "[" << machine()->machine_id() << "] "
                 << "Added file d" << i << " to " << dirs << " dirs";
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Created " << dirs * files << " files. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }

  void LatencyExperimentAppend()
  {
    // Setup.
    LatencyExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Begin mix of operations.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < 1000; i++)
    {
      BackgroundAppendStringToFile(RandomData(RandomBlockSize()), RandomFile());

      if (i % 10 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE test progress: " << i / 10 << "/" << 100;
      }
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Append workload completed. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }

  void LatencyExperimentMix()
  {
    // Setup.
    LatencyExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Begin mix of operations.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < 1000; i++)
    {
      int seed = rand() % 100;

      // 60% read operations
      if (seed < 60)
      {
        BackgroundReadFile(RandomFile());

        // 10% file creation operations
      }
      else if (seed < 70)
      {
        BackgroundCreateFile(
            "/a" + IntToString(rand() % machine()->config().size()) +
            "/b" + IntToString(rand() % 100) +
            "/x" + UInt64ToString(1000 + machine()->GetGUID()));

        // 30% append operations
      }
      else
      {
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            RandomFile());
      }
      if (i % 10 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE test progress: " << i / 10 << "/" << 100;
      }
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Mixed workload completed. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }
  void CrashExperimentSetup()
  {
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Create top-level dir.
    string tld("/a" + IntToString(machine()->machine_id()));
    CreateFile(tld, DIR);
    Spin(1);

    // Create subdirs.
    for (int i = 0; i < 1000; i++)
    {
      BackgroundCreateFile(tld + "/b" + IntToString(i), DIR);
      if (i % 10 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "CE prep progress A: " << i / 10 << "/" << 100;
      }
    }
    Spin(1);

    // Create files.
    for (int i = 0; i < 1000; i++)
    {
      BackgroundCreateFile(tld + "/b" + IntToString(i) + "/c", DATA);
      if (i % 10 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "CE prep progress B: " << i / 10 << "/" << 100;
      }
    }
    Spin(1);

    // Append to some files.
    for (int i = 0; i < 1000; i++)
    {
      while (rand() % 2 == 0)
      {
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            tld + "/b" + IntToString(i) + "/c");
      }
      if (i % 10 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "CE prep progress C: " << i / 10 << "/" << 100;
      }
    }
    Spin(1);

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    LOG(ERROR) << "[" << machine()->machine_id() << "] CE prep complete";
  }

  string RandomFileC()
  {
    return "/a" + IntToString(rand() % machine()->config().size()) +
           "/b" + IntToString(rand() % 1000) + "/c";
  }

  void CrashExperiment()
  {
    // Setup.
    CrashExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    reporting_ = true;
    double start = GetTime();
    double tick = start + 1;
    int tickid = 0;
    while (GetTime() < start + 60)
    {
      double t = GetTime();
      if (t > tick)
      {
        string report("\n");
        for (auto it = latencies_.begin(); it != latencies_.end(); ++it)
        {
          vector<double> v;
          double d;
          while (it->second->Pop(&d))
          {
            v.push_back(d);
          }
          sort(v.begin(), v.end());
          if (!v.empty())
          {
            report.append(
                "[" + UInt64ToString(machine()->machine_id()) + "] " +
                IntToString(tickid) + " " +
                IntToString(v.size()) + " " + it->first + "-count\n");
            report.append(
                "[" + UInt64ToString(machine()->machine_id()) + "] " +
                IntToString(tickid) + " " +
                DoubleToString(v[v.size() / 2]) + " " + it->first + "-median\n");
            report.append(
                "[" + UInt64ToString(machine()->machine_id()) + "] " +
                IntToString(tickid) + " " +
                DoubleToString(v[v.size() * 99 / 100]) + " " + it->first + "-99th-percentile\n");
          }
        }
        LOG(ERROR) << report;

        if (tickid == 30)
        {
          if (replica_ == 1)
          {
            LOG(ERROR) << "[" + UInt64ToString(machine()->machine_id()) + "] "
                       << "KABOOM!";
            exit(0);
          }
          else
          {
            capacity_ += kMaxCapacity / 2;
          }
        }
        tick += 1;
        tickid++;
      }

      if (machine()->machine_id() % 3 == 0)
      {
        BackgroundReadFile(RandomFileC());
      }
      else if (machine()->machine_id() % 3 == 1)
      {
        BackgroundCreateFile(
            "/a" + IntToString(rand() % machine()->config().size()) +
            "/b" + IntToString(rand() % 100) +
            "/x" + UInt64ToString(1000 + machine()->GetGUID()));
      }
      else
      {
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            RandomFileC());
      }
    }
  }

  void CopyExperiment()
  {
    Spin(1);
    dir_tree = new BTNode;
    metadata_->Init(dir_tree);
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    double start = GetTime();
    for (int i = 0; i < 1; i++)
    {
      BackgroundCopyFile("/a" + IntToString(machine()->machine_id()),
                         "/a" + IntToString(rand() % machine()->config().size()) + "/b" + IntToString(rand() % 3));
      // 上面的目的路径感觉不太对，怎么会到一个/dxxx呢，copy肯定是拷贝到一个目录下呀
      if (i % 100 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "Test progress : " << i / 100 << "/" << 5;
      }
    }

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }

    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Copyed "
               << "500 files. Elapsed time: "
               << (GetTime() - start) << " seconds";
    Spin(1);
    print_dir_tree(dir_tree);
  }

  void RenameExperiment()
  {
    Spin(1);
    // dir_tree = new BTNode;
    // metadata_->Init_tree_20(dir_tree);
    metadata_->Init_from_txt("/home/wenxin/CalvinFS/src/fs/Init.txt");
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // double start = GetTime();
    // string from_path;
    // string to_path;

    // from_path = "/a" + IntToString(0);
    // to_path = "/a" + IntToString(1) + "/b" + IntToString(1) + "/d" + IntToString(machine()->GetGUID());
    // uint64 from_id = config_->LookupMetadataShard(config_->HashFileName(from_path), config_->LookupReplica(machine()->machine_id()));
    // uint64 to_id = config_->LookupMetadataShard(config_->HashFileName(to_path), config_->LookupReplica(machine()->machine_id()));
    // LOG(ERROR) << from_path << " in machine[" << from_id << "]  renamed to   " << to_path << " in machine[" << to_id << "]";
    // BackgroundRenameFile(from_path, to_path);

    // // Wait for all operations to finish.
    // while (capacity_.load() < kMaxCapacity)
    // {
    //   usleep(10);
    //   // LOG(ERROR)<<capacity_.load();
    // }

    // // Report.

    // LOG(ERROR) << "Renamed "
    //            << "1 files. Elapsed time:"
    //            << (GetTime() - start) << " seconds";

  }
  void DeleteExperiment()
  { // gaoxuan --删除文件的实验
    Spin(1);
    dir_tree = new BTNode;
    metadata_->Init(dir_tree);
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    double start = GetTime();
    string from_path;
    for (int j = 0; j < 3; j++)
    {
      for (int k = 0; k < 2; k++)
      {
        string from_path = "/a" + IntToString(machine()->machine_id()) + "/b" + IntToString(j) + "/c" + IntToString(k);
        BackgroundDeleteFile(from_path);
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "Deleted file " << from_path;
      }
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Deleted " << 10 << " files. Elapsed time: "
               << (GetTime() - start) << " seconds";
    Spin(1);
    print_dir_tree(dir_tree);
  }
  void LsExperiment()
  { 
    Spin(1);
    metadata_->Init_from_txt("/home/wenxin/CalvinFS/src/fs/Init.txt");
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);
    // double start = GetTime();
    // string path1 = "/u0";
    // LOG(ERROR)<<"LS path: "<<path1;
    // for (int j = 0; j < operation_num; j++)
    // {
    //   BackgroundLS(path1);
    // }

    // while (capacity_.load() < kMaxCapacity)
    // {
    //   usleep(10);
    // //  LOG(ERROR)<<capacity_.load();
    // }
    // // Report.
    // double end = GetTime();
    // LOG(ERROR) << "[" << machine()->machine_id() << "] "
    //            << "LS " << operation_num << " files. Elapsed time: "
    //            << end - start << " seconds";
    }

  void LatencyExperimentRenameFile()
  {

    Spin(1);
    metadata_->Init();
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    reporting_ = true;
    double start = GetTime();

    for (int j = 0; j < 250; j++)
    {
      int a1 = rand() % 1000;
      int a2 = rand() % 1000;
      while (a2 == a1)
      {
        a2 = rand() % 1000;
      }
      BackgroundRenameFile("/a" + IntToString(machine()->machine_id()) + "/b" + IntToString(a1) + "/c" + IntToString(j),
                           "/a" + IntToString(rand() % machine()->config().size()) + "/b" + IntToString(a2) + "/d" + IntToString(machine()->GetGUID()));

      if (j % 50 == 0)
      {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "Test progress : " << j / 50 << "/" << 5;
      }
    }

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity)
    {
      usleep(10);
    }

    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Renamed "
               << "250 files. Elapsed time:may here "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }

  void Report()
  {
    string report;
    for (auto it = latencies_.begin(); it != latencies_.end(); ++it)
    {
      double d;
      while (it->second->Pop(&d))
      {
        report.append(it->first + " " + DoubleToString(d) + "\n");
      }
    }
    string filename =
        "/tmp/report." + UInt64ToString(machine()->machine_id());
    leveldb::Status s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        report,
        filename);

    if (s.ok())
    {
      LOG(ERROR) << "reporting latencies to " << filename;
    }
    else
    {
      LOG(ERROR) << "failed to save report: " << filename;
    }
  }

  void rename_dir_tree(BTNode *&dir_tree, string from_path, string to_path);
  void copy_dir_tree(BTNode *&dir_tree, string from_path, string to_path);
  void create_dir_tree(BTNode *&dir_tree, string path);
  void delete_dir_tree(BTNode *&dir_tree, string path);
  BTNode *find_path(BTNode *dir_tree, string path, BTNode *&pre);
  // Caller takes ownership of returned MessageBuffers.
  // Returns serialized MetadataEntry protobuf.
  MessageBuffer *GetMetadataEntry(const Slice &path);
  // Returns client-side printable output.
  MessageBuffer *CreateFile(const Slice &path, FileType type = DATA);
  MessageBuffer *AppendStringToFile(const Slice &data, const Slice &path);
  MessageBuffer *ReadFile(const Slice &path);
  int Dir_dep(const string &path); // 获取深度
  MessageBuffer *LS(const Slice &path);
  MessageBuffer *CopyFile(const Slice &from_path, const Slice &to_path);
  MessageBuffer *RenameFile(const Slice &from_path, const Slice &to_path);
  MessageBuffer *DeleteFile(const Slice &delete_path, FileType type = DATA);

  void BackgroundCreateFile(const Slice &path, FileType type = DIR)
  {
    Header *header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("CREATE_FILE");
    header->add_misc_bool(type == DIR); // DIR = true, DATA = false
    header->add_misc_string(path.data(), path.size());
    if (reporting_ && rand() % 2 == 0)
    {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string((type == DIR) ? "mkdir" : "touch");
      header->add_misc_double(GetTime());
    }
    else
    {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0)
      {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }
  void BackgroundDeleteFile(const Slice &path, FileType type = DATA)
  {
    Header *header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to((machine()->machine_id() + 1) % 2);
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("DELETE_FILE");
    header->add_misc_bool(type == DIR); // DIR = true, DATA = false
    header->add_misc_string(path.data(), path.size());
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
    // 现在flag中存放的就是子串的数量
    header->set_from_length(flag); // 设置拆分后的实际子串占据的格子数量
    while (flag != 8)
    {
      string temp = "     ";               // 用五个空格填充一下
      header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
      flag++;                              // 拆分的字符串数量++
    }
    if (reporting_ && rand() % 2 == 0)
    {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string((type == DIR) ? "mkdir" : "touch");
      header->add_misc_double(GetTime());
    }
    else
    {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0)
      {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }

  void BackgroundAppendStringToFile(const Slice &data, const Slice &path)
  {
    Header *header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to((machine()->machine_id() + 1) % 2);
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("APPEND");
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
    // 现在flag中存放的就是子串的实际数量
    header->set_from_length(flag);
    while (flag != 8)
    {
      string temp = "     ";               // 用五个空格填充一下
      header->add_split_string_from(temp); // 将拆出来的子串加到header里面去
      flag++;                              // 拆分的字符串数量++
    }

    // 这一行之前是gaoxuan添加的

    if (reporting_ && rand() % 2 == 0)
    {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("append");
      header->add_misc_double(GetTime());
    }
    else
    {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0)
      {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer(data));
  }

  void BackgroundReadFile(const Slice &path)
  {
    Header *header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to((machine()->machine_id() + 1) % 2);
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("READ_FILE");
    header->add_misc_string(path.data(), path.size());
    // gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

    // 第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用空格填充上
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

    if (reporting_ && rand() % 2 == 0)
    {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("cat");
      header->add_misc_double(GetTime());
    }
    else
    {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0)
      {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }

  void BackgroundLS(const Slice &path)
  {
    Header *header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    // LOG(ERROR)<<"LS from "<<machine()->machine_id()<<" to "<<machine()->machine_id();
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("LS");
    header->add_misc_string(path.data(), path.size());
    if (reporting_ && rand() % 2 == 0)
    {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("ls");
      header->add_misc_double(GetTime());
    }
    else
    {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0)
      {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }

    machine()->SendMessage(header, new MessageBuffer());
  }

  void BackgroundCopyFile(const Slice &from_path, const Slice &to_path)
  {
    Header *header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to((machine()->machine_id() + 1) % 2);
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("COPY_FILE");
    header->add_misc_string(from_path.data(), from_path.size());
    header->add_misc_string(to_path.data(), to_path.size());
    // gaoxuan --在这里发出消息之前，把from_path.data()和to_path.data()拆分一下

    // 第一步：将from_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用五个空格填充上
    // 拆分的算法，遇到一个/就把之前的字符串放进去
    // 将拆分后的元素添加去的方法：header->add_split_string(拆分的字符串)
    int flag = 0;       // 用来标识此时split_string 里面有多少子串
    char pattern = '/'; // 根据/进行字符串拆分

    string temp_from = from_path.data();
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

    int flag1 = 0;
    // 第二步：将to_path.data()拆分放进split_string里面，拆完后，不够八个格子的，使用五个空格填充上
    string temp_to = to_path.data();
    temp_to = temp_to.substr(1, temp_to.size()); // 这一行是为了去除最前面的/
    temp_to = temp_to + pattern;                 // 在最后面添加一个/便于处理
    int pos1 = temp_to.find(pattern);            // 找到第一个/的位置
    while (pos1 != std::string::npos)            // 循环不断找/，找到一个拆分一次
    {
      string temp1 = temp_to.substr(0, pos1); // temp里面就是拆分出来的第一个子串
      string temp = temp1;
      for (int i = temp.size(); i < 5; i++)
      {
        temp = temp + " ";
      }
      header->add_split_string_to(temp); // 将拆出来的子串加到header里面去
      flag1++;                           // 拆分的字符串数量++
      temp_to = temp_to.substr(pos1 + 1, temp_to.size());
      pos1 = temp_to.find(pattern);
    }
    header->set_to_length(flag1);
    while (flag1 != 8)
    {
      string temp = "     ";             // 用五个空格填充一下
      header->add_split_string_to(temp); // 将拆出来的子串加到header里面去
      flag1++;                           // 拆分的字符串数量++
    }

    // 这一行之前是gaoxuan添加的

    if (reporting_ && rand() % 2 == 0)
    {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("copy");
      header->add_misc_double(GetTime());
    }
    else
    {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0)
      {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }

  void BackgroundRenameFile(const Slice &from_path, const Slice &to_path)
  {

    Header *header = new Header();
    // LOG(ERROR)<<"in backgroundrename :: "<<from_path.data()<<" and "<<to_path.data();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("RENAME_FILE"); // gaoxuan --call RenameFile() in calvinfs_client_app.cc
    header->add_misc_string(from_path.data(), from_path.size());
    header->add_misc_string(to_path.data(), to_path.size());

    if (reporting_ && rand() % 2 == 0)
    { // gaoxuan --this branch will never be executed in RenameExperiment(),reporting_ is false
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("rename");
      header->add_misc_double(GetTime());
    }
    else
    {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0)
      {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }

    machine()->SendMessage(header, new MessageBuffer());
  }

  // gaoxuan --这个函数用来输出一下目录树
  void preorder(BTNode *root, string path)
  {
    if (root != NULL)
    {
      string s = path + root->path;
      LOG(ERROR) << s;
      preorder(root->sibling, path);
      if (s == "/")
      {
        preorder(root->child, s);
      }
      else
      {
        preorder(root->child, s + "/");
      }
    }
  }
  void print_dir_tree(BTNode *dir_tree)
  {
    if (dir_tree == NULL)
    {
      LOG(ERROR) << "Empty tree";
    }
    else
    {
      preorder(dir_tree, "/");
    }
    // 采用先序遍历就好，不过是先遍历右子树那种方式
  }

  inline Slice RandomData(uint64 size)
  {
    uint64 start = rand() % (random_data_.size() - size);
    return Slice(random_data_.data() + start, size);
  }

  void set_start_time(double t) { start_time_ = t; }
  double start_time_;

  void set_experiment(int e, int c)
  {
    experiment = e;
    kMaxCapacity = c;
  }
  int experiment;
  int kMaxCapacity;

  atomic<int> action_count_;
  atomic<int> capacity_;

  string random_data_;

  map<string, AtomicQueue<double> *> latencies_;

  atomic<bool> go_;
  atomic<bool> going_;
  bool reporting_;

  // Configuration for this CalvinFS instance.
  CalvinFSConfigMap *config_;

  // Local replica id.
  uint64 replica_;

  // Block store.
  DistributedBlockStoreApp *blocks_;

  // BlockLogApp for appending new requests.
  BlockLogApp *log_;

  // Scheduler for getting safe version.
  Scheduler *scheduler_;

  // MetadataStore for getting RWSets.
  MetadataStore *metadata_;

  // gaoxuan 这里加个指针，多叉树的根指针，初始化为空。在任何实验开始的时候，我们调用metadata_->Init()的时候，作为参数传进去，然后init执行过程中就直接顺便构建
  // 每个操作更改就在这里面handlemessage里面些就好
  BTNode *dir_tree;
};

#endif // CALVIN_FS_CALVINFS_CLIENT_APP_H_
