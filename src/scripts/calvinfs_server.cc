// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun  Ren  <kun.ren@yale.edu>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_config.h"
#include "machine/machine.h"
#include "components/log/log.h"
#include "components/log/log_app.h"
#include "components/log/paxos.h"
#include "components/store/store.h"
#include "components/store/store_app.h"
#include "components/scheduler/scheduler.h"
#include "components/scheduler/locking_scheduler.h"
#include "fs/batch.pb.h"
#include "fs/block_log.h"
#include "fs/block_store.h"
#include "fs/calvinfs.h"
#include "fs/calvinfs_client_app.h"
#include "fs/metadata_store.h"
#include "fs/metadata.pb.h"
#include "scripts/script_utils.h"

//gaoxuan --这个DEFINE_bool之类的是gflags这个外部库提供的功能，就是能够从命令行接受一些参数
//接受一部分启动时传来的参数bin/scripts/cluster --command="start" --experiment=4  --clients=100 --max_active=1000 --max_running=100 --local_percentage=100
DEFINE_bool(calvin_version, false, "Print Calvin version information");
DEFINE_string(binary, "calvinfs_server", "Calvin binary executable program");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_int32(machine_id, 0, "machine id");
DEFINE_double(time, 0, "start time");
DEFINE_int32(experiment, 0, "experiment that you want to run");
DEFINE_int32(clients, 20, "number of concurrent clients on each machine");
DEFINE_int32(max_active, 1000, "max active actions for locking scheduler");
DEFINE_int32(max_running, 100, "max running actions for locking scheduler");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  // Print Calvin version
  if (FLAGS_calvin_version) {//gaoxuan --这个没被执行
    // Check whether Calvin have been running
    if (is_process_exist((char *)FLAGS_binary.c_str()) == true) {
      return -2;
    } else {
      printf("Machine %d: (CalvinFS) 0.1 (c) Yale University 2016.\n",
             (int)FLAGS_machine_id);
      return 0;
    }
  }
 //gaoxuan --怎么直接输出了，没看到怎么指定machine id的呀，下面才是创建机器，有点奇怪
  LOG(ERROR) << "Preparing to start CalvinFS node "
             << FLAGS_machine_id << "...";

  while (GetTime() < FLAGS_time) {
    usleep(100);
  }

  // Create machine.
  ClusterConfig cc;
  cc.FromFile(FLAGS_config);

  int replicas = (cc.size() >= 3) ? 3 : 1;//gaoxuan --在这指定副本数量，这里只有两台机器，所以副本数是1
  int partitions = cc.size() / replicas;//gaoxuan --这个划分也就是划分几份的意思吧，这里就是2/1=2

  LOG(ERROR) << "Starting CalvinFS node " << FLAGS_machine_id
             << " (partition " << (FLAGS_machine_id % partitions)
             << "/" << partitions
             << ", replica " << (FLAGS_machine_id / partitions)
             << "/" << replicas << ")";

  Machine m(FLAGS_machine_id, cc);//gaoxuan --对呀，这里才是创建，怎么FLAGS_machine_id那台机器启动他咋就知道呢
  Spin(1);

  string fsconfig;
  MakeCalvinFSConfig(partitions, replicas).SerializeToString(&fsconfig);
  m.AppData()->Put("calvinfs-config", fsconfig);
  Spin(1);

  // Start paxos app (maybe).
  if (FLAGS_machine_id % partitions == 0) {//gaoxuan --这个地方是两台机器第一处不同，paxos Metalog只会在偶数号机器上创建
    StartAppProto sap;
    for (int i = 0; i < replicas; i++) {
      sap.add_participants(i * partitions);
    }

    sap.set_app("Paxos2App2");
    sap.set_app_name("paxos2");
    string args;
    sap.SerializeToString(&args);
    sap.set_app_args(args);
    m.AddApp(sap);
    LOG(ERROR) << "[" << FLAGS_machine_id << "] created Metalog (Paxos2)";
  }

  m.GlobalBarrier();
  Spin(1);

  //gaoxuan --这里元数据存储app在哪，创建怎么创建的，他这里app名字就是metadata，我前面写的传进来的app的name就是client，所以只可能是这两种app？
  // Start metadata store app.
  //gaoxuan --这里是add_app,也就是说App这个东西是手动创建的
  //gaoxuan --reinterpret_cast <new_type> (expression)是将expression转换成new_type类型的东西
  m.AddApp("MetadataStoreApp", "metadata");
  reinterpret_cast<MetadataStore*>(
      reinterpret_cast<StoreApp*>(m.GetApp("metadata"))->store())
          ->SetMachine(&m);
  LOG(ERROR) << "[" << FLAGS_machine_id << "] created MetadataStore";
  m.GlobalBarrier();
  Spin(1);

  // Start scheduler app.
  m.AddApp("LockingScheduler", "scheduler");
  Scheduler* scheduler_ = reinterpret_cast<Scheduler*>(m.GetApp("scheduler"));

  LOG(ERROR) << "[" << FLAGS_machine_id << "] created LockingScheduler";
  m.GlobalBarrier();
  Spin(1);

  // Bind scheduler to store.
  scheduler_->SetParameters(FLAGS_max_active, FLAGS_max_running);
  scheduler_->SetStore("metadata");

  LOG(ERROR) << "[" << FLAGS_machine_id << "] bound Scheduler to MetadataStore";
  m.GlobalBarrier();
  Spin(1);

  // Start block store app.
  m.AddApp("DistributedBlockStore", "blockstore");

  LOG(ERROR) << "[" << FLAGS_machine_id << "] created BlockStore";
  m.GlobalBarrier();
  Spin(1);

  // Start log app.
  m.AddApp("BlockLogApp", "blocklog");

  LOG(ERROR) << "[" << FLAGS_machine_id << "] created BlockLog";
  m.GlobalBarrier();
  Spin(1);

  // Connect log to scheduler.
  scheduler_->SetActionSource(
      reinterpret_cast<BlockLogApp*>(m.GetApp("blocklog"))->GetActionSource());

  LOG(ERROR) << "[" << FLAGS_machine_id << "] connected BlockLog to Scheduler";
  m.GlobalBarrier();
  Spin(1);

  // Start client app.
  m.AddApp("CalvinFSClientApp", "client");

  LOG(ERROR) << "[" << FLAGS_machine_id << "] created CalvinFSClientApp";
  reinterpret_cast<CalvinFSClientApp*>(m.GetApp("client"))
      ->set_start_time(FLAGS_time);
  reinterpret_cast<CalvinFSClientApp*>(m.GetApp("client"))
      ->set_experiment(FLAGS_experiment, FLAGS_clients); //gaoxuan --应该是在这个地方转到调用RenameFileExperiment()的

  while (!m.Stopped()) {
    usleep(10000);
  }

  printf("Machine %d : Calvin server exit!\n", (int)FLAGS_machine_id);
  usleep(1000*1000);
}

