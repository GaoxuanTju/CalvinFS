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

DEFINE_bool(calvin_version, false, "Print Calvin version information");
DEFINE_string(binary, "calvinfs_server", "Calvin binary executable program");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_int32(machine_id, 0, "machine id");
DEFINE_double(time, 0, "start time");
DEFINE_int32(experiment, 0, "experiment that you want to run");
DEFINE_int32(clients, 20, "number of concurrent clients on each machine");
DEFINE_int32(max_active, 1000, "max active actions for locking scheduler");
DEFINE_int32(max_running, 100, "max running actions for locking scheduler");

int main(int argc, char **argv)
{
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  // Print Calvin version
  if (FLAGS_calvin_version)
  {
    // Check whether Calvin have been running
    if (is_process_exist((char *)FLAGS_binary.c_str()) == true)
    {
      return -2;
    }
    else
    {
      printf("Machine %d: (CalvinFS) 0.1 (c) Yale University 2016.\n",
             (int)FLAGS_machine_id);
      return 0;
    }
  }

  LOG(ERROR) << "Preparing to start CalvinFS node "
             << FLAGS_machine_id << "...";

  while (GetTime() < FLAGS_time)
  {
    usleep(100);
  }

  // Create machine.
  ClusterConfig cc;
  cc.FromFile(FLAGS_config); // gaoxuan --这个地方是获得集群中机器的ip地址，从calvin.conf

  int replicas = (cc.size() >= 3) ? 3 : 1; // gaoxuan --在这里cc.size根据上面的ip地址就能确定是2了,replicas是1
  int partitions = cc.size() / replicas;   // gaoxuan --partions是2，就代表划分到两台机器上了
  //int partitions =4;
  LOG(ERROR) << "Starting CalvinFS node " << FLAGS_machine_id
             << " (partition " << (FLAGS_machine_id % partitions)
             << "/" << partitions
             << ", replica " << (FLAGS_machine_id / partitions)
             << "/" << replicas << ")";

  Machine m(FLAGS_machine_id, cc); // gaoxuan --创建实际的机器对象
  Spin(1);

  string fsconfig;
  // gaoxuan --MakeCalvinFSConfig有三个函数，想改就得改这里面的函数，他可以影响到hash范围，通过设置mds_count
  // gaoxuan --一个参数的MakeCalvinFSConfig函数现在代表n台机器，每台机器上一个副本，一个mds，需要改成，只有一台机器上有mds和副本即可

  // MakeCalvinFSConfig().SerializeToString(&fsconfig);//gaoxuan --这个里面就是具体设置，这一台机器的mds，块存储
  MakeCalvinFSConfig(partitions, replicas).SerializeToString(&fsconfig);
  m.AppData()->Put("calvinfs-config", fsconfig); // gaoxuan --通过machine()->getAppData("calvinfs-config")可以得到刚刚设置的东西
  Spin(1);

  // Start paxos app (maybe).
  if (FLAGS_machine_id % partitions == 0)
  { // gaoxuan --如果上面改动的话，这里也要改一下逻辑
    StartAppProto sap;
    for (int i = 0; i < replicas; i++)
    {
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

  // Start metadata store app.

  m.AddApp("MetadataStoreApp", "metadata");

  reinterpret_cast<MetadataStore *>(
      reinterpret_cast<StoreApp *>(m.GetApp("metadata"))->store())
      ->SetMachine(&m); //
  LOG(ERROR) << "[" << FLAGS_machine_id << "] created MetadataStore";
  m.GlobalBarrier();
  Spin(1);

  // Start scheduler app.
  m.AddApp("LockingScheduler", "scheduler");
  Scheduler *scheduler_ = reinterpret_cast<Scheduler *>(m.GetApp("scheduler"));

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
      reinterpret_cast<BlockLogApp *>(m.GetApp("blocklog"))->GetActionSource());

  LOG(ERROR) << "[" << FLAGS_machine_id << "] connected BlockLog to Scheduler";
  m.GlobalBarrier();
  Spin(1);

  // Start client app.

  m.AddApp("CalvinFSClientApp", "client");
  LOG(ERROR) << "[" << FLAGS_machine_id << "] created CalvinFSClientApp";
  reinterpret_cast<CalvinFSClientApp *>(m.GetApp("client"))
      ->set_experiment(FLAGS_experiment, FLAGS_clients);
  // m.AddApp("CalvinFSClientApp", "client2");
  // LOG(ERROR) << "[" << FLAGS_machine_id << "] created CalvinFSClientApp";
  // reinterpret_cast<CalvinFSClientApp *>(m.GetApp("client2"))
  //     ->set_experiment(FLAGS_experiment, FLAGS_clients);
  // m.AddApp("CalvinFSClientApp", "client3");
  // LOG(ERROR) << "[" << FLAGS_machine_id << "] created CalvinFSClientApp";
  // reinterpret_cast<CalvinFSClientApp *>(m.GetApp("client3"))
  //     ->set_experiment(FLAGS_experiment, FLAGS_clients);

  // reinterpret_cast<CalvinFSClientApp*>(m.GetApp("client1"))
  //     ->set_start_time(FLAGS_time);
  // reinterpret_cast<CalvinFSClientApp*>(m.GetApp("client1"))
  //     ->set_experiment(FLAGS_experiment, FLAGS_clients);

  while (!m.Stopped())
  {
    usleep(10000);
  }

  printf("Machine %d : Calvin server exit!\n", (int)FLAGS_machine_id);
  usleep(1000 * 1000);
}
