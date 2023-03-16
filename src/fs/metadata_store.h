// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// All interactions with the MetadataStore MUST occur through Actions.
// See fs/metadata.proto for more details.

#ifndef CALVIN_FS_METADATA_STORE_H_
#define CALVIN_FS_METADATA_STORE_H_

#include <string>
#include "btree/btree_map.h"
#include "common/types.h"
#include "common/mutex.h"
#include "components/store/store.h"
#include "fs/metadata.pb.h"

// gaoxuan --下面的二叉链表用于存储目录树
typedef struct BTNode
{
  string path;            // 路径名字
  struct BTNode *child;   // 该节点的孩子节点
  struct BTNode *sibling; // 该节点的兄弟节点
} BTNode;

class CalvinFSConfigMap;
class Machine;
class VersionedKVStore;
class ExecutionContext;
class MetadataStore : public Store
{
public:
  // Takes ownership of '*store'.
  // Requires: '*store' is entirely empty.
  explicit MetadataStore(VersionedKVStore *store);
  virtual ~MetadataStore();

  // Inherited from Store, defined in fs/metadata_store.cc:
  virtual void GetRWSets(Action *action);
  virtual void Run(Action *action);
  void getLOOKUP(string path);
  void SetMachine(Machine *m);
  void Init(); // gaoxuan --这是原本的函数
  void Init(BTNode *dir_tree);
  void Init(BTNode *dir_tree, string level);
  void Init_for_depth(BTNode *dir_tree);
  void Init_for_30(BTNode *dir_tree);
  void Init_for_10(BTNode *dir_tree);
  void Init_tree_20(BTNode *dir_tree);
  void InitSmall();

  string getAPPname()
  {
    return APP_name;
  }
  void setAPPname(string name)
  {
    APP_name = name;
  }
  string APP_name;

  VersionedKVStore *getStore_()
  {
    return store_;
  }
  Machine *get_machine_()
  {
    return machine_;
  }

private:
  void CreateFile_Internal(
      ExecutionContext *context,
      const MetadataAction::CreateFileInput &in,
      MetadataAction::CreateFileOutput *out);

  void Erase_Internal(
      ExecutionContext *context,
      const MetadataAction::EraseInput &in,
      MetadataAction::EraseOutput *out);

  void Copy_Internal(
      ExecutionContext *context,
      const MetadataAction::CopyInput &in,
      MetadataAction::CopyOutput *out);

  void Rename_Internal(
      ExecutionContext *context,
      const MetadataAction::RenameInput &in,
      MetadataAction::RenameOutput *out);

  void Lookup_Internal(
      ExecutionContext *context,
      const MetadataAction::LookupInput &in,
      MetadataAction::LookupOutput *out);
  void Tree_Lookup_Internal(
      ExecutionContext *context,
      const MetadataAction::Tree_LookupInput &in,
      MetadataAction::Tree_LookupOutput *out);
  void Resize_Internal(
      ExecutionContext *context,
      const MetadataAction::ResizeInput &in,
      MetadataAction::ResizeOutput *out);

  void Write_Internal(
      ExecutionContext *context,
      const MetadataAction::WriteInput &in,
      MetadataAction::WriteOutput *out);

  void Append_Internal(
      ExecutionContext *context,
      const MetadataAction::AppendInput &in,
      MetadataAction::AppendOutput *out);

  void ChangePermissions_Internal(
      ExecutionContext *context,
      const MetadataAction::ChangePermissionsInput &in,
      MetadataAction::ChangePermissionsOutput *out);

  virtual bool IsLocal(const string &path);

  // Map of file paths to serialized MetadataEntries.
  VersionedKVStore *store_;

  // Pointer to local machine (for distributed action execution contexts).
  Machine *machine_;

  // Partitioning/replication configuration. Must be set if machine_ != NULL.
  CalvinFSConfigMap *config_;
};

#endif // CALVIN_FS_METADATA_STORE_H_
