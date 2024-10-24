// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db_master.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using ROCKSDB_NAMESPACE::DB_MASTER;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_master_example";
#else
std::string kDBPath = "/tmp/rocksdb_master_example";
#endif

/*

purpose of this test
:check if the rollback operation doesn't occur 
if duplicate key is in memtable or l0.

*/

int main() {
  DB_MASTER db_master = DB_MASTER();
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = db_master.DB_MASTER::Open(options, kDBPath, 2);
  assert(s.ok());

  // Put key-value
  //s = db_master.Put(WriteOptions(), "key1", "value1");
  //assert(s.ok());

  std::string value;

  std::cout << "value is : " << value << std::endl;
  
  for(int i = 2; i < 1000000; i++){
    s = db_master.Put(WriteOptions(), "key" + std::to_string(i), "value                                                                                                                                                    " + std::to_string(i));
    assert(s.ok());
  }
  s = db_master.Put(WriteOptions(), "key1", "value1");
  db_master.PutThroughCE(WriteOptions(), "key1", "value" + std::to_string(1000));
  printf("put to ce\n");

  for(int i = 2; i < 1000000; i++){
    s = db_master.Put(WriteOptions(), "key" + std::to_string(i), "value                                                                                                                                                    " + std::to_string(i));
    assert(s.ok());
  }

  // get value
  s = db_master.Get(ReadOptions(), "key1", &value);

  std::cout << "value is : " << value << std::endl;
  assert(s.ok());
  assert(value == "value" + std::to_string(1));

  db_master.DestroyDB_Master(options);

  return 0;
}