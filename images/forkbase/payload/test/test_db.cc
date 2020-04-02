// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include <sstream>
#include <utility>

#include "gtest/gtest.h"

#include "hash/hash.h"
#include "db.h"

using namespace ustore_kvdb;

TEST(GoDB, DASL_Only) {
// NOTE: clean ustore storage each time before testing
// by invoking ./bin/ustore_clean.sh

/* Construct DASL as Figure 8 in paper:  
<TMK> represents the prefix of vid for that updated entry

1:<TMK>  <- 3: <OY6>  <- 5: <FY4>  <- 10: <KJI>  <- 12: <3UK> <- 16: <26P>
1:       <- 3         <- 5         <- 10         <- 12        <- 16
1:                    <- 5         <- 10         <- 12        <- 16
1:                                 <- 10                      <- 16
1:                                                            <- 16 

Linear: 
1: <TMK>  <- 3 <ASV>  <- 5 <TTJ>  <- 10 <A6W>  <- 12 <RBG> <- 16 <J25>
*/
  KVDB db;
  Status init_status = db.InitGlobalState();
  ASSERT_TRUE(init_status.ok());

  std::string key = "key";
  std::vector<std::string> empty;
  ASSERT_TRUE(db.PutState(key, "v1", "txn1", 1, empty));
  ASSERT_TRUE(db.Commit().first.ok());

  DLOG(INFO) << "-------------------------------------------------------------------------";

  ASSERT_TRUE(db.PutState(key, "v3", "txn3", 3, empty));
  ASSERT_TRUE(db.Commit().first.ok());

  DLOG(INFO) << "-------------------------------------------------------------------------";
  ASSERT_TRUE(db.PutState(key, "v5", "txn5", 5, empty));
  ASSERT_TRUE(db.Commit().first.ok());

  DLOG(INFO) << "-------------------------------------------------------------------------";
  ASSERT_TRUE(db.PutState(key, "v10", "txn10", 10, empty));
  ASSERT_TRUE(db.Commit().first.ok());

  DLOG(INFO) << "-------------------------------------------------------------------------";
  ASSERT_TRUE(db.PutState(key, "v12", "txn12", 12, empty));
  ASSERT_TRUE(db.Commit().first.ok());

  DLOG(INFO) << "-------------------------------------------------------------------------";
  ASSERT_TRUE(db.PutState(key, "v16", "txn16", 16, empty));
  ASSERT_TRUE(db.Commit().first.ok());


  ASSERT_EQ(16, int(db.GetLatestVersion(key)));

  DLOG(INFO) << "-------------------------------------------------------------------------";
  HistReturn hr = db.Hist(key);
  ASSERT_TRUE(hr.status().ok());
  ASSERT_EQ(16, int(hr.blk_idx()));
  ASSERT_EQ("v16", hr.value());


  DLOG(INFO) << "-------------------------------------------------------------------------";
  hr = db.Hist(key, 11);
  ASSERT_TRUE(hr.status().ok());
  ASSERT_EQ(10, int(hr.blk_idx()));
  ASSERT_EQ("v10", hr.value());

  DLOG(INFO) << "-------------------------------------------------------------------------";
  hr = db.Hist(key, 5);
  ASSERT_TRUE(hr.status().ok());
  ASSERT_EQ(5, int(hr.blk_idx()));
  ASSERT_EQ("v5", hr.value());

  DLOG(INFO) << "-------------------------------------------------------------------------";
  hr = db.Hist(key, 0);  // test for too small blk_idx
  ASSERT_FALSE(hr.status().ok());

  DLOG(INFO) << "-------------------------------------------------------------------------";
  hr = db.Hist("Non-exist-key", 0);  // test for too small blk_idx
  ASSERT_FALSE(hr.status().ok());
}

TEST(GoDB, DAG_Only) {
  /* Construct DAG as Figure 6 in paper: 
  */
  KVDB db;
  Status init_status = db.InitGlobalState();
  ASSERT_TRUE(init_status.ok());

  ASSERT_TRUE(db.PutState("k1", "val1", "txn1", 1, {}));
  ASSERT_TRUE(db.Commit().first.ok());

  ASSERT_TRUE(db.PutState("k0", "val2", "txn2", 2, {"k1"}));
  ASSERT_TRUE(db.PutState("k2", "val3", "txn3", 2, {"k1"}));
  ASSERT_TRUE(db.Commit().first.ok());

  ASSERT_TRUE(db.PutState("k1", "val4", "txn4", 3, {"k1"}));
  ASSERT_TRUE(db.Commit().first.ok());

  ASSERT_TRUE(db.PutState("k0", "val5", "txn5", 4, {"k0", "k1"}));
  ASSERT_TRUE(db.PutState("k2", "val6", "txn6", 4, {"k1", "k2"}));
  ASSERT_TRUE(db.Commit().first.ok());

  HistReturn hr = db.Hist("k1", 4294967295);
  ASSERT_TRUE(hr.status().ok());
  ASSERT_EQ("val4", hr.value());


  BackwardReturn br = db.Backward("k0", 4);
  ASSERT_TRUE(br.status().ok());
  ASSERT_EQ("txn5", br.txnID());

  ASSERT_EQ(size_t(2), br.dep_keys().size());
  ASSERT_EQ(size_t(2), br.dep_blk_idx().size());

  ASSERT_EQ("k0", br.dep_keys()[0]);
  ASSERT_EQ("k1", br.dep_keys()[1]);

  ASSERT_EQ(2, int(br.dep_blk_idx()[0]));
  ASSERT_EQ(3, int(br.dep_blk_idx()[1]));


  ForwardReturn fr = db.Forward("k1", 1);
  ASSERT_TRUE(fr.status().ok());
  ASSERT_EQ(size_t(3), fr.txnIDs().size()); 
  ASSERT_EQ("txn2", fr.txnIDs()[0]);
  ASSERT_EQ("txn3", fr.txnIDs()[1]);
  ASSERT_EQ("txn4", fr.txnIDs()[2]);

  ASSERT_EQ(size_t(3), fr.forward_keys().size()); 
  ASSERT_EQ("k0", fr.forward_keys()[0]);
  ASSERT_EQ("k2", fr.forward_keys()[1]);
  ASSERT_EQ("k1", fr.forward_keys()[2]);

  ASSERT_EQ(size_t(3), fr.forward_blk_idx().size()); 
  ASSERT_EQ(size_t(2), fr.forward_blk_idx()[0]);
  ASSERT_EQ(size_t(2), fr.forward_blk_idx()[1]);
  ASSERT_EQ(size_t(3), fr.forward_blk_idx()[2]);


  fr = db.Forward("k1", 3);
  ASSERT_TRUE(fr.status().ok());
  ASSERT_EQ(size_t(2), fr.txnIDs().size()); 
  ASSERT_EQ("txn5", fr.txnIDs()[0]);
  ASSERT_EQ("txn6", fr.txnIDs()[1]);

  ASSERT_EQ(size_t(2), fr.forward_keys().size()); 
  ASSERT_EQ("k0", fr.forward_keys()[0]);
  ASSERT_EQ("k2", fr.forward_keys()[1]);

  ASSERT_EQ(size_t(2), fr.forward_blk_idx().size()); 
  ASSERT_EQ(4, int(fr.forward_blk_idx()[0]));
  ASSERT_EQ(4, int(fr.forward_blk_idx()[1]));


  fr = db.Forward("Non-existent-key", 3);
  ASSERT_FALSE(fr.status().ok());

  fr = db.Forward("k1", 5); // larger blk_idx
  ASSERT_TRUE(fr.status().ok());
  ASSERT_EQ(size_t(2), fr.txnIDs().size()); // WRONG HERE!!
  ASSERT_EQ("txn5", fr.txnIDs()[0]);
  ASSERT_EQ("txn6", fr.txnIDs()[1]);


  fr = db.Forward("k1", 0); // too small blk_idx
  ASSERT_FALSE(fr.status().ok()); // Wrong here!!

  // Test for generic storage api
  std::string gk = "kkk";
  ASSERT_TRUE(db.Put(gk, "v").ok());
  std::string val;
  ASSERT_TRUE(db.Get(gk, &val).ok());
  ASSERT_EQ("v", val);

  ASSERT_TRUE(db.Put(gk, "vv").ok());
  ASSERT_TRUE(db.Get(gk, &val).ok());
  ASSERT_EQ("vv", val);
}


TEST(GoDB, Snapshot) {
// NOTE: clean ustore storage each time before testing
// by invoking ./bin/ustore_clean.sh
  KVDB db;
  Status init_status = db.InitGlobalState();
  ASSERT_TRUE(init_status.ok());

  ASSERT_TRUE(db.PutState("kk0", "val", "txn1", 1, {}, ""));
  ASSERT_TRUE(db.PutState("kk1", "val", "txn1", 1, {}, ""));
  ASSERT_TRUE(db.PutState("kk2", "val", "txn1", 1, {}, ""));
  ASSERT_TRUE(db.PutState("kk3", "val", "txn1", 1, {}, ""));
  auto status1 = db.Commit();
  ASSERT_TRUE(status1.first.ok());
  std::string snapshot1 = status1.second;

  ASSERT_TRUE(db.PutState("kk0", "val", "txn2", 2, {"kk0", "kk1", "kk2"}, snapshot1));
  auto status2 = db.Commit();
  ASSERT_TRUE(status2.first.ok());
  std::string snapshot2 = status2.second;

  ASSERT_TRUE(db.PutState("kk1", "val", "txn3", 3, {"kk0", "kk1"}, snapshot1));
  ASSERT_TRUE(db.PutState("kk2", "val", "txn2", 3, {"kk0", "kk2"}, snapshot2));
  auto status3 = db.Commit();
  ASSERT_TRUE(status3.first.ok());
  std::string snapshot3 = status3.second;

  ASSERT_TRUE(db.PutState("kk2", "val", "txn4", 4, {"kk0", "kk2", "kk3"}, snapshot2));
  auto status4 = db.Commit();
  ASSERT_TRUE(status4.first.ok());
  std::string snapshot4 = status3.second;


  BackwardReturn br = db.Backward("kk1", 3);
  ASSERT_TRUE(br.status().ok());

  ASSERT_EQ(size_t(2), br.dep_keys().size());
  ASSERT_EQ(size_t(2), br.dep_blk_idx().size());

  ASSERT_EQ("kk0", br.dep_keys()[0]);
  ASSERT_EQ("kk1", br.dep_keys()[1]);

  ASSERT_EQ(1, int(br.dep_blk_idx()[0]));
  ASSERT_EQ(1, int(br.dep_blk_idx()[1]));

  br = db.Backward("kk2", 3);
  ASSERT_TRUE(br.status().ok());

  ASSERT_EQ(size_t(2), br.dep_keys().size());
  ASSERT_EQ(size_t(2), br.dep_blk_idx().size());

  ASSERT_EQ("kk0", br.dep_keys()[0]);
  ASSERT_EQ("kk2", br.dep_keys()[1]);

  ASSERT_EQ(2, int(br.dep_blk_idx()[0]));
  ASSERT_EQ(1, int(br.dep_blk_idx()[1]));

  br = db.Backward("kk2", 5);
  ASSERT_TRUE(br.status().ok());

  ASSERT_EQ(size_t(3), br.dep_keys().size());
  ASSERT_EQ(size_t(3), br.dep_blk_idx().size());

  ASSERT_EQ("kk0", br.dep_keys()[0]);
  ASSERT_EQ("kk2", br.dep_keys()[1]);
  ASSERT_EQ("kk3", br.dep_keys()[2]);

  ASSERT_EQ(2, int(br.dep_blk_idx()[0]));
  ASSERT_EQ(1, int(br.dep_blk_idx()[1]));
  ASSERT_EQ(1, int(br.dep_blk_idx()[1]));


  ForwardReturn fr = db.Forward("kk0", 3);
  ASSERT_TRUE(fr.status().ok());

  ASSERT_EQ(size_t(2), fr.forward_keys().size()); 
  ASSERT_EQ("kk2", fr.forward_keys()[0]);
  ASSERT_EQ("kk2", fr.forward_keys()[1]);
  
  ASSERT_EQ(size_t(2), fr.forward_blk_idx().size()); 
  ASSERT_EQ(size_t(3), fr.forward_blk_idx()[0]);
  ASSERT_EQ(size_t(4), fr.forward_blk_idx()[1]);

  fr = db.Forward("kk0", 1);
  ASSERT_TRUE(fr.status().ok());
  
  // By right, it should return two states. 
  // The one missing state is kk1 at blk 3. 
  // It is because when kk1 at blk3 is committed, kk0 at blk1 is already staled. 
  // Hece it can not be captured for the forward query. 
  // Refer to the comment in db.cc for details. 

  ASSERT_EQ(size_t(1), fr.forward_keys().size()); 
  ASSERT_EQ("kk0", fr.forward_keys()[0]);
  
  ASSERT_EQ(size_t(1), fr.forward_blk_idx().size()); 
  ASSERT_EQ(size_t(2), fr.forward_blk_idx()[0]);

  fr = db.Forward("kk2", 1);
  ASSERT_TRUE(fr.status().ok());
  // For the same reason as above, this forward query result misses kk2 at blk 4. 

  ASSERT_EQ(size_t(2), fr.forward_keys().size()); 
  ASSERT_EQ(size_t(2), fr.forward_blk_idx().size()); 

  ASSERT_EQ("kk0", fr.forward_keys()[0]);
  ASSERT_EQ("kk2", fr.forward_keys()[1]);
  
  ASSERT_EQ(size_t(2), fr.forward_blk_idx()[0]);
  ASSERT_EQ(size_t(3), fr.forward_blk_idx()[1]);
}