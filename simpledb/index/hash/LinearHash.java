package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;
import java.util.ArrayList;

public class LinearHash implements Index{
  public int num_buckets = 1;
  private int expandConstant;
  private int splitPointer = 0; // next bucket to be split
  private int level = 0;
  private ArrayList<TableScan> buckets = new ArrayList<TableScan>();
  private ArrayList buckets_blocks = new ArrayList();
  private int currentBucketNum;
  private String idxname;
  private Schema sch;
  private Transaction tx;
  private Constant searchkey = null;
  private TableScan ts = null;

  public LinearHash(String idxname, Schema sch, Transaction tx){
    this.idxname = idxname;
    this.sch = sch;
    this.tx = tx;
  }

  public void beforeFirst(Constant searchkey){
    close();
    this.searchkey = searchkey;
    int bucket = address(level, searchkey);
    if (bucket < splitPointer){
      bucket = address(level+1, searchkey);
    }
    String tblname = idxname + bucket;
    TableInfo ti = new TableInfo(tblname, sch);
    currentBucketNum = bucket;
    ts = new TableScan(ti, tx);
    if(bucket >= buckets.size()){
      buckets.add(bucket, ts);
      buckets_blocks.add(bucket, ts.size());
    }
    else{
      buckets.set(bucket, ts);
      buckets_blocks.set(bucket, ts.size());
    }
  }

  public boolean next(){
    while(ts.next()){
      if(ts.getVal("dataval").equals(searchkey)){
        return true;
      }
    }
    return false;
  }

  public RID getDataRid(){
    int blknum = ts.getInt("block");
    int id = ts.getInt("id");
    return new RID(blknum, id);
  }

  public void insert(Constant val, RID rid){
    beforeFirst(val);
    ts.insert();
    ts.setInt("block", rid.blockNumber());
    ts.setInt("id", rid.id());
    ts.setVal("dataval", val);
    // update buckets and buckets_blocks number
    buckets.set(currentBucketNum, ts);
    buckets_blocks.set(currentBucketNum, ts.size());
  }

  // Closes the index by closing the current table scan
  public void close(){
    if (ts != null){
      ts.close();
    }
  }

  public int address(int lvl, Constant key){
    int bucket = key.hashCode() % (2^lvl);
    return bucket;
  }

  public void delete(Constant key, RID rid){
  }

  private void incrementSplitPointer(){
    splitPointer++;
    if (splitPointer == Math.pow(2,level)){
      level++;
      splitPointer = 0;
    }
  }
}

