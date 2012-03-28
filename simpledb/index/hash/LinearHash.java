package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;
import java.util.ArrayList;

public class LinearHash implements Index{
  public int numBuckets = 1;
  private int expandConstant = 1;
  private int splitPointer = 0; // next bucket to be split
  private int level = 0;
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
    ts = new TableScan(ti, tx);
  }
  
  //sets the current tablescan to the bucket that will be split
  public void setSplitBucket(){
	  String tblname = idxname + splitPointer;
	  TableInfo ti = new TableInfo(tblname,sch);
	  ts = new TableScan(ti,tx);  
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
	//preserve current TableScan
	TableScan prev = ts;
    beforeFirst(val);
    int initialBlocks = ts.size();
    ts.insert();
    ts.setInt("block", rid.blockNumber());
    ts.setInt("id", rid.id());
    ts.setVal("dataval", val);
    int finalBlocks = ts.size();
    ts = prev;
    if(finalBlocks>initialBlocks){
      expand();
    }
  }

  // Expands the bucket pointed to by the splitPointer
  public void expand(){
    printIndex();

    numBuckets++;
    setSplitBucket();

    while(ts.next()){
      // rehash all of the records in ts
      int block = ts.getInt("block");
      int id = ts.getInt("id");
      RID rid = new RID(block, id);
      Constant dataval = ts.getVal("dataval");
      ts.delete();
      insert(dataval, rid);
    }
    printIndex();
    incrementSplitPointer();
  }

  // Closes the index by closing the current table scan
  public void close(){
    if (ts != null){
      ts.close();
    }
  }

  public int address(int lvl, Constant key){
    int bucket = key.hashCode() % ((2^lvl)*numBuckets);
    return bucket;
  }
 
  

  public void delete(Constant key, RID rid){
	  return;
  }

  public void printIndex(){
    close();
    System.out.printf("Level: %d \t Next: %d", level, splitPointer);
    int len = buckets.size();
    int i;
    int block;
    boolean overflow = false;
    for(i=0; i<len; i++){
      ts = buckets.get(i);
      ts.beforeFirst();
      System.out.printf("Bucket #%d\n", i);
      while(ts.next()){
        block = ts.currentBlock();
        if(block==0){
          // Not in the overflow blocks
          System.out.printf("%d \t", ts.getInt("id"));
        }
        else{
          if(!overflow){
            System.out.println();
            System.out.printf("Bucket #%d overflow\n", i);
            overflow = true;
          }
          System.out.printf("%d \t", ts.getInt("id"));
        }
      }
      System.out.println("\n"); // white space between buckets
    }
  }

  private void incrementSplitPointer(){
    splitPointer++;
    if (splitPointer == Math.pow(2,level)){
      level++;
      splitPointer = 0;
    }
  }
}

