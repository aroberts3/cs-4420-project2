package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;
import simpledb.file.*;

import java.util.ArrayList;


public class LinearHash implements Index{
  public int numBuckets = 1;
  public int startingBuckets = 1;
  private int overflowCount = 0;
  private int maxOverflow = 1;
  private int blockSize = 5; //max records per block
  private int splitPointer = 0; // next bucket to be split
  private boolean redistributing = false; //additional expands will not be performed mid-expand
  private int[] records = new int[100]; //number of records in each bucket including overflow
  private int[] buckets = new int[100]; //keeps track of which locations represent viable buckets
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
    initializeArrays();
  }

  public void beforeFirst(Constant searchkey){
    close();
    this.searchkey = searchkey;
    int bucket = address(searchkey);
    String tblname = idxname + bucket;
    TableInfo ti = new TableInfo(tblname, sch);
    ts = new TableScan(ti, tx);
  }
  
  //sets the current tablescan to the bucket that will be split
  public void setSplitBucket(){
	  ts.close();
	  String tblname = idxname + splitPointer;
	  TableInfo ti = new TableInfo(tblname,sch);
	  ts = new TableScan(ti,tx);
  }
  
  public RID search(Constant searchkey){
	  beforeFirst(searchkey);
	  while(ts.next()){
	      if(ts.getVal("dataval").equals(searchkey)){
	        return new RID(ts.getInt("block"), ts.getInt("id"));
	      }
	  }
	  return null;
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
	close();
	int bucket = address(val);
	int numRecords = records[bucket];
    beforeFirst(val); 
    ts.insert();
    ts.setInt("block", rid.blockNumber());
    ts.setInt("id", rid.id());
    ts.setVal("dataval", val);
    records[bucket]=numRecords+1;
    if(((numRecords+1)>(blockSize))&&(!redistributing)){
    	overflowCount++;
    	if(overflowCount==maxOverflow){
    		expand();
    	}
    }
  }

  // Expands the bucket pointed to by the splitPointer
  public void expand(){
	close();
	ArrayList<RID> rids = new ArrayList<RID>();
    ArrayList<Constant> vals = new ArrayList<Constant>();
    int nextBucket;
    int block;
    int id;
    int i;
    Constant dataval;
    RID rid;
	overflowCount=0;
	numBuckets++;
	records[splitPointer]= 0;
	System.out.println("\nBefore expand: \n-------------------------");
    printIndex();
    setSplitBucket();
    nextBucket = splitPointer + (int)Math.pow(2,level) * startingBuckets;
    buckets[nextBucket]=1;
    //clear the bucket; contents are saved to be redistributed after
    while(ts.next()){
      block = ts.getInt("block");
      id = ts.getInt("id");
      rid = new RID(block, id);
      dataval = ts.getVal("dataval");
      rids.add(rid);
      vals.add(dataval); 
      ts.delete();
    }   
    incrementSplitPointer();
    //rehash contents of split bucket
    redistributing = true;
    for(i = 0; i<rids.size(); i++){
    	rid= rids.get(i);
    	dataval = vals.get(i);
    	insert(dataval,rid);
    }
    redistributing = false;
    System.out.println("\nAfter expand: \n-------------------------");
    printIndex();
  }

  // Closes the index by closing the current table scan
  public void close(){
    if (ts != null){
      ts.close();
    }
  }

  public int address(Constant key){
    double bucket = (key.hashCode() % (Math.pow(2,level)*startingBuckets));
    if (bucket<splitPointer){
    	bucket = (key.hashCode() % ((int)(Math.pow(2,level+1))*startingBuckets));
    }
    return (int)bucket;
  }
  
  public void initializeArrays(){
	  int i;
	  for(i=0;i<records.length;i++){
		  records[i]=0;
		  buckets[i]=0;
	  }
	  //flag starting buckets as viable
	  for(i=0;i<startingBuckets;i++){
		  buckets[i]=1;
	  }
  }
 
  

  public void delete(Constant key, RID rid){
	  return;
  }

  
  public void printIndex(){
    System.out.printf("\n Level: %d \t Next: %d \n \n", level, splitPointer);
    int i;
    int recs;	//count the records as we print them so we know when we are in the overflow
    String val;
    String tblname;
    TableInfo ti;
    boolean overflow;
    for(i=0; i<buckets.length; i++){
		if(buckets[i]==0){
			//this bucket is not in use and cannot contain any records
			continue;
		}
		ts.close();
		recs = 0;
		overflow = false;
		tblname = idxname + i;
		ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
		ts.beforeFirst();
		System.out.printf("Bucket #%d\n", i);
		while(ts.next()){
		    recs++;
		    if(recs<=blockSize){
			    // Not in the overflow blocks
				val = ts.getVal("dataval").toString();
				System.out.printf(val.substring(val.length()-3) + "\t");
	    	}
			else{
			  if(!overflow){
			    System.out.printf("\nBucket #%d overflow\n", i);
			    overflow = true;
			  }
			  val = ts.getVal("dataval").toString();
			  System.out.printf(val.substring(val.length()-3) + "\t");
			}
		}
		System.out.println("\n"); // white space between buckets
    }
  }

  private void incrementSplitPointer(){
    splitPointer = (int)((splitPointer+1)%(Math.pow(2,level)));
    if (splitPointer == 0){
      level++;
    }
  }
}

