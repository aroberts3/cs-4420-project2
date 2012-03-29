import simpledb.server.*;
import simpledb.index.*;
import simpledb.index.hash.*;
import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import java.util.Scanner;

public class HashDriver{

	private static Schema idxsch = new Schema();
	private static Transaction tx;
   
	public static void main(String[] args) {
	  SimpleDB.init("studentdb");    
	  System.out.println("BUILD LINEAR HASH INDEX");
	  tx = new Transaction();
	  // Defines the schema for an index record for Col2 of the messy table 
	  idxsch.addStringField("dataval",64);
	  idxsch.addIntField("block");
	  idxsch.addIntField("id"); 
	  // Builds a Linear Hash Index on Col2 of the messy table 
	  Index idx = new LinearHash("hashIdxTest", idxsch, tx);
	  Plan p = new TablePlan("messy", tx);
	  UpdateScan s = (UpdateScan) p.open();
	  while (s.next())
	      idx.insert(s.getVal("col2"), s.getRid());
	  s.close(); 
	  idx.close();
	  tx.rollback();
	  Scanner scan = new Scanner(System.in);
	  /*
	  while(true){
		  Constant searchKey = (Constant)scan.next();
		  System.out.println(idx.search(searchKey));	  
	  }*/
	}
}
