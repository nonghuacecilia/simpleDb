package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;





/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator
{
	

	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;
	TupleDesc td;
	List <Tuple> list;
	Map<Field, Tuple> map;//groupby
	Map <Field ,ArrayList<Integer>> avg_map;//groupby+avg
	ArrayList <Integer> avg_list;//no group
	
	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */

	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what)
	{
		// some code goes here
		this.gbfield=gbfield;
		this.gbfieldtype=gbfieldtype;
		this.afield=afield;
		this.what=what;
		this.list=new ArrayList<Tuple>();
		this.map = new HashMap<Field, Tuple> ();
		this.avg_list= new ArrayList();
		this.avg_map=new HashMap<Field ,ArrayList<Integer>>();
	/*
	 * create the tupledesc of the new tuple
	 * 	
	 */
		if(gbfield==Aggregator.NO_GROUPING){
			Type [] type={Type.INT_TYPE};
			this.td=new TupleDesc(type);
		}else{
			Type [] type={gbfieldtype,Type.INT_TYPE};
			this.td=new TupleDesc(type);
		}
		
		
	}

	private Tuple createNewTuple(Tuple tup){
		//renew a tuple
		Tuple tnew=new Tuple(td);//td is the new tuple
		if(gbfield==Aggregator.NO_GROUPING)//no grouping 
		 tnew.setField(0, tup.getField(afield));
		else{
			tnew.setField(0, tup.getField(gbfield));
			tnew.setField(1, tup.getField(afield));
		}
		return tnew;
		
	}
	private void mergeMin(Tuple tup) {
		// TODO Auto-generated method stub
		Tuple tupnew=createNewTuple(tup);
		Field fgroup=tupnew.getField(0);// the first field is the group,if there exist the group,get the group and compare with
		// the already exit min .
		if(map.containsKey(fgroup)){//there has a groupby
			Tuple old = map.get(fgroup);
			IntField oldfield= getAggField(old);
			IntField currentField= (IntField) tup.getField(afield);
			if(currentField.getValue()<oldfield.getValue())
				oldfield.setValue(currentField.getValue());
			
			
		}else{
			
			map.put(fgroup, tupnew);
			list.add(tupnew);
		}
		
	}
	public IntField getAggField(Tuple t){//get the aggField
		
		if(gbfield==Aggregator.NO_GROUPING){
			return (IntField)t.getField(0);
		}else{
			return (IntField)t.getField(1);
		}
		
		
		
	}
	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	
	public void merge(Tuple tup)
	{

		// some code goes here
		if(Op.MIN==this.what){
			mergeMin(tup);
		}
		if(Op.MAX==this.what){
			mergeMax(tup);
		}
		if(Op.COUNT==this.what){
			mergeCount(tup);
		}
		if(Op.SUM==this.what){
			mergeSum(tup);
		}
		if(Op.AVG==this.what){
			mergeAvg(tup);
		}
		
		
	}

	private void mergeAvg(Tuple tup) {
		// TODO Auto-generated method stub
		Tuple tupnew=createNewTuple(tup);
		if(gbfield!=Aggregator.NO_GROUPING){//has group
			Field gbfield=tupnew.getField(0);
			if(map.containsKey(gbfield)){
				Tuple old = map.get(gbfield);
				IntField oldfield= getAggField(old);
				IntField currentField= (IntField) tup.getField(afield);
				
				ArrayList<Integer> array=avg_map.get(gbfield);//array store all agg number
				array.add(currentField.getValue());//add the agg num to the array
				oldfield.setValue(calAvg(array));
			}else{
				
				map.put(gbfield, tupnew);
				list.add(tupnew);
				avg_map.put(gbfield, new ArrayList<Integer>());
				
				ArrayList<Integer> afieldList = avg_map.get(gbfield);
				afieldList.add(getAggField(tupnew).getValue());
			}
			
		}else{
		// no group
			
			if(list.isEmpty()){
				
				list.add(tupnew);
				avg_list.add(((IntField)tupnew.getField(0)).getValue());
				
			}else{//is not empty
				
				Tuple old = list.get(list.size()-1);
				
				IntField currentField= (IntField) tup.getField(afield);
				avg_list.add(currentField.getValue());
				IntField oldfield= getAggField(old);//already stored avgerage
				oldfield.setValue(calAvg(avg_list));
				
			}
			
		}
		
		
		
	}

	private int calAvg(ArrayList<Integer> array) {
		// TODO Auto-generated method stub
		int count = array.size();
		
		int total=0;
		for(int i=0;i<array.size();i++){
			total=total+array.get(i);
			
			
		}
		return total/count;
	}

	private void mergeSum(Tuple tup) {
		// TODO Auto-generated method stub
		Tuple tupnew=createNewTuple(tup);
		Field fgroup=tupnew.getField(0);// the first field is the group,if there exist the group,get the group and compare with
		// the already exit min .
		if(map.containsKey(fgroup)){//there has a groupby
			Tuple old = map.get(fgroup);
			IntField oldcount= getAggField(old);
			oldcount.setValue(oldcount.getValue()+((IntField)tupnew.getField(1)).getValue());
			
		}else{
			
			map.put(fgroup, tupnew);
			list.add(tupnew);
		}
	}

	private void mergeCount(Tuple tup) {
		// TODO Auto-generated method stub
		
		Tuple tupnew=createNewTuple(tup);
		Field fgroup=tupnew.getField(0);// the first field is the group,if there exist the group,get the group and compare with
		// the already exit min .
		if(map.containsKey(fgroup)){//there has a groupby
			
			Tuple tt=map.get(fgroup);// original count
			IntField oldcount= getAggField(tt);
			oldcount.setValue(oldcount.getValue()+1);
			
		}else{
			
			map.put(fgroup, tupnew);
			reset(tupnew);
			list.add(tupnew);
		}
		
	}

	

	private void reset(Tuple tupnew) {
		// TODO Auto-generated method stub
		if(gbfield==Aggregator.NO_GROUPING){
			//no grouping 
			IntField ifield=(IntField) tupnew.getField(0);
			ifield.setValue(1);
		}
		else{
			IntField ifield=(IntField) tupnew.getField(1);
			ifield.setValue(1);
		}
	}

	private void mergeMax(Tuple tup) {
		// TODO Auto-generated method stub
		Tuple tupnew=createNewTuple(tup);
		Field fgroup=tupnew.getField(0);// the first field is the group,if there exist the group,get the group and compare with
		// the already exit min .
		if(map.containsKey(fgroup)){//there has a groupby
			Tuple old = map.get(fgroup);
			IntField oldfield= getAggField(old);
			IntField currentField= (IntField) tup.getField(afield);
			if(currentField.getValue()>oldfield.getValue())
				oldfield.setValue(currentField.getValue());
			
			
		}else{
			
			map.put(fgroup, tupnew);
			list.add(tupnew);
		}
	}

	public static class IntAggregatorIterator implements DbIterator{
		
		List <Tuple> list;
		Iterator <Tuple> it;
		public IntAggregatorIterator(List <Tuple> list){
			this.list=list;
			
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			it=list.iterator();
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			// TODO Auto-generated method stub
			return it.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			// TODO Auto-generated method stub
			return it.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			close();
			open();
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			Tuple t= list.get(0);
			return t.getTupleDesc();
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			it=null;
		}
		
		
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public DbIterator iterator()
	{
		// some code goes here

		return new IntAggregatorIterator(list);
		//throw new UnsupportedOperationException("implement me");
	}

	
}
