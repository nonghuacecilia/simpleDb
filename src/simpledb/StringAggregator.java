package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator
{

	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;

	TupleDesc td;
	List<Tuple> list;
	Map<Field, Tuple> map;// for groupby
	


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
	 *            aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException
	 *             if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what)
	{
		// some code goes here
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		
		this.list = new LinkedList<Tuple>();
		this.map = new HashMap<Field, Tuple>();
		
		
		if(gbfield==Aggregator.NO_GROUPING){//no grouping
			
			Type[] type = { Type.INT_TYPE };
			this.td = new TupleDesc(type);
			
		}else{
			
			Type[] type = { gbfieldtype, Type.INT_TYPE };
			this.td = new TupleDesc(type);
		}
		
		
	}
		
	private Tuple createTuple(Tuple tup)
	{
		Tuple t = new Tuple(td);
		if (gbfield==Aggregator.NO_GROUPING)
		{
			t.setField(0,  new IntField(1));
		}
		else
		{
			t.setField(0, tup.getField(gbfield));
			t.setField(1, new IntField(1));
		}
		return t;
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
		switch (this.what)
		{
	
		case COUNT:
			mergeCount(tup);
			break;
		default:
			;
			
		}

	}

	private void mergeCount(Tuple tup) {
		// TODO Auto-generated method stub
		Tuple tnew = createTuple(tup);
		Field gbfield = tnew.getField(0);
		if (map.containsKey(gbfield))
		{// 分组已经存在
			Tuple tt = map.get(gbfield);
			addcount(tt);
		}
		else
		{
			map.put(tnew.getField(0), tnew);
			resetTuple(tnew);
			list.add(tnew);

		}
	}

	private void resetTuple(Tuple tupnew) {
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

	private void addcount(Tuple tup) {
		// TODO Auto-generated method stub
		IntField ifield = (IntField) getAggField(tup);
		ifield.setValue(ifield.getValue() + 1);
	}

	private IntField getAggField(Tuple tup) {
		// TODO Auto-generated method stub
		if(gbfield==Aggregator.NO_GROUPING){
			return (IntField)tup.getField(0);
		}else{
			return (IntField)tup.getField(1);
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
		return new StringAggregatorIterator(list);
		// some code goes here
		
	}
	public static class StringAggregatorIterator implements DbIterator{

		List <Tuple> list;
		Iterator <Tuple> it;
		public StringAggregatorIterator(List<Tuple> list) {
			// TODO Auto-generated constructor stub
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

	

}
