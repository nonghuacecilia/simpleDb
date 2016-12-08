package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends AbstractDbIterator
{
	DbIterator child;//give tuple
	int afield;
	int gfield;
	Aggregator.Op aop;
	
	Aggregator add;
	DbIterator it;//aggregator iterator
	/**
	 * Constructor.
	 *
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an IntAggregator or StringAggregator to help you with your
	 * implementation of readNext().
	 * 
	 *
	 * @param child
	 *            The DbIterator that is feeding us tuples.
	 * @param afield
	 *            The column over which we are computing an aggregate.
	 * @param gfield
	 *            The column over which we are grouping the result, or -1 if
	 *            there is no grouping
	 * @param aop
	 *            The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop)
	{
		// some code goes here
		this.child=child;
		this.afield=afield;
		this.gfield=gfield;
		this.aop=aop;
		
		it=null;
		Type gfieldType ;
		Type afieldType;
		if(this.gfield==Aggregator.NO_GROUPING){// no group
			gfieldType=null;
		}else{// has group
			gfieldType=this.child.getTupleDesc().getType(gfield);
		}
		
		
		afieldType=this.child.getTupleDesc().getType(afield);
		if(Type.INT_TYPE==afieldType){
			add= new IntAggregator(gfield, gfieldType, afield, aop);
		}else{
			
			add= new StringAggregator(gfield, gfieldType, afield, aop);
		}
		try {
			this.it=createAggregate();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	private DbIterator createAggregate() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		child.open();
		while(child.hasNext()){
			this.add.merge(child.next());
			
		}
		
		return it=add.iterator();
		
		
	}


	public static String aggName(Aggregator.Op aop)
	{
		switch (aop)
		{
		case MIN:
			return "min";
		case MAX:
			return "max";
		case AVG:
			return "avg";
		case SUM:
			return "sum";
		case COUNT:
			return "count";
		}
		return "";
	}

	
	public void open() throws NoSuchElementException, DbException,
			TransactionAbortedException
	{
		// some code goes here
		it.open();
		
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException
	{
		// some code goes here
		if(it.hasNext()){
			return it.next();
		}
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException
	{
		// some code goes here
		it.close();
		it.open();
	
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc()
	{
		if(gfield==Aggregator.NO_GROUPING){
			Type [] type={child.getTupleDesc().getType(afield)};
			String [] field ={child.getTupleDesc().getFieldName(afield)};
			TupleDesc tupdecnew=new TupleDesc(type,field);
			return tupdecnew;
		}else{
			Type [] type={child.getTupleDesc().getType(gfield),
						child.getTupleDesc().getType(afield)};
			String [] field ={child.getTupleDesc().getFieldName(gfield),child.getTupleDesc().getFieldName(afield)};
			TupleDesc tupdecnew=new TupleDesc(type,field);
			
			return tupdecnew;
		}
		// some code goes here
	
	}

	public void close()
	{
		// some code goes here
		it.close();
	}
}
