package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile
{

	File f;
	TupleDesc td;

	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td)
	{
		// some code goes here
		this.f = f;
		this.td = td;
	}

	public static class HeapFileIterator implements DbFileIterator
	{

		TransactionId tid;
		Iterator<Tuple> it;
		List<Tuple> list;

		public HeapFileIterator(TransactionId tid, List<Tuple> list)
		{
			this.tid = tid;
			this.list = list;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException
		{
			it = this.list.iterator();

		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException
		{
			if (it == null)
				return false;
			
			return it.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException
		{
			if (it == null)
				throw new NoSuchElementException("tuple is null");
			
			return it.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException
		{
			close();
			open();

		}

		@Override
		public void close()
		{
			it = null;
		}

	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile()
	{
		// some code goes here
		return this.f;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId()
	{
		return this.f.getAbsoluteFile().hashCode();
		// throw new UnsupportedOperationException("implement this");
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		return this.td;
		// throw new UnsupportedOperationException("implement this");
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid)
	{
		// some code goes here
		try
		{
			
			RandomAccessFile rAf = new RandomAccessFile(f, "r");
		
			int offset = pid.pageno() * BufferPool.PAGE_SIZE;
			
			rAf.seek(offset);
			
			byte[] page = new byte[BufferPool.PAGE_SIZE];
			
			rAf.read(page, 0, BufferPool.PAGE_SIZE);
			rAf.close();

			HeapPageId id = (HeapPageId) pid;

			return new HeapPage(id, page);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		throw new IllegalArgumentException();
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException
	{
		// some code goes here
		// not necessary for lab1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages()
	{

		// some code goes here
		long file_size = this.f.length();
		int num = (int) (file_size / BufferPool.PAGE_SIZE);
		if (file_size % BufferPool.PAGE_SIZE > 0)
			num++;
		return num;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException
	{
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException
	{
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid)
	{
		// some code goes here
		
		
		List<Tuple> ftupleList = new ArrayList<Tuple>();
		int page_count = this.numPages();
		
		for (int i = 0; i < page_count; i++)
		{
			//PageId pid = new HeapPageId(getId(), i);
			
			PageId pid = null;
			int tableid = getId();
			for ( PageId p : Database.getBufferPool().pageMap.keySet())
			{
				if (tableid == p.getTableId() && i == p.pageno())
					pid = p;
			}
			if (pid == null)
				pid = new HeapPageId(tableid, i);
			HeapPage page = null;
			try
			{
				page = (HeapPage) Database.getBufferPool().getPage(tid, pid,
						Permissions.READ_WRITE);
			} catch (TransactionAbortedException e)
			{
				e.printStackTrace();
			} catch (DbException e)
			{
				e.printStackTrace();
			}
			Iterator<Tuple> it = page.iterator();
			while (it.hasNext())
				ftupleList.add(it.next());
		}

		return new HeapFileIterator(tid, ftupleList);
	}

}