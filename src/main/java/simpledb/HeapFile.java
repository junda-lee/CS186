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
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    private final TupleDesc td;
    private final File file;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
    @Override
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();

        try(RandomAccessFile f = new RandomAccessFile(file, "r")){
            if ((pgNo + 1) * BufferPool.getPagesize() > file.length()){
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPagesize()];
            f.seek(pgNo * BufferPool.getPagesize());
            int read = f.read(bytes, 0, BufferPool.getPagesize());
            if (read != BufferPool.getPagesize()){
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(tableId, pgNo);
            return new HeapPage(id, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int num = (int)Math.floor(file.length() * 1.0 / BufferPool.getPagesize());
        return num;
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
    }

    // see DbFile.java for javadocs
    @Override
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    public static final class HeapFileIterator implements  DbFileIterator{

        private final HeapFile heapFile;

        private final TransactionId tid;

        private Iterator<Tuple> it;

        private int whichPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            it = getPageTuple(whichPage);
        }

        public Iterator<Tuple> getPageTuple(int pageNum) throws DbException, TransactionAbortedException {
            if (pageNum < 0 || pageNum >= heapFile.numPages()) {
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNum,heapFile.getId()));
            }
            HeapPageId pid = new HeapPageId(heapFile.getId(), pageNum);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null){
                return false;
            }
            if (!it.hasNext()) {
                if (whichPage < heapFile.numPages() - 1){
                    it = getPageTuple(++whichPage);
                    return it.hasNext();
                }else {
                    return false;
                }
            }else {
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // TODO Auto-generated method stub
            if(it == null || !it.hasNext()){
                throw new NoSuchElementException();
            }
            return it.next();

        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            close();
            open();
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub
            it = null;
        }
    }
}

