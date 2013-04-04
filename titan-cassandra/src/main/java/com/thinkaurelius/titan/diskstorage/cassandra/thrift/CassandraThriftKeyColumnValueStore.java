package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;

/**
 * A Titan {@code KeyColumnValueStore} backed by Cassandra.
 * This uses the Cassandra Thrift API.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 * @see CassandraThriftStoreManager
 */
public class CassandraThriftKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger =
            LoggerFactory.getLogger(CassandraThriftKeyColumnValueStore.class);

    private final CassandraThriftStoreManager storeManager;
    private final String keyspace;
    private final String columnFamily;
    
    public CassandraThriftKeyColumnValueStore(String keyspace, String columnFamily, CassandraThriftStoreManager storeManager) {
        this.storeManager = storeManager;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    /**
     * Call Cassandra's Thrift get_slice() method.
     * <p/>
     * When columnEnd equals columnStart, and both startInclusive
     * and endInclusive are true, then this method calls
     * {@link #get(java.nio.ByteBuffer, java.nio.ByteBuffer, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction)}
     * instead of calling Thrift's getSlice() method and returns
     * a one-element list containing the result.
     * <p/>
     * When columnEnd equals columnStart and either startInclusive
     * or endInclusive is false (or both are false), then this
     * method returns an empty list without making any Thrift calls.
     * <p/>
     * If columnEnd = columnStart + 1, and both startInclusive and
     * startExclusive are false, then the arguments effectively form
     * an empty interval.  In this case, as in the one previous,
     * an empty list is returned.  However, it may not necessarily
     * be handled efficiently; a Thrift call might still be made
     * before returning the empty list.
     *
     * @throws com.thinkaurelius.titan.diskstorage.StorageException
     *          when columnEnd < columnStart
     */
    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        Preconditions.checkArgument(query.getLimit() >= 0);
        if (0 == query.getLimit()) return ImmutableList.<Entry>of();

        ColumnParent parent = new ColumnParent(columnFamily);        /*
		 * Cassandra cannot handle columnStart = columnEnd.
		 * Cassandra's Thrift getSlice() throws InvalidRequestException
		 * if columnStart = columnEnd.
		 */
        if (ByteBufferUtil.compare(query.getSliceStart(), query.getSliceEnd())>=0) {
            // Check for invalid arguments where columnEnd < columnStart
            if (ByteBufferUtil.isSmallerThan(query.getSliceEnd(), query.getSliceStart())) {
                throw new PermanentStorageException("columnStart=" + query.getSliceStart() +
                        " is greater than columnEnd=" + query.getSliceEnd() + ". " +
                        "columnStart must be less than or equal to columnEnd");
            }
            if (0 != query.getSliceStart().remaining() && 0 != query.getSliceEnd().remaining()) {
                logger.debug("Return empty list due to columnEnd==columnStart and neither empty");
                return ImmutableList.<Entry>of();
            }
        }

        // true: columnStart < columnEnd
        ConsistencyLevel consistency = getTx(txh).getReadConsistencyLevel().getThriftConsistency();
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange();
        range.setCount(query.getLimit());
        range.setStart(query.getSliceStart());
        range.setFinish(query.getSliceEnd());
        predicate.setSlice_range(range);


        CTConnection conn = null;
        CTConnectionPool pool = storeManager.getPool();
        try {
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            List<ColumnOrSuperColumn> rows = client.get_slice(query.getKey(), parent, predicate, consistency);
			/*
			 * The final size of the "result" List may be at most rows.size().
			 * However, "result" could also be up to two elements smaller than
			 * rows.size(), depending on startInclusive and endInclusive
			 */
            List<Entry> result = new ArrayList<Entry>(rows.size());
            for (ColumnOrSuperColumn r : rows) {
                Column c = r.getColumn();

                // Skip column if it is equal to columnEnd because columnEnd is exclusive
                if (query.getSliceEnd().equals(c.bufferForName())) continue;

                result.add(new Entry(c.bufferForName(), c.bufferForValue()));
            }
            
            storeManager.updateCounter(query.getKey());
            
            return result;
        } catch (Exception e) {
        	if (null != conn) {
        		pool.genericInvalidateObject(keyspace, conn);
        		conn = null;
        	}
            throw convertException(e);
        } finally {
            if (null != conn)
            	pool.genericReturnObject(keyspace, conn);
        }
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        ColumnParent parent = new ColumnParent(columnFamily);
        ConsistencyLevel consistency = getTx(txh).getReadConsistencyLevel().getThriftConsistency();
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange();
        range.setCount(1);
        byte[] empty = new byte[0];
        range.setStart(empty);
        range.setFinish(empty);
        predicate.setSlice_range(range);
        CTConnection conn = null;
        CTConnectionPool pool = storeManager.getPool();
        try {
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            List<?> result = client.get_slice(key, parent, predicate, consistency);
            storeManager.updateCounter(key);
            return 0 < result.size();
        } catch (Exception e) {
        	if (null != conn) {
        		pool.genericInvalidateObject(keyspace, conn);
        		conn = null;
        	}
            throw convertException(e);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keyspace, conn);
        }
    }


    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column,
                          StoreTransaction txh) throws StorageException {
        ColumnPath path = new ColumnPath(columnFamily);
        path.setColumn(column);
        CTConnection conn = null;
        CTConnectionPool pool = storeManager.getPool();
        try {
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            ColumnOrSuperColumn result =
                    client.get(key, path, getTx(txh).getReadConsistencyLevel().getThriftConsistency());
            storeManager.updateCounter(key);
            return result.getColumn().bufferForValue();
        } catch (NotFoundException e) {
            return null;
        } catch (Exception e) {
        	if (null != conn) {
        		pool.genericInvalidateObject(keyspace, conn);
        		conn = null;
        	}
            throw convertException(e);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keyspace, conn);
        }
    }


    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
                                     StoreTransaction txh) throws StorageException {
        ColumnParent parent = new ColumnParent(columnFamily);
        ConsistencyLevel consistency = getTx(txh).getReadConsistencyLevel().getThriftConsistency();
        SlicePredicate predicate = new SlicePredicate();
        predicate.setColumn_names(Arrays.asList(column.duplicate()));
        CTConnection conn = null;
        CTConnectionPool pool = storeManager.getPool();
        try {
            conn = pool.genericBorrowObject(keyspace);
            Cassandra.Client client = conn.getClient();
            List<?> result = client.get_slice(key, parent, predicate, consistency);
            storeManager.updateCounter(key);
            return 0 < result.size();
        } catch (Exception ex) {
        	if (null != conn) {
        		pool.genericInvalidateObject(keyspace, conn);
        		conn = null;
        	}
            throw convertException(ex);
        } finally {
            if (null != conn)
                pool.genericReturnObject(keyspace, conn);
        }
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        CTConnection conn = null;
        CTConnectionPool pool = storeManager.getPool();

        final IPartitioner<?> partitioner = storeManager.getCassandraPartitioner();

        if (!(partitioner instanceof RandomPartitioner) && !(partitioner instanceof Murmur3Partitioner))
            throw new PermanentStorageException("This operation is only allowed when random partitioner (md5 or murmur3) is used.");

        final Token maximumToken = (partitioner instanceof RandomPartitioner)
                                    ? new BigIntegerToken(RandomPartitioner.MAXIMUM)
                                    : new LongToken(Murmur3Partitioner.MAXIMUM);
        try {
            conn = pool.genericBorrowObject(keyspace);
            final Cassandra.Client client = conn.getClient();

            return new RecordIterator<ByteBuffer>() {
                Iterator<KeySlice> keys = getKeySlice(client,
                                                      ArrayUtils.EMPTY_BYTE_ARRAY,
                                                      ArrayUtils.EMPTY_BYTE_ARRAY,
                                                      PAGE_SIZE);

                private ByteBuffer lastSeenKey = null;

                @Override
                public boolean hasNext() throws StorageException {
                    boolean hasNext = keys.hasNext();

                    if (!hasNext && lastSeenKey != null) {
                        keys = getKeySlice(client, partitioner.getToken(lastSeenKey), maximumToken, PAGE_SIZE);
                        hasNext = keys.hasNext();
                    }

                    return hasNext;
                }

                @Override
                public ByteBuffer next() throws StorageException {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    KeySlice slice = keys.next();

                    try {
                        return slice.bufferForKey().duplicate();
                    } finally {
                        lastSeenKey = slice.bufferForKey();
                    }
                }

                @Override
                public void close() throws StorageException {
                    // nothing to clean-up here
                }
            };
            
            // Updating counters doesn't really matter for random/murmur3 at the moment
        } catch (Exception e) {
        	if (null != conn) {
        		pool.genericInvalidateObject(keyspace, conn);
        		conn = null;
        	}
            throw convertException(e);
        } finally {
            if (conn != null)
                pool.genericReturnObject(keyspace, conn);
        }
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }


    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransaction txh) throws StorageException {
        Map<ByteBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        mutateMany(mutations, txh);
    }

    public void mutateMany(Map<ByteBuffer, KCVMutation> mutations, StoreTransaction txh) throws StorageException {
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    static final StorageException convertException(Throwable e) {
        if (e instanceof TException) {
            return new PermanentStorageException(e);
        } else if (e instanceof TimedOutException) {
            return new TemporaryStorageException(e);
        } else if (e instanceof UnavailableException) {
            return new TemporaryStorageException(e);
        } else if (e instanceof InvalidRequestException) {
            return new PermanentStorageException(e);
        } else {
            return new PermanentStorageException(e);
        }
    }

    @Override
    public String toString() {
        return "CassandraThriftKeyColumnValueStore[ks="
                + keyspace + ", cf=" + columnFamily + "]";
    }


    private Iterator<KeySlice> getKeySlice(Cassandra.Client client, byte[] startKey, byte[] endKey, int pageSize) throws StorageException {
        return getKeySlice(client, new KeyRange().setStart_key(startKey).setEnd_key(endKey).setCount(pageSize));
    }

    private Iterator<KeySlice> getKeySlice(Cassandra.Client client, Token startToken, Token endToken, int pageSize) throws StorageException {
        return getKeySlice(client, new KeyRange().setStart_token(startToken.token.toString()).setEnd_token(endToken.token.toString()).setCount(pageSize));
    }

    private Iterator<KeySlice> getKeySlice(Cassandra.Client client, KeyRange keyRange) throws StorageException {
        try {
            /* Note: we need to fetch columns for each row as well to remove "range ghosts" */
            return Iterators.filter(client.get_range_slices(new ColumnParent(columnFamily),
                                                            new SlicePredicate()
                                                                    .setSlice_range(new SliceRange()
                                                                                         .setStart(ArrayUtils.EMPTY_BYTE_ARRAY)
                                                                                         .setFinish(ArrayUtils.EMPTY_BYTE_ARRAY)
                                                                                         .setCount(5)),
                                                            keyRange,
                                                            ConsistencyLevel.QUORUM).iterator(), new KeyIterationPredicate());
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    private static class KeyIterationPredicate implements Predicate<KeySlice> {
        @Override
        public boolean apply(@Nullable KeySlice row) {
            return (row == null) ? false : row.getColumns().size() > 0;
        }
    }


}
