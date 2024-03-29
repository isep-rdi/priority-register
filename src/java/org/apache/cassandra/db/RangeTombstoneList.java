/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data structure holding the range tombstones of a ColumnFamily.
 * <p>
 * This is essentially a sorted list of non-overlapping (tombstone) ranges.
 * <p>
 * A range tombstone has 4 elements: the start and end of the range covered,
 * and the deletion infos (markedAt timestamp and local deletion time). The
 * markedAt timestamp is what define the priority of 2 overlapping tombstones.
 * That is, given 2 tombstones [0, 10]@t1 and [5, 15]@t2, then if t2 > t1 (and
 * are the tombstones markedAt values), the 2nd tombstone take precedence over
 * the first one on [5, 10]. If such tombstones are added to a RangeTombstoneList,
 * the range tombstone list will store them as [[0, 5]@t1, [5, 15]@t2].
 * <p>
 * The only use of the local deletion time is to know when a given tombstone can
 * be purged, which will be done by the purge() method.
 */
public class RangeTombstoneList implements Iterable<RangeTombstone>
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTombstoneList.class);

    public static final Serializer serializer = new Serializer();

    private final Comparator<ByteBuffer> comparator;

    // Note: we don't want to use a List for the markedAts and delTimes to avoid boxing. We could
    // use a List for starts and ends, but having arrays everywhere is almost simpler.
    private ByteBuffer[] starts;
    private ByteBuffer[] ends;
    private long[] markedAts;
    private int[] delTimes;

    private int size;

    private RangeTombstoneList(Comparator<ByteBuffer> comparator, ByteBuffer[] starts, ByteBuffer[] ends, long[] markedAts, int[] delTimes, int size)
    {
        assert starts.length == ends.length && starts.length == markedAts.length && starts.length == delTimes.length;
        this.comparator = comparator;
        this.starts = starts;
        this.ends = ends;
        this.markedAts = markedAts;
        this.delTimes = delTimes;
        this.size = size;
    }

    public RangeTombstoneList(Comparator<ByteBuffer> comparator, int capacity)
    {
        this(comparator, new ByteBuffer[capacity], new ByteBuffer[capacity], new long[capacity], new int[capacity], 0);
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int size()
    {
        return size;
    }

    public Comparator<ByteBuffer> comparator()
    {
        return comparator;
    }

    public RangeTombstoneList copy()
    {
        return new RangeTombstoneList(comparator,
                                      Arrays.copyOf(starts, size),
                                      Arrays.copyOf(ends, size),
                                      Arrays.copyOf(markedAts, size),
                                      Arrays.copyOf(delTimes, size),
                                      size);
    }

    public void add(RangeTombstone tombstone)
    {
        add(tombstone.min, tombstone.max, tombstone.data.markedForDeleteAt, tombstone.data.localDeletionTime);
    }

    /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case),
     * but it doesn't assume it.
     */
    public void add(ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        if (isEmpty())
        {
            addInternal(0, start, end, markedAt, delTime);
            return;
        }

        int c = comparator.compare(ends[size-1], start);

        // Fast path if we add in sorted order
        if (c < 0)
        {
            addInternal(size, start, end, markedAt, delTime);
        }
        else
        {
            // Note: insertFrom expect i to be the insertion point in term of interval ends
            int pos = Arrays.binarySearch(ends, 0, size, start, comparator);
            insertFrom((pos >= 0 ? pos : -pos-1), start, end, markedAt, delTime);
        }
    }

    /**
     * Adds all the range tombstones of {@code tombstones} to this RangeTombstoneList.
     */
    public void addAll(RangeTombstoneList tombstones)
    {
        if (tombstones.isEmpty())
            return;

        if (isEmpty())
        {
            copyArrays(tombstones, this);
            return;
        }

        /*
         * We basically have 2 techniques we can use here: either we repeatedly call add() on tombstones values,
         * or we do a merge of both (sorted) lists. If this lists is bigger enough than the one we add, then
         * calling add() will be faster, otherwise it's merging that will be faster.
         *
         * Let's note that during memtables updates, it might not be uncommon that a new update has only a few range
         * tombstones, while the CF we're adding it to (the one in the memtable) has many. In that case, using add() is
         * likely going to be faster.
         *
         * In other cases however, like when diffing responses from multiple nodes, the tombstone lists we "merge" will
         * be likely sized, so using add() might be a bit inefficient.
         *
         * Roughly speaking (this ignore the fact that updating an element is not exactly constant but that's not a big
         * deal), if n is the size of this list and m is tombstones size, merging is O(n+m) while using add() is O(m*log(n)).
         *
         * But let's not crank up a logarithm computation for that. Long story short, merging will be a bad choice only
         * if this list size is lot bigger that the other one, so let's keep it simple.
         */
        if (size > 10 * tombstones.size)
        {
            for (int i = 0; i < tombstones.size; i++)
                add(tombstones.starts[i], tombstones.ends[i], tombstones.markedAts[i], tombstones.delTimes[i]);
        }
        else
        {
            int i = 0;
            int j = 0;
            while (i < size && j < tombstones.size)
            {
                if (comparator.compare(tombstones.starts[j], ends[i]) <= 0)
                {
                    insertFrom(i, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
                    j++;
                }
                else
                {
                    i++;
                }
            }
            // Addds the remaining ones from tombstones if any (note that addInternal will increment size if relevant).
            for (; j < tombstones.size; j++)
                addInternal(size, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(ByteBuffer name, long timestamp)
    {
        int idx = searchInternal(name);
        return idx >= 0 && markedAts[idx] >= timestamp;
    }

    /**
     * Returns a new {@link InOrderTester}.
     */
    InOrderTester inOrderTester()
    {
        return new InOrderTester();
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime search(ByteBuffer name) {
        int idx = searchInternal(name);
        return idx < 0 ? null : new DeletionTime(markedAts[idx], delTimes[idx]);
    }

    private int searchInternal(ByteBuffer name)
    {
        if (isEmpty())
            return -1;

        int pos = Arrays.binarySearch(starts, 0, size, name, comparator);
        if (pos >= 0)
        {
            // We're exactly on an interval start. The one subtility is that we need to check if
            // the previous is not equal to us and doesn't have a higher marked at
            if (pos > 0 && comparator.compare(name, ends[pos-1]) == 0 && markedAts[pos-1] > markedAts[pos])
                return pos-1;
            else
                return pos;
        }
        else
        {
            // We potentially intersect the range before our "insertion point"
            int idx = -pos-2;
            if (idx < 0)
                return -1;

            return comparator.compare(name, ends[idx]) <= 0 ? idx : -1;
        }
    }

    public int dataSize()
    {
        int dataSize = TypeSizes.NATIVE.sizeof(size);
        for (int i = 0; i < size; i++)
        {
            dataSize += starts[i].remaining() + ends[i].remaining();
            dataSize += TypeSizes.NATIVE.sizeof(markedAts[i]);
            dataSize += TypeSizes.NATIVE.sizeof(delTimes[i]);
        }
        return dataSize;
    }

    public long minMarkedAt()
    {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < size; i++)
            min = Math.min(min, markedAts[i]);
        return min;
    }

    public long maxMarkedAt()
    {
        long max = Long.MIN_VALUE;
        for (int i = 0; i < size; i++)
            max = Math.max(max, markedAts[i]);
        return max;
    }

    public void updateAllTimestamp(long timestamp)
    {
        for (int i = 0; i < size; i++)
            markedAts[i] = timestamp;
    }

    /**
     * Removes all range tombstones whose local deletion time is older than gcBefore.
     */
    public void purge(int gcBefore)
    {
        int j = 0;
        for (int i = 0; i < size; i++)
        {
            if (delTimes[i] >= gcBefore)
                setInternal(j++, starts[i], ends[i], markedAts[i], delTimes[i]);
        }
        size = j;
    }

    /**
     * Returns whether {@code purge(gcBefore)} would remove something or not.
     */
    public boolean hasPurgeableTombstones(int gcBefore)
    {
        for (int i = 0; i < size; i++)
        {
            if (delTimes[i] < gcBefore)
                return true;
        }
        return false;
    }

    public Iterator<RangeTombstone> iterator()
    {
        return new AbstractIterator<RangeTombstone>()
        {
            private int idx;

            protected RangeTombstone computeNext()
            {
                if (idx >= size)
                    return endOfData();

                RangeTombstone t = new RangeTombstone(starts[idx], ends[idx], markedAts[idx], delTimes[idx]);
                idx++;
                return t;
            }
        };
    }

    /**
     * Evaluates a diff between superset (known to be all merged tombstones) and this list for read repair
     *
     * @return null if there is no difference
     */
    public RangeTombstoneList diff(RangeTombstoneList superset)
    {
        if (isEmpty())
            return superset;

        RangeTombstoneList diff = null;

        int j = 0; // index to iterate through our own list
        for (int i = 0; i < superset.size; i++)
        {
            // we can assume that this list is a subset of the superset list
            while (j < size && comparator.compare(starts[j], superset.starts[i]) < 0)
                j++;

            if (j >= size)
            {
                // we're at the end of our own list, add the remainder of the superset to the diff
                if (i < superset.size)
                {
                    if (diff == null)
                        diff = new RangeTombstoneList(comparator, superset.size - i);

                    for(int k = i; k < superset.size; k++)
                        diff.add(superset.starts[k], superset.ends[k], superset.markedAts[k], superset.delTimes[k]);
                }
                return diff;
            }

            // we don't care about local deletion time here, because it doesn't matter for read repair
            if (!starts[j].equals(superset.starts[i])
                || !ends[j].equals(superset.ends[i])
                || markedAts[j] != superset.markedAts[i])
            {
                if (diff == null)
                    diff = new RangeTombstoneList(comparator, Math.min(8, superset.size - i));
                diff.add(superset.starts[i], superset.ends[i], superset.markedAts[i], superset.delTimes[i]);
            }
        }

        return diff;
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof RangeTombstoneList))
            return false;
        RangeTombstoneList that = (RangeTombstoneList)o;
        if (size != that.size)
            return false;

        for (int i = 0; i < size; i++)
        {
            if (!starts[i].equals(that.starts[i]))
                return false;
            if (!ends[i].equals(that.ends[i]))
                return false;
            if (markedAts[i] != that.markedAts[i])
                return false;
            if (delTimes[i] != that.delTimes[i])
                return false;
        }
        return true;
    }

    @Override
    public final int hashCode()
    {
        int result = size;
        for (int i = 0; i < size; i++)
        {
            result += starts[i].hashCode() + ends[i].hashCode();
            result += (int)(markedAts[i] ^ (markedAts[i] >>> 32));
            result += delTimes[i];
        }
        return result;
    }

    private static void copyArrays(RangeTombstoneList src, RangeTombstoneList dst)
    {
        dst.grow(src.size);
        System.arraycopy(src.starts, 0, dst.starts, 0, src.size);
        System.arraycopy(src.ends, 0, dst.ends, 0, src.size);
        System.arraycopy(src.markedAts, 0, dst.markedAts, 0, src.size);
        System.arraycopy(src.delTimes, 0, dst.delTimes, 0, src.size);
        dst.size = src.size;
    }

    /*
     * Inserts a new element starting at index i. This method assumes that:
     *    ends[i-1] <= start <= ends[i]
     *
     * A RangeTombstoneList is a list of range [s_0, e_0]...[s_n, e_n] such that:
     *   - s_i <= e_i
     *   - e_i <= s_i+1
     *   - if s_i == e_i and e_i == s_i+1 then s_i+1 < e_i+1
     * Basically, range are non overlapping except for their bound and in order. And while
     * we allow ranges with the same value for the start and end, we don't allow repeating
     * such range (so we can't have [0, 0][0, 0] even though it would respect the first 2
     * conditions).
     *
     */
    private void insertFrom(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        while (i < size)
        {
            assert i == 0 || comparator.compare(ends[i-1], start) <= 0;

            int c = comparator.compare(start, ends[i]);
            assert c <= 0;
            if (c == 0)
            {
                // If start == ends[i], then we can insert from the next one (basically the new element
                // really start at the next element), except for the case where starts[i] == ends[i].
                // In this latter case, if we were to move to next element, we could end up with ...[x, x][x, x]...
                if (comparator.compare(starts[i], ends[i]) == 0)
                {
                    // The current element cover a single value which is equal to the start of the inserted
                    // element. If the inserted element overwrites the current one, just remove the current
                    // (it's included in what we insert) and proceed with the insert.
                    if (markedAt > markedAts[i])
                    {
                        removeInternal(i);
                        continue;
                    }

                    // Otherwise (the current singleton interval override the new one), we want to leave the
                    // current element and move to the next, unless start == end since that means the new element
                    // is in fact fully covered by the current one (so we're done)
                    if (comparator.compare(start, end) == 0)
                        return;
                }
                i++;
                continue;
            }

            // Do we overwrite the current element?
            if (markedAt > markedAts[i])
            {
                // We do overwrite.

                // First deal with what might come before the newly added one.
                if (comparator.compare(starts[i], start) < 0)
                {
                    addInternal(i, starts[i], start, markedAts[i], delTimes[i]);
                    i++;
                    // We don't need to do the following line, but in spirit that's what we want to do
                    // setInternal(i, start, ends[i], markedAts, delTime])
                }

                // now, start <= starts[i]

                // Does the new element stops before/at the current one,
                int endCmp = comparator.compare(end, starts[i]);
                if (endCmp <= 0)
                {
                    // Here start <= starts[i] and end <= starts[i]
                    // This means the current element is before the current one. However, one special
                    // case is if end == starts[i] and starts[i] == ends[i]. In that case,
                    // the new element entirely overwrite the current one and we can just overwrite
                    if (endCmp == 0 && comparator.compare(starts[i], ends[i]) == 0)
                        setInternal(i, start, end, markedAt, delTime);
                    else
                        addInternal(i, start, end, markedAt, delTime);
                    return;
                }

                // Do we overwrite the current element fully?
                int cmp = comparator.compare(ends[i], end);
                if (cmp <= 0)
                {
                    // We do overwrite fully:
                    // update the current element until it's end and continue
                    // on with the next element (with the new inserted start == current end).

                    // If we're on the last element, we can optimize
                    if (i == size-1)
                    {
                        setInternal(i, start, end, markedAt, delTime);
                        return;
                    }

                    setInternal(i, start, ends[i], markedAt, delTime);
                    if (cmp == 0)
                        return;

                    start = ends[i];
                    i++;
                }
                else
                {
                    // We don't ovewrite fully. Insert the new interval, and then update the now next
                    // one to reflect the not overwritten parts. We're then done.
                    addInternal(i, start, end, markedAt, delTime);
                    i++;
                    setInternal(i, end, ends[i], markedAts[i], delTimes[i]);
                    return;
                }
            }
            else
            {
                // we don't overwrite the current element

                // If the new interval starts before the current one, insert that new interval
                if (comparator.compare(start, starts[i]) < 0)
                {
                    // If we stop before the start of the current element, just insert the new
                    // interval and we're done; otherwise insert until the beginning of the
                    // current element
                    if (comparator.compare(end, starts[i]) <= 0)
                    {
                        addInternal(i, start, end, markedAt, delTime);
                        return;
                    }
                    addInternal(i, start, starts[i], markedAt, delTime);
                    i++;
                }

                // After that, we're overwritten on the current element but might have
                // some residual parts after ...

                // ... unless we don't extend beyond it.
                if (comparator.compare(end, ends[i]) <= 0)
                    return;

                start = ends[i];
                i++;
            }
        }

        // If we got there, then just insert the remainder at the end
        addInternal(i, start, end, markedAt, delTime);
    }

    private int capacity()
    {
        return starts.length;
    }

    /*
     * Adds the new tombstone at index i, growing and/or moving elements to make room for it.
     */
    private void addInternal(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        assert i >= 0;

        if (size == capacity())
            growToFree(i);
        else if (i < size)
            moveElements(i);

        setInternal(i, start, end, markedAt, delTime);
        size++;
    }

    private void removeInternal(int i)
    {
        assert i >= 0;

        System.arraycopy(starts, i+1, starts, i, size - i - 1);
        System.arraycopy(ends, i+1, ends, i, size - i - 1);
        System.arraycopy(markedAts, i+1, markedAts, i, size - i - 1);
        System.arraycopy(delTimes, i+1, delTimes, i, size - i - 1);

        --size;
        starts[size] = null;
        ends[size] = null;
    }

    /*
     * Grow the arrays, leaving index i "free" in the process.
     */
    private void growToFree(int i)
    {
        int newLength = (capacity() * 3) / 2 + 1;
        grow(i, newLength);
    }

    /*
     * Grow the arrays to match newLength capacity.
     */
    private void grow(int newLength)
    {
        if (capacity() < newLength)
            grow(-1, newLength);
    }

    private void grow(int i, int newLength)
    {
        starts = grow(starts, size, newLength, i);
        ends = grow(ends, size, newLength, i);
        markedAts = grow(markedAts, size, newLength, i);
        delTimes = grow(delTimes, size, newLength, i);
    }

    private static ByteBuffer[] grow(ByteBuffer[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        ByteBuffer[] newA = new ByteBuffer[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static long[] grow(long[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        long[] newA = new long[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static int[] grow(int[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        int[] newA = new int[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    /*
     * Move elements so that index i is "free", assuming the arrays have at least one free slot at the end.
     */
    private void moveElements(int i)
    {
        if (i >= size)
            return;

        System.arraycopy(starts, i, starts, i+1, size - i);
        System.arraycopy(ends, i, ends, i+1, size - i);
        System.arraycopy(markedAts, i, markedAts, i+1, size - i);
        System.arraycopy(delTimes, i, delTimes, i+1, size - i);
    }

    private void setInternal(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        starts[i] = start;
        ends[i] = end;
        markedAts[i] = markedAt;
        delTimes[i] = delTime;
    }

    public static class Serializer implements IVersionedSerializer<RangeTombstoneList>
    {
        private Serializer() {}

        public void serialize(RangeTombstoneList tombstones, DataOutput out, int version) throws IOException
        {
            if (tombstones == null)
            {
                out.writeInt(0);
                return;
            }

            out.writeInt(tombstones.size);
            for (int i = 0; i < tombstones.size; i++)
            {
                ByteBufferUtil.writeWithShortLength(tombstones.starts[i], out);
                ByteBufferUtil.writeWithShortLength(tombstones.ends[i], out);
                out.writeInt(tombstones.delTimes[i]);
                out.writeLong(tombstones.markedAts[i]);
            }
        }

        /*
         * RangeTombstoneList depends on the column family comparator, but it is not serialized.
         * Thus deserialize(DataInput, int, Comparator<ByteBuffer>) should be used instead of this method.
         */
        public RangeTombstoneList deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public RangeTombstoneList deserialize(DataInput in, int version, Comparator<ByteBuffer> comparator) throws IOException
        {
            int size = in.readInt();
            if (size == 0)
                return null;

            RangeTombstoneList tombstones = new RangeTombstoneList(comparator, size);

            for (int i = 0; i < size; i++)
            {
                ByteBuffer start = ByteBufferUtil.readWithShortLength(in);
                ByteBuffer end = ByteBufferUtil.readWithShortLength(in);
                int delTime =  in.readInt();
                long markedAt = in.readLong();

                if (version >= MessagingService.VERSION_20)
                {
                    tombstones.setInternal(i, start, end, markedAt, delTime);
                }
                else
                {
                    /*
                     * The old implementation used to have range sorted by left value, but with potentially
                     * overlapping range. So we need to use the "slow" path.
                     */
                    tombstones.add(start, end, markedAt, delTime);
                }
            }

            // The "slow" path take care of updating the size, but not the fast one
            if (version >= MessagingService.VERSION_20)
                tombstones.size = size;
            return tombstones;
        }

        public long serializedSize(RangeTombstoneList tombstones, TypeSizes typeSizes, int version)
        {
            if (tombstones == null)
                return typeSizes.sizeof(0);

            long size = typeSizes.sizeof(tombstones.size);
            for (int i = 0; i < tombstones.size; i++)
            {
                int startSize = tombstones.starts[i].remaining();
                size += typeSizes.sizeof((short)startSize) + startSize;
                int endSize = tombstones.ends[i].remaining();
                size += typeSizes.sizeof((short)endSize) + endSize;
                size += typeSizes.sizeof(tombstones.delTimes[i]);
                size += typeSizes.sizeof(tombstones.markedAts[i]);
            }
            return size;
        }

        public long serializedSize(RangeTombstoneList tombstones, int version)
        {
            return serializedSize(tombstones, TypeSizes.NATIVE, version);
        }
    }

    /**
     * This object allow testing whether a given column (name/timestamp) is deleted
     * or not by this RangeTombstoneList, assuming that the column given to this
     * object are passed in (comparator) sorted order.
     *
     * This is more efficient that calling RangeTombstoneList.isDeleted() repeatedly
     * in that case since we're able to take the sorted nature of the RangeTombstoneList
     * into account.
     */
    public class InOrderTester
    {
        private int idx;

        public boolean isDeleted(ByteBuffer name, long timestamp)
        {
            while (idx < size)
            {
                int cmp = comparator.compare(name, starts[idx]);
                if (cmp == 0)
                {
                    // As for searchInternal, we need to check the previous end
                    if (idx > 0 && comparator.compare(name, ends[idx-1]) == 0 && markedAts[idx-1] > markedAts[idx])
                        return markedAts[idx-1] >= timestamp;
                    else
                        return markedAts[idx] >= timestamp;
                }
                else if (cmp < 0)
                {
                    return false;
                }
                else
                {
                    if (comparator.compare(name, ends[idx]) <= 0)
                        return markedAts[idx] >= timestamp;
                    else
                        idx++;
                }
            }
            return false;
        }
    }
}
