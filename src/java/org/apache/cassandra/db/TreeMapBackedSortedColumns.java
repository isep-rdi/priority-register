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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import com.google.common.base.Function;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractCompositeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CompositeType.ReplacementComparator;
import org.apache.cassandra.utils.Allocator;

public class TreeMapBackedSortedColumns extends AbstractThreadUnsafeSortedColumns
{
    private final TreeMap<ByteBuffer, Column> map;

    private ReplacementComparator replacementComparator = null;
    public static final ColumnFamily.Factory<TreeMapBackedSortedColumns> factory = new Factory<TreeMapBackedSortedColumns>()
    {
        public TreeMapBackedSortedColumns create(CFMetaData metadata, boolean insertReversed)
        {
            assert !insertReversed;
            return new TreeMapBackedSortedColumns(metadata);
        }
    };

    public AbstractType<?> getComparator()
    {
        return (AbstractType<?>)map.comparator();
    }

    private TreeMapBackedSortedColumns(CFMetaData metadata)
    {
        super(metadata);
        if(metadata.getReplacementOrdering() > 0 && !(metadata.comparator instanceof ReplacementComparator))
        {
            CompositeType composite = (CompositeType)metadata.comparator;
            this.replacementComparator = composite.getReplacementComparator(composite.types, metadata.getReplacementOrdering(), metadata.getReplacementPriority());
            //metadata.comparator = replacementComparator;
            this.map = new TreeMap<ByteBuffer, Column>(replacementComparator);
        }
        else
        {
            this.map = new TreeMap<ByteBuffer, Column>(metadata.comparator);
        }
    }

    private TreeMapBackedSortedColumns(CFMetaData metadata, SortedMap<ByteBuffer, Column> columns)
    {
        super(metadata);
        this.map = new TreeMap<ByteBuffer, Column>(columns);
    }

    public ColumnFamily.Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new TreeMapBackedSortedColumns(metadata, map);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column
    */
    public void addColumn(Column column, Allocator allocator)
    {
        String CFName = metadata.cfName;
        ByteBuffer name = column.name;
//        logger.info("CFName : {}", CFName);
//        logger.info("Column Name : {}", name.hashCode());

        if(metadata.getReplacementOrdering() >= 0)
        {
            List<AbstractCompositeType.CompositeComponent> columnList;

//            logger.info("Comparator Name : {}", new String(metadata.comparator.toString().trim()));
            CompositeType composite = (CompositeType)metadata.comparator;
//            logger.info("composite.getString(name) : {}", new String(composite.getString(name)));

            columnList = composite.deconstructForReplacement(name, metadata.getReplacementOrdering(), metadata.getReplacementPriority(), metadata.getReplacementCql());
//            logger.info("Components Size : {}", columnList.size());

            ByteBuffer replacementKey = composite.replacementKey;
            name = replacementKey;

            column.orderingPriority = columnList;

            Column existingColumn = map.get(name);
            if(existingColumn != null)
            {

                //if(column.orderingPriority.get(0).comparator.compose(column.orderingPriority.get(0).value).compareTo(existingColumn.orderingPriority.get(0).comparator.compose(existingColumn.orderingPriority.get(0).value)) < 0)
                for(int i=0; i < column.orderingPriority.size(); i++)
                {
                    if(column.orderingPriority.get(i).comparator.compare(column.orderingPriority.get(i).value, existingColumn.orderingPriority.get(i).value) < 0)
                    {
                        return;
                    }
                    if(column.orderingPriority.get(i).comparator.compare(column.orderingPriority.get(i).value, existingColumn.orderingPriority.get(i).value) > 0)
                    {
                        map.remove(name);
                        break;
                    }
                }
            }
        }
//        ByteBuffer name = column.name();
        // this is a slightly unusual way to structure this; a more natural way is shown in ThreadSafeSortedColumns,
        // but TreeMap lacks putAbsent.  Rather than split it into a "get, then put" check, we do it as follows,
        // which saves the extra "get" in the no-conflict case [for both normal and super columns],
        // in exchange for a re-put in the SuperColumn case.
        Column oldColumn = map.put(name, column);
        if (oldColumn == null)
            return;

        // calculate reconciled col from old (existing) col and new col
        map.put(name, column.reconcile(oldColumn, allocator));
    }

    /**
     * We need to go through each column in the column container and resolve it before adding
     */
    public void addAll(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation)
    {
        delete(cm.deletionInfo());
        for (Column column : cm)
            addColumn(transformation.apply(column), allocator);
    }

    public boolean replace(Column oldColumn, Column newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        // We are not supposed to put the newColumn is either there was not
        // column or the column was not equal to oldColumn (to be coherent
        // with other implementation). We optimize for the common case where
        // oldColumn do is present though.
        Column previous = map.put(oldColumn.name(), newColumn);
        if (previous == null)
        {
            map.remove(oldColumn.name());
            return false;
        }
        if (!previous.equals(oldColumn))
        {
            map.put(oldColumn.name(), previous);
            return false;
        }
        return true;
    }

    public Column getColumn(ByteBuffer name)
    {
        return map.get(name);
    }

    public void clear()
    {
        setDeletionInfo(DeletionInfo.live());
        map.clear();
    }

    public int getColumnCount()
    {
        return map.size();
    }

    public Collection<Column> getSortedColumns()
    {
        return map.values();
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return map.descendingMap().values();
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        return map.navigableKeySet();
    }

    public Iterator<Column> iterator()
    {
        return map.values().iterator();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(map, slices);
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(map.descendingMap(), slices);
    }
}
