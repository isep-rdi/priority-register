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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import javax.management.openmbean.*;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionHistoryTabularData;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.thrift.cassandraConstants;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

public class SystemKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspace.class);

    // see CFMetaData for schema definitions
    public static final String PEERS_CF = "peers";
    public static final String PEER_EVENTS_CF = "peer_events";
    public static final String LOCAL_CF = "local";
    public static final String INDEX_CF = "IndexInfo";
    public static final String COUNTER_ID_CF = "NodeIdInfo";
    public static final String HINTS_CF = "hints";
    public static final String RANGE_XFERS_CF = "range_xfers";
    public static final String BATCHLOG_CF = "batchlog";
    // see layout description in the DefsTables class header
    public static final String SCHEMA_KEYSPACES_CF = "schema_keyspaces";
    public static final String SCHEMA_COLUMNFAMILIES_CF = "schema_columnfamilies";
    public static final String SCHEMA_COLUMNS_CF = "schema_columns";
    public static final String SCHEMA_TRIGGERS_CF = "schema_triggers";
    public static final String COMPACTION_LOG = "compactions_in_progress";
    public static final String PAXOS_CF = "paxos";
    public static final String SSTABLE_ACTIVITY_CF = "sstable_activity";
    public static final String COMPACTION_HISTORY_CF = "compaction_history";

    private static final String LOCAL_KEY = "local";
    private static final ByteBuffer ALL_LOCAL_NODE_ID_KEY = ByteBufferUtil.bytes("Local");

    public static final List<String> allSchemaCfs = Arrays.asList(SCHEMA_KEYSPACES_CF,
                                                                  SCHEMA_COLUMNFAMILIES_CF,
                                                                  SCHEMA_COLUMNS_CF,
                                                                  SCHEMA_TRIGGERS_CF);

    private static volatile Map<UUID, Pair<ReplayPosition, Long>> truncationRecords;

    public enum BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS
    }

    private static DecoratedKey decorate(ByteBuffer key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }

    public static void finishStartup()
    {
        setupVersion();

        copyAllAliasesToColumnsProper();

        // add entries to system schema columnfamilies for the hardcoded system definitions
        KSMetaData ksmd = Schema.instance.getKSMetaData(Keyspace.SYSTEM_KS);

        // delete old, possibly obsolete entries in schema columnfamilies
        for (String cfname : Arrays.asList(SystemKeyspace.SCHEMA_KEYSPACES_CF,
                                           SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF,
                                           SystemKeyspace.SCHEMA_COLUMNS_CF,
                                           SystemKeyspace.SCHEMA_TRIGGERS_CF))
            processInternal(String.format("DELETE FROM system.%s WHERE keyspace_name = '%s'", cfname, ksmd.name));

        // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
        ksmd.toSchema(FBUtilities.timestampMicros() + 1).apply();
    }

    // Starting with 2.0 (CASSANDRA-5125) we keep all the 'aliases' in system.schema_columns together with the regular columns,
    // but only for the newly-created tables. This migration is for the pre-2.0 created tables.
    private static void copyAllAliasesToColumnsProper()
    {
        for (UntypedResultSet.Row row : processInternal(String.format("SELECT * FROM system.%s", SCHEMA_COLUMNFAMILIES_CF)))
        {
            CFMetaData table = CFMetaData.fromSchema(row);
            String query = String.format("SELECT writetime(type) "
                                         + "FROM system.%s "
                                         + "WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                                         SCHEMA_COLUMNFAMILIES_CF,
                                         table.ksName,
                                         table.cfName);
            long timestamp = processInternal(query).one().getLong("writetime(type)");
            try
            {
                table.toSchema(timestamp).apply();
            }
            catch (ConfigurationException e)
            {
                // shouldn't happen
            }
        }
    }

    private static void setupVersion()
    {
        String req = "INSERT INTO system.%s (key, release_version, cql_version, thrift_version, native_protocol_version, data_center, rack, partitioner, rpc_address, broadcast_address, listen_address) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        processInternal(String.format(req, LOCAL_CF,
                                         LOCAL_KEY,
                                         FBUtilities.getReleaseVersionString(),
                                         QueryProcessor.CQL_VERSION.toString(),
                                         cassandraConstants.VERSION,
                                         Server.CURRENT_VERSION,
                                         snitch.getDatacenter(FBUtilities.getBroadcastAddress()),
                                         snitch.getRack(FBUtilities.getBroadcastAddress()),
                                         DatabaseDescriptor.getPartitioner().getClass().getName(),
                                         DatabaseDescriptor.getRpcAddress().getHostAddress(),
                                         FBUtilities.getBroadcastAddress().getHostAddress(),
                                         FBUtilities.getLocalAddress().getHostAddress()));
    }

    /**
     * Write compaction log, except columfamilies under system keyspace.
     *
     * @param cfs
     * @param toCompact sstables to compact
     * @return compaction task id or null if cfs is under system keyspace
     */
    public static UUID startCompaction(ColumnFamilyStore cfs, Iterable<SSTableReader> toCompact)
    {
        if (Keyspace.SYSTEM_KS.equals(cfs.keyspace.getName()))
            return null;

        UUID compactionId = UUIDGen.getTimeUUID();
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, inputs) VALUES (%s, '%s', '%s', {%s})";
        Iterable<Integer> generations = Iterables.transform(toCompact, new Function<SSTableReader, Integer>()
        {
            public Integer apply(SSTableReader sstable)
            {
                return sstable.descriptor.generation;
            }
        });
        processInternal(String.format(req, COMPACTION_LOG, compactionId, cfs.keyspace.getName(), cfs.name, StringUtils.join(Sets.newHashSet(generations), ',')));
        forceBlockingFlush(COMPACTION_LOG);
        return compactionId;
    }

    /**
     * Deletes the entry for this compaction from the set of compactions in progress.  The compaction does not need
     * to complete successfully for this to be called.
     * @param taskId what was returned from {@code startCompaction}
     */
    public static void finishCompaction(UUID taskId)
    {
        assert taskId != null;

        String req = "DELETE FROM system.%s WHERE id = %s";
        processInternal(String.format(req, COMPACTION_LOG, taskId));
        forceBlockingFlush(COMPACTION_LOG);
    }

    /**
     * Returns a Map whose keys are KS.CF pairs and whose values are maps from sstable generation numbers to the
     * task ID of the compaction they were participating in.
     */
    public static Map<Pair<String, String>, Map<Integer, UUID>> getUnfinishedCompactions()
    {
        String req = "SELECT * FROM system.%s";
        UntypedResultSet resultSet = processInternal(String.format(req, COMPACTION_LOG));

        Map<Pair<String, String>, Map<Integer, UUID>> unfinishedCompactions = new HashMap<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            String keyspace = row.getString("keyspace_name");
            String columnfamily = row.getString("columnfamily_name");
            Set<Integer> inputs = row.getSet("inputs", Int32Type.instance);
            UUID taskID = row.getUUID("id");

            Pair<String, String> kscf = Pair.create(keyspace, columnfamily);
            Map<Integer, UUID> generationToTaskID = unfinishedCompactions.get(kscf);
            if (generationToTaskID == null)
                generationToTaskID = new HashMap<>(inputs.size());

            for (Integer generation : inputs)
                generationToTaskID.put(generation, taskID);

            unfinishedCompactions.put(kscf, generationToTaskID);
        }
        return unfinishedCompactions;
    }

    public static void discardCompactionsInProgress()
    {
        ColumnFamilyStore compactionLog = Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(COMPACTION_LOG);
        compactionLog.truncateBlocking();
    }

    public static void updateCompactionHistory(String ksname,
                                               String cfname,
                                               long compactedAt,
                                               long bytesIn,
                                               long bytesOut,
                                               Map<Integer, Long> rowsMerged)
    {
        // don't write anything when the history table itself is compacted, since that would in turn cause new compactions
        if (ksname.equals("system") && cfname.equals(COMPACTION_HISTORY_CF))
            return;
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged) "
                     + "VALUES (%s, '%s', '%s', %d, %d, %d, {%s})";
        processInternal(String.format(req, COMPACTION_HISTORY_CF, UUIDGen.getTimeUUID().toString(), ksname, cfname, compactedAt, bytesIn, bytesOut, FBUtilities.toString(rowsMerged)));
    }

    public static TabularData getCompactionHistory() throws OpenDataException
    {
        UntypedResultSet queryResultSet = processInternal("SELECT * from system.compaction_history");
        return CompactionHistoryTabularData.from(queryResultSet);
    }

    public static synchronized void saveTruncationRecord(ColumnFamilyStore cfs, long truncatedAt, ReplayPosition position)
    {
        String req = "UPDATE system.%s SET truncated_at = truncated_at + %s WHERE key = '%s'";
        processInternal(String.format(req, LOCAL_CF, truncationAsMapEntry(cfs, truncatedAt, position), LOCAL_KEY));
        truncationRecords = null;
        forceBlockingFlush(LOCAL_CF);
    }

    /**
     * This method is used to remove information about truncation time for specified column family
     */
    public static synchronized void removeTruncationRecord(UUID cfId)
    {
        String req = "DELETE truncated_at[%s] from system.%s WHERE key = '%s'";
        processInternal(String.format(req, cfId, LOCAL_CF, LOCAL_KEY));
        truncationRecords = null;
        forceBlockingFlush(LOCAL_CF);
    }

    private static String truncationAsMapEntry(ColumnFamilyStore cfs, long truncatedAt, ReplayPosition position)
    {
        DataOutputBuffer out = new DataOutputBuffer();
        try
        {
            ReplayPosition.serializer.serialize(position, out);
            out.writeLong(truncatedAt);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return String.format("{%s: 0x%s}",
                             cfs.metadata.cfId,
                             ByteBufferUtil.bytesToHex(ByteBuffer.wrap(out.getData(), 0, out.getLength())));
    }

    public static ReplayPosition getTruncatedPosition(UUID cfId)
    {
        Pair<ReplayPosition, Long> record = getTruncationRecord(cfId);
        return record == null ? null : record.left;
    }

    public static long getTruncatedAt(UUID cfId)
    {
        Pair<ReplayPosition, Long> record = getTruncationRecord(cfId);
        return record == null ? Long.MIN_VALUE : record.right;
    }

    private static synchronized Pair<ReplayPosition, Long> getTruncationRecord(UUID cfId)
    {
        if (truncationRecords == null)
            truncationRecords = readTruncationRecords();
        return truncationRecords.get(cfId);
    }

    private static Map<UUID, Pair<ReplayPosition, Long>> readTruncationRecords()
    {
        UntypedResultSet rows = processInternal(String.format("SELECT truncated_at FROM system.%s WHERE key = '%s'",
                                                              LOCAL_CF,
                                                              LOCAL_KEY));

        Map<UUID, Pair<ReplayPosition, Long>> records = new HashMap<>();

        if (!rows.isEmpty() && rows.one().has("truncated_at"))
        {
            Map<UUID, ByteBuffer> map = rows.one().getMap("truncated_at", UUIDType.instance, BytesType.instance);
            for (Map.Entry<UUID, ByteBuffer> entry : map.entrySet())
                records.put(entry.getKey(), truncationRecordFromBlob(entry.getValue()));
        }

        return records;
    }

    private static Pair<ReplayPosition, Long> truncationRecordFromBlob(ByteBuffer bytes)
    {
        try
        {
            DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(bytes));
            return Pair.create(ReplayPosition.serializer.deserialize(in), in.available() > 0 ? in.readLong() : Long.MIN_VALUE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Record tokens being used by another node
     */
    public static synchronized void updateTokens(InetAddress ep, Collection<Token> tokens)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
        {
            removeEndpoint(ep);
            return;
        }

        String req = "INSERT INTO system.%s (peer, tokens) VALUES ('%s', %s)";
        processInternal(String.format(req, PEERS_CF, ep.getHostAddress(), tokensAsSet(tokens)));
    }

    public static synchronized void updatePreferredIP(InetAddress ep, InetAddress preferred_ip)
    {
        String req = "INSERT INTO system.%s (peer, preferred_ip) VALUES ('%s', '%s')";
        processInternal(String.format(req, PEERS_CF, ep.getHostAddress(), preferred_ip.getHostAddress()));
        forceBlockingFlush(PEERS_CF);
    }

    public static synchronized void updatePeerInfo(InetAddress ep, String columnName, String value)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
            return;

        String req = "INSERT INTO system.%s (peer, %s) VALUES ('%s', %s)";
        processInternal(String.format(req, PEERS_CF, columnName, ep.getHostAddress(), value));
    }

    public static synchronized void updateHintsDropped(InetAddress ep, UUID timePeriod, int value)
    {
        // with 30 day TTL
        String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ %s ] = %s WHERE peer = '%s'";
        processInternal(String.format(req, PEER_EVENTS_CF, timePeriod.toString(), value, ep.getHostAddress()));
    }

    public static synchronized void updateSchemaVersion(UUID version)
    {
        String req = "INSERT INTO system.%s (key, schema_version) VALUES ('%s', %s)";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, version.toString()));
    }

    private static String tokensAsSet(Collection<Token> tokens)
    {
        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        Iterator<Token> iter = tokens.iterator();
        while (iter.hasNext())
        {
            sb.append("'").append(factory.toString(iter.next())).append("'");
            if (iter.hasNext())
                sb.append(",");
        }
        sb.append("}");
        return sb.toString();
    }

    private static Collection<Token> deserializeTokens(Collection<String> tokensStrings)
    {
        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = new ArrayList<Token>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    /**
     * Remove stored tokens being used by another node
     */
    public static synchronized void removeEndpoint(InetAddress ep)
    {
        String req = "DELETE FROM system.%s WHERE peer = '%s'";
        processInternal(String.format(req, PEERS_CF, ep.getHostAddress()));
    }

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
    */
    public static synchronized void updateTokens(Collection<Token> tokens)
    {
        assert !tokens.isEmpty() : "removeEndpoint should be used instead";
        String req = "INSERT INTO system.%s (key, tokens) VALUES ('%s', %s)";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, tokensAsSet(tokens)));
        forceBlockingFlush(LOCAL_CF);
    }

    /**
     * Convenience method to update the list of tokens in the local system keyspace.
     *
     * @param addTokens tokens to add
     * @param rmTokens tokens to remove
     * @return the collection of persisted tokens
     */
    public static synchronized Collection<Token> updateLocalTokens(Collection<Token> addTokens, Collection<Token> rmTokens)
    {
        Collection<Token> tokens = getSavedTokens();
        tokens.removeAll(rmTokens);
        tokens.addAll(addTokens);
        updateTokens(tokens);
        return tokens;
    }

    public static void forceBlockingFlush(String cfname)
    {
        if (!Boolean.getBoolean("cassandra.unsafesystem"))
            FBUtilities.waitOnFuture(Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(cfname).forceFlush());
    }

    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    public static SetMultimap<InetAddress, Token> loadTokens()
    {
        SetMultimap<InetAddress, Token> tokenMap = HashMultimap.create();
        for (UntypedResultSet.Row row : processInternal("SELECT peer, tokens FROM system." + PEERS_CF))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("tokens"))
                tokenMap.putAll(peer, deserializeTokens(row.getSet("tokens", UTF8Type.instance)));
        }

        return tokenMap;
    }

    /**
     * Return a map of store host_ids to IP addresses
     *
     */
    public static Map<InetAddress, UUID> loadHostIds()
    {
        Map<InetAddress, UUID> hostIdMap = new HashMap<InetAddress, UUID>();
        for (UntypedResultSet.Row row : processInternal("SELECT peer, host_id FROM system." + PEERS_CF))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("host_id"))
            {
                hostIdMap.put(peer, row.getUUID("host_id"));
            }
        }
        return hostIdMap;
    }

    /**
     * Get preferred IP for given endpoint if it is known. Otherwise this returns given endpoint itself.
     *
     * @param ep endpoint address to check
     * @return Preferred IP for given endpoint if present, otherwise returns given ep
     */
    public static InetAddress getPreferredIP(InetAddress ep)
    {
        String req = "SELECT preferred_ip FROM system.%s WHERE peer='%s'";
        UntypedResultSet result = processInternal(String.format(req, PEERS_CF, ep.getHostAddress()));
        if (!result.isEmpty() && result.one().has("preferred_ip"))
            return result.one().getInetAddress("preferred_ip");
        return ep;
    }

    /**
     * Return a map of IP addresses containing a map of dc and rack info
     */
    public static Map<InetAddress, Map<String,String>> loadDcRackInfo()
    {
        Map<InetAddress, Map<String, String>> result = new HashMap<InetAddress, Map<String, String>>();
        for (UntypedResultSet.Row row : processInternal("SELECT peer, data_center, rack from system." + PEERS_CF))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("data_center") && row.has("rack"))
            {
                Map<String, String> dcRack = new HashMap<String, String>();
                dcRack.put("data_center", row.getString("data_center"));
                dcRack.put("rack", row.getString("rack"));
                result.put(peer, dcRack);
            }
        }
        return result;
    }

    /**
     * One of three things will happen if you try to read the system keyspace:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad
     * @throws ConfigurationException
     */
    public static void checkHealth() throws ConfigurationException
    {
        Keyspace keyspace;
        try
        {
            keyspace = Keyspace.open(Keyspace.SYSTEM_KS);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            ConfigurationException ex = new ConfigurationException("Could not read system keyspace!");
            ex.initCause(err);
            throw ex;
        }
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(LOCAL_CF);

        String req = "SELECT cluster_name FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        if (result.isEmpty() || !result.one().has("cluster_name"))
        {
            // this is a brand new node
            if (!cfs.getSSTables().isEmpty())
                throw new ConfigurationException("Found system keyspace files, but they couldn't be loaded!");

            // no system files.  this is a new node.
            req = "INSERT INTO system.%s (key, cluster_name) VALUES ('%s', '%s')";
            processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, DatabaseDescriptor.getClusterName()));
            return;
        }

        String savedClusterName = result.one().getString("cluster_name");
        if (!DatabaseDescriptor.getClusterName().equals(savedClusterName))
            throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.getClusterName());
    }

    public static Collection<Token> getSavedTokens()
    {
        String req = "SELECT tokens FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));
        return result.isEmpty() || !result.one().has("tokens")
             ? Collections.<Token>emptyList()
             : deserializeTokens(result.one().<String>getSet("tokens", UTF8Type.instance));
    }

    public static int incrementAndGetGeneration()
    {
        String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        int generation;
        if (result.isEmpty() || !result.one().has("gossip_generation"))
        {
            // seconds-since-epoch isn't a foolproof new generation
            // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
            // but it's as close as sanely possible
            generation = (int) (System.currentTimeMillis() / 1000);
        }
        else
        {
            // Other nodes will ignore gossip messages about a node that have a lower generation than previously seen.
            final int storedGeneration = result.one().getInt("gossip_generation") + 1;
            final int now = (int) (System.currentTimeMillis() / 1000);
            if (storedGeneration >= now)
            {
                logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  See CASSANDRA-3654 if you experience problems",
                            storedGeneration, now);
                generation = storedGeneration;
            }
            else
            {
                generation = now;
            }
        }

        req = "INSERT INTO system.%s (key, gossip_generation) VALUES ('%s', %d)";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, generation));
        forceBlockingFlush(LOCAL_CF);

        return generation;
    }

    public static BootstrapState getBootstrapState()
    {
        String req = "SELECT bootstrapped FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        if (result.isEmpty() || !result.one().has("bootstrapped"))
            return BootstrapState.NEEDS_BOOTSTRAP;

        return BootstrapState.valueOf(result.one().getString("bootstrapped"));
    }

    public static boolean bootstrapComplete()
    {
        return getBootstrapState() == BootstrapState.COMPLETED;
    }

    public static boolean bootstrapInProgress()
    {
        return getBootstrapState() == BootstrapState.IN_PROGRESS;
    }

    public static void setBootstrapState(BootstrapState state)
    {
        String req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', '%s')";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, state.name()));
        forceBlockingFlush(LOCAL_CF);
    }

    public static boolean isIndexBuilt(String keyspaceName, String indexName)
    {
        ColumnFamilyStore cfs = Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(INDEX_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(ByteBufferUtil.bytes(keyspaceName)),
                                                        INDEX_CF,
                                                        FBUtilities.singleton(ByteBufferUtil.bytes(indexName), cfs.getComparator()),
                                                        System.currentTimeMillis());
        return ColumnFamilyStore.removeDeleted(cfs.getColumnFamily(filter), Integer.MAX_VALUE) != null;
    }

    public static void setIndexBuilt(String keyspaceName, String indexName)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(Keyspace.SYSTEM_KS, INDEX_CF);
        cf.addColumn(new Column(ByteBufferUtil.bytes(indexName), ByteBufferUtil.EMPTY_BYTE_BUFFER, FBUtilities.timestampMicros()));
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, ByteBufferUtil.bytes(keyspaceName), cf);
        rm.apply();
        forceBlockingFlush(INDEX_CF);
    }

    public static void setIndexRemoved(String keyspaceName, String indexName)
    {
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, ByteBufferUtil.bytes(keyspaceName));
        rm.delete(INDEX_CF, ByteBufferUtil.bytes(indexName), FBUtilities.timestampMicros());
        rm.apply();
    }

    /**
     * Read the host ID from the system keyspace, creating (and storing) one if
     * none exists.
     */
    public static UUID getLocalHostId()
    {
        UUID hostId = null;

        String req = "SELECT host_id FROM system.%s WHERE key='%s'";
        UntypedResultSet result = processInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        // Look up the Host UUID (return it if found)
        if (!result.isEmpty() && result.one().has("host_id"))
        {
            return result.one().getUUID("host_id");
        }

        // ID not found, generate a new one, persist, and then return it.
        hostId = UUID.randomUUID();
        logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);
        return setLocalHostId(hostId);
    }

    /**
     * Sets the local host ID explicitly.  Should only be called outside of SystemTable when replacing a node.
     */
    public static UUID setLocalHostId(UUID hostId)
    {
        String req = "INSERT INTO system.%s (key, host_id) VALUES ('%s', %s)";
        processInternal(String.format(req, LOCAL_CF, LOCAL_KEY, hostId));
        return hostId;
    }

    /**
     * Read the current local node id from the system keyspace or null if no
     * such node id is recorded.
     */
    public static CounterId getCurrentLocalCounterId()
    {
        Keyspace keyspace = Keyspace.open(Keyspace.SYSTEM_KS);

        // Get the last CounterId (since CounterId are timeuuid is thus ordered from the older to the newer one)
        QueryFilter filter = QueryFilter.getSliceFilter(decorate(ALL_LOCAL_NODE_ID_KEY),
                                                        COUNTER_ID_CF,
                                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                        true,
                                                        1,
                                                        System.currentTimeMillis());
        ColumnFamily cf = keyspace.getColumnFamilyStore(COUNTER_ID_CF).getColumnFamily(filter);
        if (cf != null && cf.getColumnCount() != 0)
            return CounterId.wrap(cf.iterator().next().name());
        else
            return null;
    }

    /**
     * Write a new current local node id to the system keyspace.
     *
     * @param newCounterId the new current local node id to record
     * @param now microsecond time stamp.
     */
    public static void writeCurrentLocalCounterId(CounterId newCounterId, long now)
    {
        ByteBuffer ip = ByteBuffer.wrap(FBUtilities.getBroadcastAddress().getAddress());

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(Keyspace.SYSTEM_KS, COUNTER_ID_CF);
        cf.addColumn(new Column(newCounterId.bytes(), ip, now));
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, ALL_LOCAL_NODE_ID_KEY, cf);
        rm.apply();
        forceBlockingFlush(COUNTER_ID_CF);
    }

    public static List<CounterId.CounterIdRecord> getOldLocalCounterIds()
    {
        List<CounterId.CounterIdRecord> l = new ArrayList<CounterId.CounterIdRecord>();

        Keyspace keyspace = Keyspace.open(Keyspace.SYSTEM_KS);
        QueryFilter filter = QueryFilter.getIdentityFilter(decorate(ALL_LOCAL_NODE_ID_KEY), COUNTER_ID_CF, System.currentTimeMillis());
        ColumnFamily cf = keyspace.getColumnFamilyStore(COUNTER_ID_CF).getColumnFamily(filter);

        CounterId previous = null;
        for (Column c : cf)
        {
            if (previous != null)
                l.add(new CounterId.CounterIdRecord(previous, c.timestamp()));

            // this will ignore the last column on purpose since it is the
            // current local node id
            previous = CounterId.wrap(c.name());
        }
        return l;
    }

    /**
     * @param cfName The name of the ColumnFamily responsible for part of the schema (keyspace, ColumnFamily, columns)
     * @return CFS responsible to hold low-level serialized schema
     */
    public static ColumnFamilyStore schemaCFS(String cfName)
    {
        return Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(cfName);
    }

    public static List<Row> serializedSchema()
    {
        List<Row> schema = new ArrayList<>();

        schema.addAll(serializedSchema(SCHEMA_KEYSPACES_CF));
        schema.addAll(serializedSchema(SCHEMA_COLUMNFAMILIES_CF));
        schema.addAll(serializedSchema(SCHEMA_COLUMNS_CF));
        schema.addAll(serializedSchema(SCHEMA_TRIGGERS_CF));

        return schema;
    }

    /**
     * @param schemaCfName The name of the ColumnFamily responsible for part of the schema (keyspace, ColumnFamily, columns)
     * @return low-level schema representation (each row represents individual Keyspace or ColumnFamily)
     */
    public static List<Row> serializedSchema(String schemaCfName)
    {
        Token minToken = StorageService.getPartitioner().getMinimumToken();

        return schemaCFS(schemaCfName).getRangeSlice(new Range<RowPosition>(minToken.minKeyBound(), minToken.maxKeyBound()),
                                                     null,
                                                     new IdentityQueryFilter(),
                                                     Integer.MAX_VALUE,
                                                     System.currentTimeMillis());
    }

    public static Collection<RowMutation> serializeSchema()
    {
        Map<DecoratedKey, RowMutation> mutationMap = new HashMap<>();

        serializeSchema(mutationMap, SCHEMA_KEYSPACES_CF);
        serializeSchema(mutationMap, SCHEMA_COLUMNFAMILIES_CF);
        serializeSchema(mutationMap, SCHEMA_COLUMNS_CF);
        serializeSchema(mutationMap, SCHEMA_TRIGGERS_CF);

        return mutationMap.values();
    }

    private static void serializeSchema(Map<DecoratedKey, RowMutation> mutationMap, String schemaCfName)
    {
        for (Row schemaRow : serializedSchema(schemaCfName))
        {
            if (Schema.ignoredSchemaRow(schemaRow))
                continue;

            RowMutation mutation = mutationMap.get(schemaRow.key);
            if (mutation == null)
            {
                mutation = new RowMutation(Keyspace.SYSTEM_KS, schemaRow.key.key);
                mutationMap.put(schemaRow.key, mutation);
            }

            mutation.add(schemaRow.cf);
        }
    }

    public static Map<DecoratedKey, ColumnFamily> getSchema(String cfName)
    {
        Map<DecoratedKey, ColumnFamily> schema = new HashMap<DecoratedKey, ColumnFamily>();

        for (Row schemaEntity : SystemKeyspace.serializedSchema(cfName))
            schema.put(schemaEntity.key, schemaEntity.cf);

        return schema;
    }

    public static ByteBuffer getSchemaKSKey(String ksName)
    {
        return AsciiType.instance.fromString(ksName);
    }

    public static Row readSchemaRow(String ksName)
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(getSchemaKSKey(ksName));

        ColumnFamilyStore schemaCFS = SystemKeyspace.schemaCFS(SCHEMA_KEYSPACES_CF);
        ColumnFamily result = schemaCFS.getColumnFamily(QueryFilter.getIdentityFilter(key, SCHEMA_KEYSPACES_CF, System.currentTimeMillis()));

        return new Row(key, result);
    }

    /**
     * Fetches a subset of schema (table data, columns metadata or triggers) for the keyspace+table pair.
     *
     * @param schemaCfName the schema table to get the data from (schema_columnfamilies, schema_columns or schema_triggers)
     * @param ksName the keyspace of the table we are interested in
     * @param cfName the table we are interested in
     * @return a Row containing the schema data of a particular type for the table
     */
    public static Row readSchemaRow(String schemaCfName, String ksName, String cfName)
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(getSchemaKSKey(ksName));
        ColumnFamilyStore schemaCFS = SystemKeyspace.schemaCFS(schemaCfName);
        ColumnFamily cf = schemaCFS.getColumnFamily(key,
                                                    DefsTables.searchComposite(cfName, true),
                                                    DefsTables.searchComposite(cfName, false),
                                                    false,
                                                    Integer.MAX_VALUE,
                                                    System.currentTimeMillis());
        return new Row(key, cf);
    }

    public static PaxosState loadPaxosState(ByteBuffer key, CFMetaData metadata)
    {
        String req = "SELECT * FROM system.%s WHERE row_key = 0x%s AND cf_id = %s";
        UntypedResultSet results = processInternal(String.format(req, PAXOS_CF, ByteBufferUtil.bytesToHex(key), metadata.cfId));
        if (results.isEmpty())
            return new PaxosState(key, metadata);
        UntypedResultSet.Row row = results.one();
        Commit promised = row.has("in_progress_ballot")
                        ? new Commit(key, row.getUUID("in_progress_ballot"), EmptyColumns.factory.create(metadata))
                        : Commit.emptyCommit(key, metadata);
        // either we have both a recently accepted ballot and update or we have neither
        Commit accepted = row.has("proposal")
                        ? new Commit(key, row.getUUID("proposal_ballot"), ColumnFamily.fromBytes(row.getBytes("proposal")))
                        : Commit.emptyCommit(key, metadata);
        // either most_recent_commit and most_recent_commit_at will both be set, or neither
        Commit mostRecent = row.has("most_recent_commit")
                          ? new Commit(key, row.getUUID("most_recent_commit_at"), ColumnFamily.fromBytes(row.getBytes("most_recent_commit")))
                          : Commit.emptyCommit(key, metadata);
        return new PaxosState(promised, accepted, mostRecent);
    }

    public static void savePaxosPromise(Commit promise)
    {
        String req = "UPDATE %s USING TIMESTAMP %d AND TTL %d SET in_progress_ballot = %s WHERE row_key = 0x%s AND cf_id = %s";
        processInternal(String.format(req,
                                      PAXOS_CF,
                                      UUIDGen.microsTimestamp(promise.ballot),
                                      paxosTtl(promise.update.metadata),
                                      promise.ballot,
                                      ByteBufferUtil.bytesToHex(promise.key),
                                      promise.update.id()));
    }

    public static void savePaxosProposal(Commit proposal)
    {
        processInternal(String.format("UPDATE %s USING TIMESTAMP %d AND TTL %d SET proposal_ballot = %s, proposal = 0x%s WHERE row_key = 0x%s AND cf_id = %s",
                                      PAXOS_CF,
                                      UUIDGen.microsTimestamp(proposal.ballot),
                                      paxosTtl(proposal.update.metadata),
                                      proposal.ballot,
                                      ByteBufferUtil.bytesToHex(proposal.update.toBytes()),
                                      ByteBufferUtil.bytesToHex(proposal.key),
                                      proposal.update.id()));
    }

    private static int paxosTtl(CFMetaData metadata)
    {
        // keep paxos state around for at least 3h
        return Math.max(3 * 3600, metadata.getGcGraceSeconds());
    }

    public static void savePaxosCommit(Commit commit)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        String cql = "UPDATE %s USING TIMESTAMP %d AND TTL %d SET proposal_ballot = null, proposal = null, most_recent_commit_at = %s, most_recent_commit = 0x%s WHERE row_key = 0x%s AND cf_id = %s";
        processInternal(String.format(cql,
                                      PAXOS_CF,
                                      UUIDGen.microsTimestamp(commit.ballot),
                                      paxosTtl(commit.update.metadata),
                                      commit.ballot,
                                      ByteBufferUtil.bytesToHex(commit.update.toBytes()),
                                      ByteBufferUtil.bytesToHex(commit.key),
                                      commit.update.id()));
    }

    /**
     * Returns a RestorableMeter tracking the average read rate of a particular SSTable, restoring the last-seen rate
     * from values in system.sstable_activity if present.
     * @param keyspace the keyspace the sstable belongs to
     * @param table the table the sstable belongs to
     * @param generation the generation number for the sstable
     */
    public static RestorableMeter getSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "SELECT * FROM %s WHERE keyspace_name='%s' and columnfamily_name='%s' and generation=%d";
        UntypedResultSet results = processInternal(String.format(cql,
                                                                 SSTABLE_ACTIVITY_CF,
                                                                 keyspace,
                                                                 table,
                                                                 generation));

        if (results.isEmpty())
            return new RestorableMeter();

        UntypedResultSet.Row row = results.one();
        double m15rate = row.getDouble("rate_15m");
        double m120rate = row.getDouble("rate_120m");
        return new RestorableMeter(m15rate, m120rate);
    }

    /**
     * Writes the current read rates for a given SSTable to system.sstable_activity
     */
    public static void persistSSTableReadMeter(String keyspace, String table, int generation, RestorableMeter meter)
    {
        // Store values with a one-day TTL to handle corner cases where cleanup might not occur
        String cql = "INSERT INTO %s (keyspace_name, columnfamily_name, generation, rate_15m, rate_120m) VALUES ('%s', '%s', %d, %f, %f) USING TTL 864000";
        processInternal(String.format(cql,
                                      SSTABLE_ACTIVITY_CF,
                                      keyspace,
                                      table,
                                      generation,
                                      meter.fifteenMinuteRate(),
                                      meter.twoHourRate()));
    }

    /**
     * Clears persisted read rates from system.sstable_activity for SSTables that have been deleted.
     */
    public static void clearSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "DELETE FROM %s WHERE keyspace_name='%s' AND columnfamily_name='%s' and generation=%d";
        processInternal(String.format(cql, SSTABLE_ACTIVITY_CF, keyspace, table, generation));
    }
}
