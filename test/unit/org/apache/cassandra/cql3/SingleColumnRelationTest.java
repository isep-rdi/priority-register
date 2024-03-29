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
package org.apache.cassandra.cql3;

import java.util.Iterator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientState;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;
import static org.junit.Assert.assertEquals;

public class SingleColumnRelationTest
{
    static ClientState clientState;
    static String keyspace = "single_column_relation_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_partition (a int PRIMARY KEY, b int, c text)");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.compound_partition (a int, b int, c text, PRIMARY KEY ((a, b)))");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.partition_with_indices (a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d, e))");
        executeSchemaChange("CREATE INDEX ON %s.partition_with_indices (c)");
        executeSchemaChange("CREATE INDEX ON %s.partition_with_indices (f)");

        clientState = ClientState.forInternalCalls();
    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    private static void executeSchemaChange(String query) throws Throwable
    {
        try
        {
            process(String.format(query, keyspace), ConsistencyLevel.ONE);
        } catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }

    private static UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return processInternal(String.format(query, keyspace));
        } catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }

    @Test
    public void testPartitionWithIndex() throws Throwable
    {
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 0, 0, 0, 0)");
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 0, 1, 0, 1)");
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 0, 1, 1, 2)");

        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 1, 0, 0, 3)");
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 1, 1, 0, 4)");
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 1, 1, 1, 5)");

        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 2, 0, 0, 5)");

        UntypedResultSet results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND b = 0 AND c = 1");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 0, 1, 0, 0, 3);
        checkRow(1, results, 0, 0, 1, 1, 0, 4);
        checkRow(2, results, 0, 0, 1, 1, 1, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND c = 1 ALLOW FILTERING");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 0, 1, 0, 0, 3);
        checkRow(1, results, 0, 0, 1, 1, 0, 4);
        checkRow(2, results, 0, 0, 1, 1, 1, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND b = 0 AND c = 1 AND d = 1");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 0, 4);
        checkRow(1, results, 0, 0, 1, 1, 1, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND c = 1 AND d = 1 ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 0, 4);
        checkRow(1, results, 0, 0, 1, 1, 1, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND b = 0 AND c >= 1 AND f = 5");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
        checkRow(1, results, 0, 0, 2, 0, 0, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND c >= 1 AND f = 5 ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
        checkRow(1, results, 0, 0, 2, 0, 0, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND c = 1 AND d >= 1 AND f = 5 ALLOW FILTERING");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
    }

    @Test
    public void testSliceRestrictionOnPartitionKey() throws Throwable
    {
        assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                             "SELECT * FROM %s.single_partition WHERE a >= 1 and a < 4");
    }

    @Test
    public void testMulticolumnSliceRestrictionOnPartitionKey() throws Throwable
    {
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns: a",
                             "SELECT * FROM %s.single_partition WHERE (a) >= (1) and (a) < (4)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns: a",
                             "SELECT * FROM %s.compound_partition WHERE (a, b) >= (1, 1) and (a, b) < (4, 1)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns: a",
                             "SELECT * FROM %s.compound_partition WHERE a >= 1 and (a, b) < (4, 1)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns: a",
                             "SELECT * FROM %s.compound_partition WHERE b >= 1 and (a, b) < (4, 1)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns: a",
                             "SELECT * FROM %s.compound_partition WHERE (a, b) >= (1, 1) and (b) < (4)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns: b",
                             "SELECT * FROM %s.compound_partition WHERE (b) < (4) and (a, b) >= (1, 1)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns: a",
                             "SELECT * FROM %s.compound_partition WHERE (a, b) >= (1, 1) and a = 1");
    }

    @Test
    public void testMissingPartitionComponentAndFileringOnTheSecondClusteringColumnWithoutAllowFiltering() throws Throwable
    {
        assertInvalidMessage("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING",
                             "SELECT * FROM %s.partition_with_indices WHERE d >= 1 AND f = 5");
    }

    @Test
    public void testMissingPartitionComponentWithSliceRestrictionOnIndexedColumn() throws Throwable
    {
        assertInvalidMessage("Partition key part b must be restricted since preceding part is",
                             "SELECT * FROM %s.partition_with_indices WHERE a = 0 AND c >= 1 ALLOW FILTERING");
    }

    private static void checkRow(int rowIndex, UntypedResultSet results, Integer... expectedValues)
    {
        List<UntypedResultSet.Row> rows = newArrayList(results.iterator());
        UntypedResultSet.Row row = rows.get(rowIndex);
        Iterator<ColumnSpecification> columns = row.getColumns().iterator();
        for (Integer expected : expectedValues)
        {
            String columnName = columns.next().name.toString();
            int actual = row.getInt(columnName);
            assertEquals(String.format("Expected value %d for column %s in row %d, but got %s", actual, columnName, rowIndex, expected),
                         (long) expected, actual);
        }
    }

    private static void assertInvalidMessage(String expectedMsg, String query) throws Throwable
    {
        try
        {
            execute(query);
            Assert.fail("The statement should trigger an InvalidRequestException but did not");
        }
        catch (InvalidRequestException e)
        {
            assertEquals("The error message is not the expected one.",expectedMsg, e.getMessage());
        }
    }
}
