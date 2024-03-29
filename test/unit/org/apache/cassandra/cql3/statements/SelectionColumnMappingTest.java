package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SelectionColumnMappingTest
{
    private static final CFDefinition.Name NULL_DEF = null;
    static String KEYSPACE = "selection_column_mapping_test_ks";
    String tableName = "test_table";

    @BeforeClass
    public static void setupSchema() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                                          "WITH replication = {'class': 'SimpleStrategy', " +
                                          "                    'replication_factor': '1'}",
                                          KEYSPACE));
    }

    @Test
    public void testSelectionColumnMapping() throws Throwable
    {
        // Organised as a single test to avoid the overhead of
        // table creation for each variant
        tableName = "table1";
        createTable("CREATE TABLE %s (" +
                    " k int PRIMARY KEY," +
                    " v1 int," +
                    " v2 ascii)");
        insert("INSERT INTO %s (k, v1 ,v2) VALUES (1, 1, 'foo')");

        testSimpleTypes();
        testWildcard();
        testSimpleTypesWithAliases();
        testWritetimeAndTTL();
        testWritetimeAndTTLWithAliases();
        testFunction();
        testFunctionWithAlias();
        testNoArgumentFunction();
        testNestedFunctions();
        testNestedFunctionsWithArguments();
        testCount();
        testDuplicateFunctionsWithoutAliases();
        testDuplicateFunctionsWithAliases();
        testSelectDistinct();
        testMultipleAliasesOnSameColumn();
        testMixedColumnTypes();
        testMultipleUnaliasedSelectionOfSameColumn();
    }

    @Test
    public void testMultipleArgumentFunction() throws Throwable
    {
        // token() is currently the only function which accepts multiple arguments
        tableName = "table2";
        createTable("CREATE TABLE %s (a int, b text, PRIMARY KEY ((a, b)))");
        ColumnSpecification tokenSpec = columnSpecification("token(a, b)", BytesType.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(tokenSpec, columnDefinitions("a", "b"));

        // we don't use verify like with the other tests because this query will produce no results
        SelectStatement statement = getSelect("SELECT token(a,b) FROM %s");
        verifyColumnMapping(expected, statement);
        statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT);
    }

    private void testSimpleTypes() throws Throwable
    {
        // simple column identifiers without aliases are represented in
        // ResultSet.Metadata by the underlying ColumnDefinition
        CFDefinition.Name kDef = columnDefinition("k");
        CFDefinition.Name v1Def = columnDefinition("v1");
        CFDefinition.Name v2Def = columnDefinition("v2");
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(kDef, columnDefinition("k"))
                                                          .addMapping(v1Def, columnDefinition("v1"))
                                                          .addMapping(v2Def, columnDefinition("v2"));

        verify(expected, "SELECT k, v1, v2 FROM %s");
    }

    private void testWildcard() throws Throwable
    {
        // Wildcard select should behave just as though we had
        // explicitly selected each column
        CFDefinition.Name kDef = columnDefinition("k");
        CFDefinition.Name v1Def = columnDefinition("v1");
        CFDefinition.Name v2Def = columnDefinition("v2");
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(kDef, columnDefinition("k"))
                                                          .addMapping(v1Def, columnDefinition("v1"))
                                                          .addMapping(v2Def, columnDefinition("v2"));

        verify(expected, "SELECT * FROM %s");
    }

    private void testSimpleTypesWithAliases() throws Throwable
    {
        // simple column identifiers with aliases are represented in ResultSet.Metadata
        // by a ColumnSpecification based on the underlying ColumnDefinition
        ColumnSpecification kSpec = columnSpecification("k_alias", Int32Type.instance);
        ColumnSpecification v1Spec = columnSpecification("v1_alias", Int32Type.instance);
        ColumnSpecification v2Spec = columnSpecification("v2_alias", AsciiType.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(kSpec, columnDefinition("k"))
                                                          .addMapping(v1Spec, columnDefinition("v1"))
                                                          .addMapping(v2Spec, columnDefinition("v2"));

        verify(expected, "SELECT k AS k_alias, v1 AS v1_alias, v2 AS v2_alias FROM %s");
    }

    private void testWritetimeAndTTL() throws Throwable
    {
        // writetime and ttl are represented in ResultSet.Metadata by a ColumnSpecification
        // with the function name plus argument and a long or int type respectively
        ColumnSpecification wtSpec = columnSpecification("writetime(v1)", LongType.instance);
        ColumnSpecification ttlSpec = columnSpecification("ttl(v2)", Int32Type.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(wtSpec, columnDefinition("v1"))
                                                          .addMapping(ttlSpec, columnDefinition("v2"));

        verify(expected, "SELECT writetime(v1), ttl(v2) FROM %s");
    }

    private void testWritetimeAndTTLWithAliases() throws Throwable
    {
        // writetime and ttl with aliases are represented in ResultSet.Metadata
        // by a ColumnSpecification with the alias name and the appropriate numeric type
        ColumnSpecification wtSpec = columnSpecification("wt_alias", LongType.instance);
        ColumnSpecification ttlSpec = columnSpecification("ttl_alias", Int32Type.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(wtSpec, columnDefinition("v1"))
                                                          .addMapping(ttlSpec, columnDefinition("v2"));

        verify(expected, "SELECT writetime(v1) AS wt_alias, ttl(v2) AS ttl_alias FROM %s");
    }

    private void testFunction() throws Throwable
    {
        // a function such as intasblob(<col>) is represented in ResultSet.Metadata
        // by a ColumnSpecification with the function name plus args and the type set
        // to the function's return type
        ColumnSpecification fnSpec = columnSpecification("intasblob(v1)", BytesType.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(fnSpec, columnDefinition("v1"));

        verify(expected, "SELECT intasblob(v1) FROM %s");
    }

    private void testFunctionWithAlias() throws Throwable
    {
        // a function with an alias is represented in ResultSet.Metadata by a
        // ColumnSpecification with the alias and the type set to the function's
        // return type
        ColumnSpecification fnSpec = columnSpecification("fn_alias", BytesType.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(fnSpec, columnDefinition("v1"));

        verify(expected, "SELECT intasblob(v1) AS fn_alias FROM %s");
    }

    public void testNoArgumentFunction() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("now()",
                                                                                          TimeUUIDType.instance),
                                                                      NULL_DEF);
        verify(expected, "SELECT now() FROM %s");
    }

    public void testNestedFunctionsWithArguments() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("blobasint(intasblob(v1))",
                                                                                          Int32Type.instance),
                                                                      columnDefinition("v1"));
        verify(expected, "SELECT blobasint(intasblob(v1)) FROM %s");
    }

    public void testNestedFunctions() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("unixtimestampof(now())",
                                                                                          LongType.instance),
                                                                      NULL_DEF);
        verify(expected, "SELECT unixtimestampof(now()) FROM %s");
    }

    public void testCount() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("count", LongType.instance),
                                                                      NULL_DEF);
        verify(expected, "SELECT count(*) FROM %s");
        verify(expected, "SELECT count(1) FROM %s");

        expected = SelectionColumnMapping.newMapping()
                                         .addMapping(columnSpecification("other_count", LongType.instance), NULL_DEF);
        verify(expected, "SELECT count(*) AS other_count FROM %s");
        verify(expected, "SELECT count(1) AS other_count FROM %s");
    }

    public void testDuplicateFunctionsWithoutAliases() throws Throwable
    {
        // where duplicate functions are present, the ColumnSpecification list will
        // contain an entry per-duplicate but the mappings will be deduplicated (i.e.
        // a single mapping k/v pair regardless of the number of duplicates)
        ColumnSpecification spec = columnSpecification("intasblob(v1)", BytesType.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(spec, columnDefinition("v1"))
                                                          .addMapping(spec, columnDefinition("v1"));
        verify(expected, "SELECT intasblob(v1), intasblob(v1) FROM %s");
    }

    public void testDuplicateFunctionsWithAliases() throws Throwable
    {
        // where duplicate functions are present with distinct aliases, they are
        // represented as any other set of distinct columns would be - an entry
        // in theColumnSpecification list and a separate k/v mapping for each
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(columnSpecification("blob_1", BytesType.instance),
                                                                      columnDefinition("v1"))
                                                          .addMapping(columnSpecification("blob_2", BytesType.instance),
                                                                      columnDefinition("v1"));
        verify(expected, "SELECT intasblob(v1) AS blob_1, intasblob(v1) AS blob_2 FROM %s");
    }

    public void testSelectDistinct() throws Throwable
    {
        SelectionColumns expected = SelectionColumnMapping.newMapping().addMapping(columnDefinition("k"),
                                                                                   columnDefinition("k"));
        verify(expected, "SELECT DISTINCT k FROM %s");

    }

    private void testMultipleAliasesOnSameColumn() throws Throwable
    {
        // Multiple result columns derived from the same underlying column are
        // represented by ColumnSpecifications
        ColumnSpecification alias1 = columnSpecification("alias_1", Int32Type.instance);
        ColumnSpecification alias2 = columnSpecification("alias_2", Int32Type.instance);
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(alias1, columnDefinition("v1"))
                                                          .addMapping(alias2, columnDefinition("v1"));
        verify(expected, "SELECT v1 AS alias_1, v1 AS alias_2 FROM %s");
    }

    private void testMultipleUnaliasedSelectionOfSameColumn() throws Throwable
    {
        // simple column identifiers without aliases are represented in
        // ResultSet.Metadata by the underlying ColumnDefinition
        CFDefinition.Name v1 = columnDefinition("v1");
        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(v1, v1)
                                                          .addMapping(v1, v1);

        verify(expected, "SELECT v1, v1 FROM %s");
    }

    private void testMixedColumnTypes() throws Throwable
    {
        ColumnSpecification kSpec = columnSpecification("k_alias", Int32Type.instance);
        ColumnSpecification v1Spec = columnSpecification("writetime(v1)", LongType.instance);
        ColumnSpecification v2Spec = columnSpecification("ttl_alias", Int32Type.instance);

        SelectionColumns expected = SelectionColumnMapping.newMapping()
                                                          .addMapping(kSpec, columnDefinition("k"))
                                                          .addMapping(v1Spec, columnDefinition("v1"))
                                                          .addMapping(v2Spec, columnDefinition("v2"));

        verify(expected, "SELECT k AS k_alias," +
                         "       writetime(v1)," +
                         "       ttl(v2) as ttl_alias" +
                         " FROM %s");
    }

    private void verify(SelectionColumns expected, String query) throws Throwable
    {
        SelectStatement statement = getSelect(query);
        verifyColumnMapping(expected, statement);
        checkExecution(statement, expected.getColumnSpecifications());
    }

    private void checkExecution(SelectStatement statement, List<ColumnSpecification> expectedResultColumns)
    throws RequestExecutionException, RequestValidationException
    {
        UntypedResultSet rs = new UntypedResultSet(statement.executeInternal(QueryState.forInternalCalls(),
                                                                             QueryOptions.DEFAULT).result);

        assertEquals(expectedResultColumns, rs.one().getColumns());
    }

    private SelectStatement getSelect(String query) throws RequestValidationException
    {
        CQLStatement statement = QueryProcessor.getStatement(String.format(query, KEYSPACE + "." + tableName),
                                                             ClientState.forInternalCalls()).statement;
        assertTrue(statement instanceof SelectStatement);
        return (SelectStatement)statement;
    }

    private void verifyColumnMapping(SelectionColumns expected, SelectStatement select)
    {
        assertEquals(expected, select.getSelection().getColumnMapping());
    }

    private CFDefinition.Name columnDefinition(String name)
    {
        return Schema.instance.getCFMetaData(KEYSPACE, tableName)
                              .getCfDef()
                              .get(new ColumnIdentifier(name, true));

    }

    private Iterable<CFDefinition.Name> columnDefinitions(String...name)
    {
        List<CFDefinition.Name> list = new ArrayList<>();
        for (String n : name)
            list.add(columnDefinition(n));
        return list;
    }

    private ColumnSpecification columnSpecification(String name, AbstractType<?> type)
    {
        return new ColumnSpecification(KEYSPACE,
                                       tableName,
                                       new ColumnIdentifier(name, true),
                                       type);
    }

    private void insert(String cql)
    {
        QueryProcessor.processInternal(String.format(cql, KEYSPACE + "." + tableName));
    }

    private void createTable(String query) throws Throwable
    {
        executeSchemaChange(String.format(query, KEYSPACE + "." + tableName));
    }

    private static void executeSchemaChange(String query) throws Throwable
    {
        try
        {
            process(query, ConsistencyLevel.ONE);
        }
        catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }
}
