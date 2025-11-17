package com.afsun.lineage.core;

import com.afsun.lineage.core.exceptions.MetadataNotFoundException;
import com.afsun.lineage.core.exceptions.UnsupportedSyntaxException;
import com.afsun.lineage.core.meta.MetadataProvider;
import com.afsun.lineage.graph.ColumnNode;
import com.afsun.lineage.graph.TableNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DefaultSqlLineageParserTest {

    private DefaultSqlLineageParser parser;
    private MetadataProvider mockMetadata;

    @BeforeEach
    void setUp() {
        parser = new DefaultSqlLineageParser();
        mockMetadata = new MetadataProvider() {
            @Override
            public List<ColumnRef> getColumns(String database, String schema, String table) {
                if ("test_db".equalsIgnoreCase(schema) && "users".equalsIgnoreCase(table)) {
                    return Arrays.asList(
                        ColumnRef.of(null, "test_db", "users", "id", null, "test_db", "users", "id"),
                        ColumnRef.of(null, "test_db", "users", "name", null, "test_db", "users", "name"),
                        ColumnRef.of(null, "test_db", "users", "age", null, "test_db", "users", "age")
                    );
                }
                if ("test_db".equalsIgnoreCase(schema) && "orders".equalsIgnoreCase(table)) {
                    return Arrays.asList(
                        ColumnRef.of(null, "test_db", "orders", "order_id", null, "test_db", "orders", "order_id"),
                        ColumnRef.of(null, "test_db", "orders", "user_id", null, "test_db", "orders", "user_id")
                    );
                }
                return Collections.emptyList();
            }
        };
    }

    @Test
    void testSimpleSelect() {
        String sql = "SELECT id, name FROM test_db.users";
        ParseResult result = parser.parse(sql, null, mockMetadata);

        assertNotNull(result);
        assertNotNull(result.getGraph());
        assertTrue(result.getGraph().getTables().size() > 0);
    }

    @Test
    void testInsertSelect() {
        String sql = "INSERT INTO test_db.users(id, name) SELECT order_id, user_id FROM test_db.orders";
        ParseResult result = parser.parse(sql, null, mockMetadata);

        assertNotNull(result);
        assertEquals(2, result.getGraph().getTables().size());
        assertTrue(result.getGraph().getToEdges().size() > 0);
    }

    @Test
    void testCreateTableAsSelect() {
        String sql = "CREATE TABLE test_db.new_table AS SELECT id, name FROM test_db.users";
        ParseResult result = parser.parse(sql, null, mockMetadata);

        assertNotNull(result);
        assertTrue(result.getGraph().getTables().size() >= 2);
    }

    @Test
    void testMetadataNotFound() {
        String sql = "SELECT * FROM non_existent_table";
        assertThrows(MetadataNotFoundException.class, () -> {
            parser.parse(sql, null, mockMetadata);
        });
    }

    @Test
    void testEmptySQL() {
        String sql = "";
        ParseResult result = parser.parse(sql, null, mockMetadata);
        assertNotNull(result);
    }

    @Test
    void testMultipleStatements() {
        String sql = "SELECT id FROM test_db.users; SELECT order_id FROM test_db.orders;";
        ParseResult result = parser.parse(sql, null, mockMetadata);

        assertNotNull(result);
        assertEquals(2, result.getGraph().getTables().size());
    }

    @Test
    void testSQLWithComments() {
        String sql = "-- This is a comment\nSELECT id, name FROM test_db.users /* inline comment */";
        ParseResult result = parser.parse(sql, null, mockMetadata);

        assertNotNull(result);
        assertTrue(result.getGraph().getTables().size() > 0);
    }
}
