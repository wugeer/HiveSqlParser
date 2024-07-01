package org.example;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class HiveSqlParserTest {
    @Before
    public void setUp() {
        HiveSqlParser.reinitializeContext();
    }

    @Test
    public void testParseCreateTable() throws Exception {
        String query = "create table test.my_table(id int, name string)";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseSelectQuery() throws Exception {
        String query = "select id, name from test.my_table where id > 10";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        System.out.println(tableNames);
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseInsertQuery() throws Exception {
        String query = "insert into test.my_table values (1, 'John')";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(0, tableNames.size());
//        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseCreateTableAsSelect() throws Exception {
        String query = "create table test.new_table as select id, name from test.my_table";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.new_table"));
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }

    @Test
    public void testParseWithClause() throws Exception {
        String query = "with cte_table as (select id, name from test.my_table) select * from cte_table";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }


    @Test
    public void testParseInsertOverwriteQuery() throws Exception {
        String query = "insert overwrite table test.my_table select id, name from test.another_table";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.another_table", tableNames.get(0));
    }

    @Test
    public void testParseInsertOverwriteQuery1() throws Exception {
        String query = "with temp_a as (select * from test.table_1 where id=1)insert overwrite table test.my_table select id, name from test.another_table a join temp_a b on a.id=b.id";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.another_table"));
        Assert.assertTrue(tableNames.contains("test.table_1"));
    }

    @Test
    public void testParseSubquery() throws Exception {
        String query = "select id, name from (select id, name from test.my_table) t where t.id > 10";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseJoinQuery() throws Exception {
        String query = "select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.table1"));
        Assert.assertTrue(tableNames.contains("test.table2"));
    }

    @Test
    public void testParseJoinQuery1() throws Exception {
        String query = "select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id left join (select * from test.table3 a, test.table9 b where a.id=b.id ) t3 on t2.name =t3.name";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(4, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.table1"));
        Assert.assertTrue(tableNames.contains("test.table2"));
        Assert.assertTrue(tableNames.contains("test.table3"));
        Assert.assertTrue(tableNames.contains("test.table9"));
    }


    @Test
    public void testParseJoinQuery3() throws Exception {
        String query = "with temp_a as (select * from test.table5), temp_b as (select * from test.table6), temp_c as (select * from temp_a join temp_b on temp_a.id=temp_b.id)select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id left join (select * from test.table3 a, test.table9 b where a.id=b.id ) t3 on t2.name =t3.name right join temp_c t4 on t1.id=t4.id";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(6, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.table1"));
        Assert.assertTrue(tableNames.contains("test.table2"));
        Assert.assertTrue(tableNames.contains("test.table3"));
        Assert.assertTrue(tableNames.contains("test.table9"));
        Assert.assertTrue(tableNames.contains("test.table5"));
        Assert.assertTrue(tableNames.contains("test.table6"));
    }

    @Test
    public void testParseExistsQuery() throws Exception {
        String query = "select id, name from    test.my_table where exists (select 1 from test.another_table where another_table.id = my_table.id)";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
        Assert.assertTrue(tableNames.contains("test.another_table"));
    }

    @Test
    public void testParseInQuery() throws Exception {
        String query = "select id, name from test.my_table where id in (select id from test.filter_table)";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
        Assert.assertTrue(tableNames.contains("test.filter_table"));
    }

    @Test
    public void testParseGroupByQuery() throws Exception {
        String query = "select id, count(*) from test.my_table group by id";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }

    @Test
    public void testParseOrderByQuery() throws Exception {
        String query = "select id, name from test.my_table order by id desc";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }

    @Test
    public void testParseHavingQuery() throws Exception {
        String query = "select id, count(*) as cnt from test.my_table group by id having cnt > 5";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }

    @Test
    public void testParsePartitionedTable() throws Exception {
        String query = "CREATE TABLE test.partitioned_table (id INT, name STRING) PARTITIONED BY (dt STRING)";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.partitioned_table"));
    }

    @Test
    public void testParseInsertIntoPartition() throws Exception {
        String query = "INSERT INTO TABLE test.partitioned_table PARTITION (dt='2023-05-01') SELECT id, name FROM test.source_table";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.source_table"));
    }

    @Test
    public void testParseCreateExternalTable() throws Exception {
        String query = "CREATE EXTERNAL TABLE test.external_table (id INT, name STRING) STORED AS PARQUET LOCATION '/path/to/data'";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.external_table"));
    }

    @Test
    public void testParseBucketedTable() throws Exception {
        String query = "CREATE TABLE test.bucketed_table (id INT, name STRING) CLUSTERED BY (id) INTO 4 BUCKETS";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.bucketed_table"));
    }

    @Test
    public void testParseWindowFunction() throws Exception {
        String query = "SELECT id, name, AVG(salary) OVER (PARTITION BY department ORDER BY salary) AS avg_salary FROM test.employee_table";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.employee_table"));
    }

    @Test
    public void testParseLateralViewExplode() throws Exception {
        String query = "SELECT t.id, t.name, item FROM test.table_with_array t LATERAL VIEW EXPLODE(t.items) itemTable AS item";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.table_with_array"));
    }

    @Test
    public void testParseComplexUnionQuery() throws Exception {
        String query = "SELECT id, name FROM test.table1 UNION ALL SELECT id, name FROM test.table2 UNION SELECT id, name FROM test.table3";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(3, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.table1"));
        Assert.assertTrue(tableNames.contains("test.table2"));
        Assert.assertTrue(tableNames.contains("test.table3"));
    }

    @Test
    public void testParseCreateViewQuery() throws Exception {
        String query = "CREATE VIEW test.my_view AS SELECT id, name FROM test.base_table WHERE id > 100";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.base_table"));
    }

    @Test
    public void testParseComplexJoinWithSubqueries() throws Exception {
        String query = "SELECT a.id, b.name, c.value FROM " +
                "(SELECT id FROM test.table1 WHERE id > 100) a " +
                "JOIN test.table2 b ON a.id = b.id " +
                "LEFT JOIN (SELECT id, MAX(value) as value FROM test.table3 GROUP BY id) c ON a.id = c.id";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(3, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.table1"));
        Assert.assertTrue(tableNames.contains("test.table2"));
        Assert.assertTrue(tableNames.contains("test.table3"));
    }

    @Test
    public void testParseInsertOverwriteDirectory() throws Exception {
        String query = "INSERT OVERWRITE DIRECTORY '/output/path' SELECT id, name FROM test.source_table";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.source_table"));
    }

}

