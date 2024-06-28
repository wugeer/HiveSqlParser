package org.example;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MyProcessorTest {

    @Test
    public void testParseCreateTable() throws Exception {
        String query = "create table test.my_table(id int, name string)";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseSelectQuery() throws Exception {
        String query = "select id, name from test.my_table where id > 10";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseInsertQuery() throws Exception {
        String query = "insert into test.my_table values (1, 'John')";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseCreateTableAsSelect() throws Exception {
        String query = "create table test.new_table as select id, name from test.my_table";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.new_table"));
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }

    @Test
    public void testParseWithClause() throws Exception {
        String query = "with cte_table as (select id, name from test.my_table) select * from cte_table";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("cte_table"));
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }


    @Test
    public void testParseInsertOverwriteQuery() throws Exception {
        String query = "insert overwrite table test.my_table select id, name from test.another_table";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseSubquery() throws Exception {
        String query = "select id, name from (select id, name from test.my_table) t where t.id > 10";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals("test.my_table", tableNames.get(0));
    }

    @Test
    public void testParseJoinQuery() throws Exception {
        String query = "select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.table1"));
        Assert.assertTrue(tableNames.contains("test.table2"));
    }

    @Test
    public void testParseExistsQuery() throws Exception {
        String query = "select id, name from    test.my_table where exists (select 1 from test.another_table where another_table.id = my_table.id)";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
        Assert.assertTrue(tableNames.contains("test.another_table"));
    }

    @Test
    public void testParseInQuery() throws Exception {
        String query = "select id, name from test.my_table where id in (select id from test.filter_table)";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
        Assert.assertTrue(tableNames.contains("test.filter_table"));
    }

    @Test
    public void testParseGroupByQuery() throws Exception {
        String query = "select id, count(*) from test.my_table group by id";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }

    @Test
    public void testParseOrderByQuery() throws Exception {
        String query = "select id, name from test.my_table order by id desc";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }

    @Test
    public void testParseHavingQuery() throws Exception {
        String query = "select id, count(*) as cnt from test.my_table group by id having cnt > 5";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertTrue(tableNames.contains("test.my_table"));
    }

}

