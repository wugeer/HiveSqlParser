package org.example;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
//        String query = "create table test.my_table(id int,name string)row format delimited fields terminated by '\\t'";
//        String query = "select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id";

//        String query = "create table test.new_table as select id, name from test.my_table";
//        String query = "with cte_table as (select id, name from test.my_table) select * from cte_table";
//        String query = "insert overwrite table test.my_table select id, name from test.another_table";
//        String query = "with temp_a as (select * from test.table5), temp_b as (select * from test.table6), temp_c as (select * from temp_a join temp_b on temp_a.id=temp_b.id)select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id left join (select * from test.table3 a, test.table9 b where a.id=b.id ) t3 on t2.name =t3.name right join temp_c t4 on t1.id=t4.id";
//        String query = "SELECT t.id, t.name, item FROM test.table_with_array t LATERAL VIEW EXPLODE(t.items) itemTable AS item";
//        String query = "use mydb; select * from my_table; select * from test.another_table";
        String query = "select id, name from my_table where id > 10; use test_db; select * from another_table; use test_2; select * from another_table";
        HiveSqlParser processor = new HiveSqlParser();
        processor.parse(query);
        List<String> tableNames = processor.getTableNames();
        System.out.println(tableNames);
    }
}

