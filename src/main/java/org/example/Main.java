package org.example;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
//        String query = "create table test.my_table(id int,name string)row format delimited fields terminated by '\\t'";
        String query = "select t1.id, t2.name from test.table1 t1 join test.table2 t2 on t1.id = t2.id";

        MyProcessor processor = new MyProcessor();
        processor.parse(query);
    }
}

