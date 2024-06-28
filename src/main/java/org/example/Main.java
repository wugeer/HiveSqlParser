package org.example;

public class Main {
    public static void main(String[] args) throws Exception {
        String query = "create table test.my_table(id int,name string)row format delimited fields terminated by '\\t'";
        MyProcessor processor = new MyProcessor();
        processor.parse(query);
    }
}

