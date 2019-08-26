

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/*
 * Eclipse run this program to load data, mystore deployed at localhost:20000
 */
public class LoadData {
    public static void main(String[] args) {
        final KVStoreConfig config =
            new KVStoreConfig("mystore", "localhost:20000");
        final KVStore store = KVStoreFactory.getStore(config);
        StatementResult sr = store.executeSync(
            "create table mytable (id integer, tsMillis long, " +
            "logLevel string, hostId integer, message string, " +
            "primary key(id))");
        System.out.println(sr.getInfo());
        final TableAPI tableAPI = store.getTableAPI();
        Table table = tableAPI.getTable("mytable");
        for (int i = 0; i < 1000; i++) {
            final Row row = table.createRow();
            row.put("id", i);
            row.put("tsMillis", System.currentTimeMillis() + i);
            if (i % 2 == 0) {
                row.put("hostId", 1);
            } else {
                row.put("hostId", 2);
            }
            if (i % 4 == 0) {
                row.put("logLevel", "SEVERE");
                row.put("message", "error occur");
            } else {
                row.put("logLevel", "INFO");
                row.put("message", "normal");
            }
            tableAPI.put(row, null, null);
        }
        store.executeSync(
            "create table hosttable " +
            "(id integer, name string, primary key(id))");
        table = tableAPI.getTable("hosttable");
        Row row = table.createRow();
        row.put("id", 1);
        row.put("name", "hostA");
        tableAPI.put(row, null, null);
        row = table.createRow();
        row.put("id", 2);
        row.put("name", "hostB");
        tableAPI.put(row, null, null);
        store.close();
    }
}
