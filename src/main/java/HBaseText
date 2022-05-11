import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.commons.compress.utils.Lists.newArrayList;

public class HBaseConnection implements Logger{

//    private static Connection connection;

    private static Connection MyHBaseConnection(String zkServer, String port) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkServer);
        config.set("hbase.zookeeper.property.clientPort", port);

        Connection connection = ConnectionFactory.createConnection(config);
//        System.out.println(connection);

        return connection;
    }

    private static void CreateTable(TableName tableName, String columnName1, String columnName2, HBaseAdmin admin) throws IOException {
//        TableName tableName = TableName.valueOf(table_name);
        TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnName1)).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnName2)).build());
//        ColumnFamilyDescriptor family1 =  ColumnFamilyDescriptorBuilder.newBuilder(columnName1.getBytes(StandardCharsets.UTF_8)).build();
//        ColumnFamilyDescriptor family2 =  ColumnFamilyDescriptorBuilder.newBuilder(columnName2.getBytes(StandardCharsets.UTF_8)).build();

//        ColumnFamilyDescriptor family1 = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(Bytes.toBytes(columnName1));
        admin.createTable(tableDescriptor.build());
        logger.info(String.format("create table ：[%s] finished", tableName));
    }

    public static void MyPut(Connection connection, TableName tableName, String rowKey, String columnFamily, String column, String data){
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void AllPut(Connection connection, TableName tableName, List<String> rowKeyList, String columnFamily, String column, List<String> dataList){
        Table table = null;
        List<Put> putList = newArrayList();
        try {
            table = connection.getTable(tableName);
            for (int index =0; index <rowKeyList.size(); index++){
                Put put = new Put(Bytes.toBytes(rowKeyList.get(index)));
                logger.info(String.format("Add RowKey: [%s]", rowKeyList.get(index)));
                put.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(dataList.get(index)));
                logger.info(String.format("Add ColumnFamily: [%s] and Column：[%s], value is [%s]", columnFamily, column, dataList.get(index)));
                putList.add(put);
            }
            table.put(putList);
            logger.info("Add Finished");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static boolean namespaceExists(final HBaseAdmin admin, final String namespaceName)
            throws IOException {
        try {
            admin.getNamespaceDescriptor(namespaceName);
        } catch (NamespaceNotFoundException e) {
            return false;
        }
        return true;
    }


    public static void main(String[] args) throws IOException {
//        String zkServer = "10.10.10.177";
        String zkServer = "emr-worker-2,emr-worker-1,emr-header-1";
        String port = "2181";
        TableName tableName = TableName.valueOf("wangguochao:student");
        String column1 = "info";
        String column2 = "score";
        String MY_NAMESPACE_NAME = "wangguochao";

        List<String> columnFamilies = Arrays.asList("info", "score");

        Connection connection = MyHBaseConnection(zkServer, port);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        System.out.println(admin);
//        CreateTable(tableName, column1, column2, admin);
        if(!namespaceExists(admin, MY_NAMESPACE_NAME)){

            logger.info(String.format("Creating Namespace ['%s']", MY_NAMESPACE_NAME));
            admin.createNamespace(NamespaceDescriptor.create(MY_NAMESPACE_NAME).build());
        }


        if (!admin.tableExists(tableName)) {

            CreateTable(tableName, column1, column2, admin);
        } else {
            logger.info("添加数据");

            // 添加数据
            // (Connection connection, TableName tableName, String rowKey, String columnFamily, String column, String data)
            MyPut(connection, tableName,"Tom", column1, "studen_id", "20210000000001");
            MyPut(connection, tableName,"Tom", column1, "class", "1");
            MyPut(connection, tableName,"Tom", column2, "understanding", "75");
            MyPut(connection, tableName,"Tom", column2, "programing", "82");

//            List<String> rowKeyList = Arrays.asList("Tom", "Kary", "Ford");
//            List<String> ageList = Arrays.asList("Tom", "Kary", "Ford");

            List<String> rowKeyList = Arrays.asList("Tom", "Jerry", "Jack", "Rose", "WangGuoChao");
            List<String> idList = Arrays.asList("20210000000001", "20210000000002", "20210000000003", "20210000000004", "G20220735030060");
            List<String> classList = Arrays.asList("1", "1", "2", "2", "5");
            List<String> underList = Arrays.asList("75", "85", "80", "60", "90");
            List<String> programList = Arrays.asList("82", "67", "80", "61", "80");

            AllPut(connection, tableName, rowKeyList, column1, "info", idList);
            AllPut(connection, tableName, rowKeyList, column1, "class", classList);
            AllPut(connection, tableName, rowKeyList, column2, "understanding", underList);
            AllPut(connection, tableName, rowKeyList, column2, "programing", programList);
        }
    }


}
