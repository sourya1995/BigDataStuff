import java.io.IOException;
import java.lang.module.Configuration;
import java.sql.Connection;
import java.sql.SQLException;

public class CreateTable {
    public static void main(String[] args) throws IOException, SQLException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        try{
            Admin admin = connection.getAdmin();
            HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("census"));
            tableName.addFamily(new HColumnDescriptor("personal"));
            tableName.addFamily(new HColumnDescriptor("professional"));

            if(!admin.tableExists(tableName.getTableName())) {
                System.out.print("Creating the census table. ");
                admin.createTable(tableName);
                System.out.println("Done.");
            } else {
                System.out.println("Table already exists");
            }
        } finally {
            connection.close();

        }
    }
}
