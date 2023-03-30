import javax.xml.transform.Result;
import java.lang.module.Configuration;
import java.sql.Connection;

public class SimplePut {
    private static byte[] PERSONAL_CF = Bytes.toBytes("personal");
    private static byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");

    private static byte[] NAME_COLUMN = Bytes.toBytes("name");
    private static byte[] GENDER_COLUMN = Bytes.toBytes("gender");
    private static byte[] MARITAL_STATUS_COLUMN = Bytes.toBytes("marital_Status");

    private static byte[] EMPLOYED_COLUMN = Bytes.toBytes("employed");
    private static byte[] FIELD_COLUMN = Bytes.toBytes("field");

    public static void main(String[] args) {
        Configuration conf = HBaseConifguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("census"));
            Put put1 = new Put(Bytes.toBytes("!"));
            put1.addColumn(PROFESSIONAL_CF, NAME_COLUMN, Bytes.toBytes("Mike Jones"));
            Get get = new Get(Bytes.toBytes("1"));
            Result result = table.get(get);
        }finally {
            connection.close();
            if(table != null){
                table.close();
            }
        }
    }

 }
