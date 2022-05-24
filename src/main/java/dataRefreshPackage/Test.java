package dataRefreshPackage;

import java.sql.SQLException;

public class Test extends Table {
    private String tableName = "Test";
    private String clientSQL = "SELECT * FROM [dbo].Test";
    private String loadInsertHeader = "INSERT INTO Test_temp\r\n" +
            "(Id,Field1,Field2)\r\n" +
            "VALUES (?, ?, ?)";
    private String deleteSQL = "DELETE FROM Test";
    private String deleteDuplicateSQL = "DELETE FROM Test WHERE Id in (select Id from Test_temp)";
    private String insertSQL = "INSERT INTO Test ( Id, Field1, Field2)\r\n" +
            "SELECT * FROM Test_temp";

    public void Refresh() throws SQLException {
        System.out.println("\r\n........... " + tableName + " ...........\r\n");

        int rowsInsert = 0;
        rowsInsert = insertIntoTempTable(tableName, clientSQL, loadInsertHeader);

        String tempTableName = "Test_temp";

        if (rowsInsert>0) {
            updateTargetTable (tableName, deleteSQL, deleteDuplicateSQL, insertSQL, tempTableName);
        }
    }
}
