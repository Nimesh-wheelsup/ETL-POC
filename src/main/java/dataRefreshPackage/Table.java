package dataRefreshPackage;

import java.io.PrintStream;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;

public class Table {

    private Connection cConnection = DataRefresh.clientConnection;
    private Connection wConnection = DataRefresh.wuConnection;
    private PrintStream out = DataRefresh.out;

    public String beginDate = DataRefresh.beginDate;
    public String endDate = DataRefresh.endDate;

    public int insertIntoTempTable(String tableName, String clientSQL, String loadInsertHeader) throws SQLException
    {
        int rowsPrep = 0;
        /* Query client side */
        try {
            Instant start = Instant.now();
            Statement stmt = cConnection.createStatement();

            System.out.println("\r\nClientSQL:\r\n" + clientSQL + ";");

            ResultSet clientResultSet = stmt.executeQuery(clientSQL);
            int columnCount = clientResultSet.getMetaData().getColumnCount();
            Instant end = Instant.now();
            double elaspedTime = Duration.between(start, end).toSeconds();

            //Truncate temp tables
            wConnection.createStatement().executeUpdate("Truncate "+ tableName +"_temp");

            /* Insert to local */
            PreparedStatement preparedStmt = wConnection.prepareStatement(loadInsertHeader);
            while (clientResultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    preparedStmt.setString(i, clientResultSet.getString(i));
                }
                preparedStmt.addBatch();
                rowsPrep++;
            }
            preparedStmt.executeBatch();
            System.out.println("(ElaspedTime "+ elaspedTime + " seconds, " + rowsPrep + " rows prepared)\r\n");
        }
        catch (SQLException e) {
            e.printStackTrace(out);
        }
        return rowsPrep;
    }

    public void updateTargetTable (String tableName, String deleteSQL, String deleteDuplicateSQL, String insertSQL, String tempTableName) throws SQLException
    {
        try {

            Statement stmt = wConnection.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from "+ tempTableName + ";");
            int rowT = 0;
            if(rs.next()) {
                rowT = rs.getInt(1);
            }

            if(rowT>0) {
                int rowsD= updateData(deleteSQL);
                int rowsDD = 0;
                if( deleteDuplicateSQL != "") {
                    rowsDD = updateData(deleteDuplicateSQL);
                }
                int rowsI = updateData(insertSQL);
                System.out.println("\r\nDeleteSQL: \r\n" + deleteSQL + ";\r\n(" + rowsD + " rows impacted)\r\n" +
                        "\r\nDeleteDuplicateSQL: \r\n" + deleteDuplicateSQL + ";\r\n(" + rowsDD + " rows impacted)\r\n" +
                        "\r\nInsertSQL: \r\n" + insertSQL + ";\r\n(" + rowsI + " rows impacted)\r\n" );
            }
        }
        catch (SQLException e) {
            e.printStackTrace(out);

        }
    }

    public void updateTargetTable (String tableName, String deleteSQL, String insertSQL, String tempTableName) throws SQLException
    {
        try {
            Statement stmt = wConnection.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from "+ tempTableName);
            int rowT = 0;
            if(rs.next()) {
                rowT = rs.getInt(1);
            }

            if(rowT>0) {
                int rowsD = updateData(deleteSQL);
                int rowsI = updateData(insertSQL);
                System.out.println("\r\nDeleteSQL: \r\n" + deleteSQL + ";\r\n(" + rowsD + " rows impacted)\r\n" +
                        "\r\nInsertSQL: \r\n" + insertSQL + ";\r\n(" + rowsI + " rows impacted)\r\n" );
            }
        }
        catch (SQLException e) {
            e.printStackTrace(out);
        }
    }

    public int updateData(String updateSQL) throws SQLException
    {
        int updateRows = 0;
        try {
            wConnection.createStatement().executeUpdate("SET FOREIGN_KEY_CHECKS=0;");
            updateRows = wConnection.createStatement().executeUpdate(updateSQL);
        }
        catch (SQLException e) {
            e.printStackTrace(out);
        }
        return updateRows;
    }
}
