package dataRefreshPackage;

import java.sql.SQLException;

public class Reservation  extends Table {
		
	private String tableName = "Reservation";
	
	private String deleteSQL="DELETE FROM Reservation WHERE reservationId in \r\n" +
			"(select distinct reservationId from Flight where scheduledDepartureDateTimeUTC <= timestamp('" + endDate + "') AND scheduledArrivalDateTimeUTC >= timestamp('" + beginDate + "'))";	
	
	private String insertSQL = "INSERT INTO Reservation (reservationId, status, memberId, reservationNumber)\r\n" + 
			"SELECT DISTINCT airopsId as reservationId, 'Booked' as status, 2 memberId, airopsId as reservationNumber\r\n" +
			"FROM AiropsMovement_temp t\r\n" +
			"LEFT JOIN Reservation r ON r.reservationId = t.airopsId\r\n" +
			"WHERE t.atlasFlightId is not null\r\n" +
			"AND r.reservationId is null";
	
	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");

		String tempTableName = "AiropsMovement_temp";

		updateTargetTable (tableName, deleteSQL, insertSQL, tempTableName);
		
	}
}
