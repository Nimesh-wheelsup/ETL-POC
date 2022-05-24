package dataRefreshPackage;

import java.sql.SQLException;

public class AiropsMxEvents extends Table {
	
	private String tableName = "AiropsMxEvents";
	
	private String clientSQL ="SELECT\r\n" + 
			"    [JobCardID] as AiropsMxEventId\r\n" + 
			"    ,[OfflineStartDate] as scheduledStartDateTimeUTC\r\n" + 
			"    ,[OfflineEndDate] as scheduledEndDateTimeUTC\r\n" + 
			"    ,[ReturnToServiceDateUTC] as actualEndDateTimeUTC\r\n" + 
			"    ,[ReturnToServiceDate] as actualEndDateTimeLocal\r\n" + 
			"    ,j.[TailNumber] as aircraftRegistration\r\n" + 
			"    ,[MaintenanceType] as maintenanceType\r\n" + 
			"    ,j.[ICAO] as Airport\r\n" + 
			"    ,ap.[ID] as AirportId\r\n" +
			"    ,[JobCardName] as description\r\n" +
			"    ,SUBSTRING([Notes], 1, 255) as action\r\n" +
			"FROM [bi].[JobCardAnalysis] j \r\n" +
			"LEFT JOIN [bi].[AircraftAnalysis] a on j.[TailNumber] = a.[TailNumber] \r\n" +
			"LEFT JOIN [dbo].[OperatorCertificate] o on a.[OperatorCertificateID] = o.[GuidID] \r\n " +
			"LEFT JOIN [dbo].[Airport] ap on j.[AirportID] = ap.[DirectoryGuidID] and ap.active = 1 \r\n" +
			"WHERE ISNULL([ReturnToServiceDateUTC],[OfflineEndDate]) >= '" + beginDate + "' \r\n" +
			"AND (o.[Name] = 'Gama Aviation LLC' or Upper(j.[TailNumber]) like 'GAMA%') \r\n" +
			"AND [OfflineStartDate] <= '" + endDate +"'";

	private String loadInsertHeader = "INSERT INTO AiropsMxEvents_temp (airopsMxEventsId, scheduledStartDateTimeUTC, scheduledEndDateTimeUTC, \r\n" +
			"actualEndDateTimeUTC, actualEndDateTimeLocal, aircraftRegistration, maintenanceType, airport, airportId, description, action) \r\n"+
			 "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private String deleteSQL = "DELETE FROM AiropsMxEvents WHERE scheduledStartDateTimeUTC <= timestamp('" + endDate + "') " +
			"AND ifnull(actualEndDateTimeUTC, scheduledEndDateTimeUTC) >= timestamp('" + beginDate + "');";
	
	private String deleteDuplicateSQL = "DELETE FROM AiropsMxEvents WHERE AiropsMxEventsId in \r\n" +
												"(select AiropsMxEventsId from AiropsMxEvents_temp union \r\n" +
												"select airopscrewdutyId from airopscrewduty_temp \r\n" +
												"where eventType = 'General' and (DutyCategory like 'Aircraft Unavailable%')\r\n" +
												"and aircraftRegistration is not null)";
	
	private String insertSQL = "INSERT INTO AiropsMxEvents(airopsMxEventsId, airport, scheduledStartDateTimeUTC, scheduledStartDateTimeLocal,  \r\n" + 
			"    scheduledEndDateTimeUTC, scheduledEndDateTimeLocal, aircraftType, aircraftRegistration, maintenanceType, \r\n" + 
			"    airportId, description, action, actualStartDateTimeUTC, actualStartDateTimeLocal, actualEndDateTimeUTC, actualEndDateTimeLocal) \r\n" + 
			"SELECT airopsMxEventsId, airport, scheduledStartDateTimeUTC, scheduledStartDateTimeLocal,  \r\n" + 
			"    scheduledEndDateTimeUTC, scheduledEndDateTimeLocal, aircraftType, substring_index(aircraftRegistration,'\\0',1) aircraftRegistration, maintenanceType, \r\n" + 
			"    airportId, description, action, actualStartDateTimeUTC, actualStartDateTimeLocal, \r\n" + 
			"    actualEndDateTimeUTC, actualEndDateTimeLocal \r\n" + 
			"FROM AiropsMxEvents_temp\r\n" +
			"UNION ALL \r\n" +
			"SELECT airopscrewdutyId as airopsMxEventsId, startAirport as airport, onDutyDateTimeUTC as scheduledStartDateTimeUTC, null as scheduledStartDateTimeLocal,\r\n" +
			"		offDutyDateTimeUTC as scheduledEndDateTimeUTC, null as scheduledEndDateTimeLocal, null as aircraftType, aircraftRegistration, null as maintenanceType,\r\n" +
			"       startAirportId as airportId, concat(DutyCategory,': ', Subject) as description, null as action, null, null, null, null\r\n" +
			"FROM gama.airopscrewduty_temp \r\n" +
			"WHERE eventType = 'General' and (DutyCategory like 'Aircraft Unavailable%') -- or DutyCategory = 'AIRCRAFT NOTE')\r\n" +
			"AND aircraftRegistration is not null";
	
	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");
		
		int rowsInsert = 0;
		rowsInsert = insertIntoTempTable(tableName, clientSQL, loadInsertHeader);
		
		String tempTableName = "AiropsMxEvents_temp";
		
		if (rowsInsert>0) {
			updateTargetTable (tableName, deleteSQL, deleteDuplicateSQL, insertSQL, tempTableName);			
		}
	}
}
