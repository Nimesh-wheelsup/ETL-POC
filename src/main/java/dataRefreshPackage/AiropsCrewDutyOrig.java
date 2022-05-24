package dataRefreshPackage;

import java.sql.SQLException;

public class AiropsCrewDutyOrig extends Table {
	
	private String tableName = "AiropsCrewDuty";
	
	private String clientSQL = "SELECT\r\n" + 
			"    pe.[ID] as airopsCrewDutyId\r\n" + 
			"    ,u.[ID] as crewId\r\n" + 
			"    ,p.[crewCode]\r\n" + 
			"    ,concat(p.[FirstName],' ', p.[LastName]) as crewName\r\n" +
			"    ,pe.[TailNumber] as aircraftRegistration\r\n" +
			"    ,pe.[StartAirport]\r\n" + 
			"    ,pe.[EndAirport]\r\n" + 
			"    ,pe.[StartDate] as onDutyDateTimeUTC\r\n" + 
			//"    ,pe.[EndDate] as offDutyDateTimeUTC\r\n" +
			"    ,case when datepart(HOUR,EndDate) = 5 and datepart(MINUTE,EndDate) = 59 then DATEADD(Hour, -6, EndDate) \r\n" +
			"          when datepart(HOUR,EndDate) = 6 and datepart(MINUTE,EndDate) = 59 then DATEADD(Hour, -7, EndDate) else EndDate end as offDutyDateTimeUTC\r\n" +
			"    ,pe.[EventType]\r\n" + 
			"    ,pe.[DutyCategory]\r\n" +
			"    ,pe.[Container]\r\n" +
			"    ,pe.[Subject]\r\n" +
			"    ,pe.[OrganizationUserGuidId]\r\n" + 
			"    ,aps.[ID] as startAirportID\r\n" + 
			"    ,ape.[ID] as endAirportID\r\n" + 
			"FROM [bi].[PersonnelEventAnalysis] pe\r\n" +
			"LEFT JOIN [bi].[Personnel] p on pe.[OrganizationUserGuidID] = p.[PersonnelGuidID]\r\n" +
			"LEFT JOIN [dbo].[OrganizationUser] u on pe.[OrganizationUserGuidID] = u.[GuidID] and u.[Active] = 1\r\n" +
			"LEFT JOIN [dbo].[Airport] aps on pe.[StartAirportID] = aps.[DirectoryGuidID] and aps.[Active]=1\r\n" +
			"LEFT JOIN [dbo].[Airport] ape on pe.[EndAirportID] = ape.[DirectoryGuidID] and ape.[Active]=1\r\n" +
			"WHERE [EndDate] >= '" + beginDate + "' \r\n" +
			"AND [StartDate] <= '" + endDate + "'\r\n" +
			"AND u.[OperatorCertificateID] = 4 \r\n" +
			"--AND [EventType] in ('Duty','General')\r\n" +
			"--AND [DutyCategory] not in ('Office Duty','Standby/On Call Duty')";
	
	private String loadInsertHeader = "INSERT INTO AiropsCrewDuty_temp (airopsCrewDutyId, crewId, crewCode, crewName, aircraftRegistration, \r\n" +
			 "startAirport, endAirport, onDutyDateTimeUTC, offDutyDateTimeUTC, EventType, DutyCategory, Container, Subject, OrganizationUserGuidId, startAirportId, endAirportId) \r\n"+
			 "VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private String deleteSQL = "DELETE FROM AiropsCrewDutyOrig WHERE onDutyDateTimeUTC <= timestamp('" + endDate + "') AND offDutyDateTimeUTC >= timestamp('" + beginDate + "')";	
	
	private String deleteDuplicateSQL = "DELETE FROM AiropsCrewDutyOrig WHERE airopsCrewDutyId in (select airopsCrewDutyId from AiropsCrewDuty_temp)";
	
	private String insertSQL = "INSERT INTO AiropsCrewDutyOrig (airopsCrewDutyId, crewId, crewCode, crewName, crewRank, aircraftRegistration, \r\n" + 
			"    startAirport, onDutyDateTimeUTC, endAirport, offDutyDateTimeUTC, \r\n" + 
			"    startAirportId, endAirportId)\r\n" + 
			"SELECT t.airopsCrewDutyId, f.crewId, f.crewCode, t.crewName, \r\n" +
			"    f.crewRank as crewRank, \r\n" +
			"    t.aircraftRegistration, t.startAirport, t.onDutyDateTimeUTC, t.endAirport, t.offDutyDateTimeUTC, \r\n" +
			"    t.startAirportId, t.endAirportId \r\n" +
			"FROM AiropsCrewDuty_temp t \r\n" +
			"LEFT JOIN flightcrew f on t.OrganizationUserGuidId= f.PersonnelGuidID\r\n" +
			"WHERE t.EventType = 'Duty'\r\n" +
			"AND t.container not in ('Hard Day Off','Training') \r\n" +
			"AND t.aircraftRegistration is not null \r\n" +
			"-- AND f.active = 1 ";
	
	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");
		
		int rowsInsert = 0;
		rowsInsert = insertIntoTempTable(tableName, clientSQL, loadInsertHeader);
		
		String tempTableName = "AiropsCrewDuty_temp";
		
		if (rowsInsert>0) {
			updateTargetTable (tableName, deleteSQL, deleteDuplicateSQL, insertSQL, tempTableName);			
		}
	}
}
