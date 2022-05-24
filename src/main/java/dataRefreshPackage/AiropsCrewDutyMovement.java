package dataRefreshPackage;

import java.sql.SQLException;

public class AiropsCrewDutyMovement extends Table {
	
	private String tableName = "AiropsCrewDutyMovement";
	
	private String clientSQL = "SELECT DISTINCT \r\n" + 
	//private String clientSQL = "SELECT \r\n" +
			"    flc.[FlightLegOrgUserID] as airopscrewdutymovementId\r\n" + 
			"    ,f.[FlightLegId] as airopsmovementId\r\n" + 
			"    ,u.[ID] as crewId\r\n" + 
			"    ,[Position] as crewRank\r\n" + 
			"    ,case when [Position] = 'PIC' then 1 when [Position]='SIC' then 2 end Position\r\n" + 
			"FROM [bi].[FlightLegCrew] flc\r\n" + 
			"LEFT JOIN [bi].[FlightDataAnalysis] f on flc.[FlightLegGuidID] = f.[FlightLegGuidID]\r\n" +
			//"LEFT JOIN (select distinct [FlightLegGuidID], [FlightLegId], [LegStatus], [ArrivalDateUTC],[DepartureDateUTC] from [bi].[FlightDataAnalysis]) f on flc.[FlightLegGuidID] = f.[FlightLegGuidID]" +
			"LEFT JOIN [dbo].[OrganizationUser] u on flc.[CrewGuidID] = u.[GuidID] and u.[Active] = 1 \r\n" +
			"WHERE [LegStatus] <> 'Cancelled' -- exclude Cancelled\r\n" +
			"AND [Position] in ('PIC','SIC') \r\n" +
			"AND [ArrivalDateUTC] >= '" + beginDate + "' \r\n" +
			"AND [DepartureDateUTC] <= '" + endDate +"'\r\n" +
			"AND u.[OperatorCertificateID] = 4";

	private String loadInsertHeader = "INSERT INTO AiropsCrewDutyMovement_temp(AiropsCrewDutyMovementId, AiropsMovementId, CrewID, crewRank, position) \r\n"+
			 "VALUES(?, ?, ?, ?, ?)";
	
	private String deleteSQL = "DELETE FROM AiropsCrewDutyMovement WHERE airopsMovementId in \r\n" +
			"(select airopsMovementId from AiropsMovement where scheduledDepartureDateTimeUTC <= timestamp('" + endDate + "') AND scheduledArrivalDateTimeUTC >= timestamp('" + beginDate + "'))";	
	
	private String deleteDuplicateSQL = "DELETE FROM AiropsCrewDutyMovement WHERE AiropsMovementId in (select AiropsMovementId from AiropsMovement_temp)";
	
	/*
	private String insertSQL = "INSERT INTO AiropsCrewDutyMovement(AiropsCrewDutyMovementId, airopsCrewDutyId, AiropsMovementId, crewId, crewRank, position)\r\n" + 
			"SELECT t.AiropsCrewDutyMovementId, max(acd.airopsCrewDutyId) airopsCrewDutyId, t.AiropsMovementId, f.crewId, t.crewRank, t.position \r\n" + 
			"FROM AiropsCrewDutyMovement_temp t\r\n" + 
			"INNER JOIN AiropsMovement_temp m ON t.airopsMovementId = m.airopsMovementId\r\n" +
			"INNER JOIN AiropsCrewDuty_temp acd ON t.CrewGuidID = acd.OrganizationUserGuidId AND m.scheduledDepartureDateTimeUTC BETWEEN acd.onDutyDateTimeUTC AND acd.offDutyDateTimeUTC\r\n" +
			"INNER JOIN flightcrew f on t.CrewGuidID= f.PersonnelGuidID\r\n" +
			"GROUP BY t.AiropsCrewDutyMovementId,  t.AiropsMovementId, f.crewId, t.crewRank, t.position";

	private String insertSQL = "INSERT INTO AiropsCrewDutyMovement(AiropsCrewDutyMovementId, airopsCrewDutyId, AiropsMovementId, crewId, crewRank, position)\r\n" + 
			"SELECT t.AiropsCrewDutyMovementId, acd.airopsCrewDutyId, t.AiropsMovementId, f.crewId, t.crewRank, t.position \r\n" +
			"FROM AiropsCrewDutyMovement_temp t\r\n" +
			"INNER JOIN AiropsMovement_temp m ON t.airopsMovementId = m.airopsMovementId\r\n" +
			"INNER JOIN flightcrew f on t.CrewGuidID = f.PersonnelGuidID\r\n" +
			"INNER JOIN AiropsCrewDuty acd ON f.crewId = acd.crewId AND m.scheduledDepartureDateTimeUTC BETWEEN acd.onDutyDateTimeUTC AND acd.offDutyDateTimeUTC\r\n";
			*/

	private String insertSQL = "INSERT INTO AiropsCrewDutyMovement(airopsCrewDutyId, AiropsMovementId, crewId, crewRank, position)\r\n" + 
			"SELECT acd.airopsCrewDutyId, t.AiropsMovementId, t.crewId, t.crewRank, t.position \r\n" +
			"FROM AiropsCrewDutyMovement_temp t\r\n" +
			"INNER JOIN AiropsMovement_temp m ON t.airopsMovementId = m.airopsMovementId\r\n" +
			"INNER JOIN AiropsCrewDuty acd ON t.crewId = acd.crewId AND m.scheduledDepartureDateTimeUTC BETWEEN acd.onDutyDateTimeUTC AND acd.offDutyDateTimeUTC\r\n" +
			"UNION ALL\r\n" +
			"SELECT acd.airopsCrewDutyId, am.AiropsMovementId, acd.crewId, acd.crewRank, case when acd.crewRank = 'PIC' then 1 when acd.crewRank ='SIC' then 2 end position\r\n" +
			"FROM AiropsCrewDuty acd\r\n" +
			"INNER JOIN AiropsMovement_temp am\r\n" +
			"ON acd.aircraftRegistration = am.aircraftRegistration\r\n" +
			"AND am.scheduledDepartureDateTimeUTC BETWEEN acd.onDutyDateTimeUTC AND acd.offDutyDateTimeUTC\r\n" +
			"LEFT JOIN (SELECT DISTINCT airopsmovementId FROM AiropsCrewDutyMovement_temp) acdm ON am.airopsmovementId = acdm.airopsmovementId\r\n" +
			"WHERE acd.dutydate > curdate()\r\n" +
			"AND acdm.airopsmovementId is null";

	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");
		
		int rowsInsert = 0;
		rowsInsert = insertIntoTempTable(tableName, clientSQL, loadInsertHeader);

		String tempTableName = "AiropsCrewDutyMovement_temp";		
		
		if (rowsInsert>0) {
			updateTargetTable (tableName, deleteSQL, deleteDuplicateSQL, insertSQL, tempTableName);			
		}
	}
}
