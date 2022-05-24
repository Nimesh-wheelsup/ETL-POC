package dataRefreshPackage;

import java.sql.SQLException;

public class Crewdutydate extends Table {
	
	private String tableName = "crewdutydate";
	/*
	private String clientSQL = "SELECT\r\n" +
			"    ascii(substring(p.CrewCode, 1,1))*1000000+ascii(substring(p.[CrewCode], 2,2))*10000+ascii(substring(p.[CrewCode], 3,3))*100+ascii(substring(p.[CrewCode], 4,4)) crewId,\r\n" +
			"    p.[CrewCode],\r\n" +
			"    ca.[StartDate],\r\n" +
			"    ca.[EndDate],\r\n" +
			"    ca.[AvailabilityType],\r\n" +
			"    ca.[AvailabilityID]\r\n" +
			"FROM [bi].[CrewAvailabilityAnalysis] ca\r\n" +
			"INNER JOIN [bi].[Personnel] p on ca.[OrganizationUserID] = p.[PersonnelGuidID]\r\n" + 
			"WHERE ca.[EndDate] >= '" + beginDate + "' \r\n" + 
			"AND ca.[StartDate] <= '"+ endDate + "'";
			*/
	//private String loadInsertHeader = "INSERT INTO crewdutydate_temp (crewId, crewCode, actBeginDate, actEndDate, activityType, AvailabilityID) \r\n"+
	//		 "VALUES( ?, ?, ?, ?, ?, ?)";
	
	private String deleteSQL = "DELETE FROM crewdutydate WHERE date_format(dutydate,'%Y-%m-%d') BETWEEN '" + beginDate + "' AND '" + endDate + "'";	
	
	//private String deleteDuplicateSQL = "DELETE FROM AiropsCrewDutyMovement WHERE AiropsMovementId in (select AiropsMovementId from AiropsMovement_temp)";
	
	/*
	private String insertSQL = "INSERT INTO crewdutydate(crewId, crewCode, aircrafttypeid, dutydate, position, AvailabilityID, updateTime)\r\n" +
			"SELECT fc.crewId, fc.crewCode, fc.aircraftTypeId, date_format(date(d.dutydate), '%Y/%m/%d %H:%i'), fc.crewRank, max(t.AvailabilityID) AvailabilityID, sysdate() updateTime\r\n" + 
			"FROM flightcrew fc\r\n" +
			"INNER JOIN crewdutydate_temp t ON fc.crewID = t.crewID \r\n" +
			"INNER JOIN \r\n" +
			"(\r\n" +
			"	SELECT adddate(date('" + beginDate + "'), numberId) dutydate\r\n" +
			"    FROM NumberList\r\n" +
			"    WHERE adddate(date('" + beginDate + "'), numberId) <= date('" + endDate + "')\r\n" +
			") d ON date(t.actBeginDate) <= d.dutydate and t.actEndDate > d.dutydate\r\n" +
			"WHERE fc.active = 1 \r\n" +
			"GROUP BY fc.crewId, d.dutydate";
			*/
	/*
	private String insertSQL = "INSERT INTO crewdutydate(crewId, crewCode, aircrafttypeid, dutydate, position, updateTime)\r\n" +
			"SELECT DISTINCT cd.crewId, cd.crewCode, fc.aircraftTypeId, date_format(date(d.dutydate), '%Y/%m/%d %H:%i') as dutydate, fc.crewRank, sysdate() updateTime\r\n" +
			"FROM airopscrewduty cd \r\n" +
			"INNER JOIN \r\n" +
			"(\r\n" +
			"	SELECT adddate(date('" + beginDate + "'), numberId) dutydate\r\n" +
			"	FROM NumberList\r\n" +
			"	WHERE adddate(date('" + beginDate + "'), numberId) <= date('" + endDate + "')\r\n" +
			") d ON date(cd.onDutyDateTimeUTC) <= d.dutydate and cd.offDutyDateTimeUTC > d.dutydate\r\n" +
			"INNER JOIN flightcrew fc ON cd.crewID = fc.crewID \r\n" +
			"WHERE fc.active = 1";
			*/

	private String insertSQL = "INSERT INTO crewdutydate(crewId, crewCode, aircrafttypeid, dutydate, position, updateTime)\r\n" +
			"SELECT DISTINCT fc.crewId, fc.crewCode, fc.aircraftTypeId, date_format(date(d.dutydate), '%Y/%m/%d %H:%i') dutydate, fc.crewRank, sysdate() updateTime\r\n" +
			"FROM flightcrew fc\r\n" +
			"CROSS JOIN\r\n" +
			"(\r\n" +
			"    SELECT adddate(date('" + beginDate + "'), numberId) dutydate\r\n" +
			"    FROM NumberList\r\n" +
			"    WHERE adddate(date('" + beginDate + "'), numberId) <= date('" + endDate + "')\r\n" +
			") d\r\n" +
			"LEFT JOIN\r\n" +
			"(\r\n" +
			"    -- not available\r\n" +
			"    SELECT crewId, onDutyDateTimeUTC, offDutyDateTimeUTC, 1 as notAvail\r\n" +
			"    FROM AiropsCrewDuty_temp\r\n" +
			"    WHERE (EventType = 'Hard Day Off (Unavailable)' or DutyCategory like '%Training%' or container in ('Hard Day Off','Training'))\r\n" +
			"    AND (dutyCategory not like 'Aircraft Unavailable%' or DutyCategory <> 'AIRCRAFT NOTE') -- for AC \r\n" +
			") t ON fc.crewId = t.crewId AND date(t.onDutyDateTimeUTC) <= d.dutydate and t.offDutyDateTimeUTC > d.dutydate\r\n" +
			"WHERE fc.active = 1\r\n" +
			"AND fc.aircraftTypeId is not null\r\n" +
			"AND fc.crewRank is not null\r\n" +
			"AND t.crewId is null";	

	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");
		
		//int rowsInsert = 0;
		//rowsInsert = insertIntoTempTable(tableName, clientSQL, loadInsertHeader);		
		//if (rowsInsert>0) {
		//	updateTargetTable (tableName, deleteSQL, insertSQL);			
		//}
		
		//insertIntoTempTable(tableName, clientSQL, loadInsertHeader);
		
		//String tempTableName = "crewdutydate_temp";
		String tempTableName = "AiropsCrewDuty_temp";
		
		updateTargetTable (tableName, deleteSQL, insertSQL, tempTableName);
		
	}
}
