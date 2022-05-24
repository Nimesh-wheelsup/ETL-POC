package dataRefreshPackage;

import java.sql.SQLException;

public class AiropsCrewDuty extends Table {
	
	private String tableName = "AiropsCrewDuty";
	
	private String deleteSQL = "DELETE FROM AiropsCrewDuty WHERE dutyDate BETWEEN '" + beginDate + "' AND '" + endDate + "'";	
	
	//private String deleteDuplicateSQL = "DELETE FROM AiropsCrewDutyOrig WHERE airopsCrewDutyId in (select airopsCrewDutyId from AiropsCrewDuty_temp)";
	
	private String insertSQL = "INSERT INTO AiropsCrewDuty (airopsCrewDutyIdOrigId, crewId, crewCode, crewName, crewRank, aircraftRegistration, dutydate,\r\n" + 
			"    onDutyDateTimeUTC, offDutyDateTimeUTC, totalDuty)\r\n" + 
			"SELECT acd.airopsCrewDutyIdOrigId, acd.crewId, acd.crewCode, acd.crewName, acd.crewRank, acd.aircraftRegistration, acd.dutydate,\r\n" +
			"    date_add(min(acd.scheduledDepartureDateTimeUTC), interval -1 hour) onDutyDateTimeUTC,\r\n" +
			"    date_add(max(acd.scheduledArrivalDateTimeUTC), interval 30 minute) offDutyDateTimeUTC,\r\n" +
			"    round(time_to_sec(date_add(timeDiff(max(acd.scheduledArrivalDateTimeUTC), min(acd.scheduledDepartureDateTimeUTC)), interval 90 minute))/60,0) totalDuty \r\n" +
			"FROM\r\n" +
			"(\r\n" +
			"    SELECT acd.airopsCrewDutyId airopsCrewDutyIdOrigId, acd.crewId, acd.crewCode, acd.crewName, acd.crewRank, acd.aircraftRegistration, acd.onDutyDateTimeUTC, acd.offDutyDateTimeUTC, \r\n" +
			"        am.airopsmovementId, am.scheduledDepartureDateTimeUTC, am.scheduledArrivalDateTimeUTC,\r\n" +
			"        CASE\r\n" +
			"            WHEN hour(scheduledDepartureDateTimeUTC) < 9 THEN date(date_add(scheduledDepartureDateTimeUTC, interval -1 day))\r\n" +
			"            ELSE date(scheduledDepartureDateTimeUTC)\r\n" +
			"        END as dutydate\r\n" +
			"    FROM AiropsCrewDutyOrig acd\r\n" +
			"    INNER JOIN AiropsMovement am\r\n" +
			"    ON acd.aircraftRegistration = am.aircraftRegistration\r\n" +
			"    AND am.scheduledDepartureDateTimeUTC BETWEEN acd.onDutyDateTimeUTC AND acd.offDutyDateTimeUTC\r\n" +
			"    WHERE date(acd.onDutyDateTimeUTC) <= date('" + endDate + "')\r\n" +
			"    AND date(acd.offDutyDateTimeUTC) >= date('"+ beginDate + "')\r\n" +
			") acd\r\n" +
			"WHERE acd.dutydate between date('" + beginDate +"') and date('" + endDate +"')\r\n" +
			"GROUP BY acd.airopsCrewDutyIdOrigId, acd.crewId, acd.crewCode, acd.crewRank, acd.aircraftRegistration, acd.dutydate\r\n";
	
	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");

		String tempTableName = "AiropsCrewDuty_temp";
		
		updateTargetTable (tableName, deleteSQL, insertSQL, tempTableName);
	}
}
