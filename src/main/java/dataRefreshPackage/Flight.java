package dataRefreshPackage;

import java.sql.SQLException;

public class Flight extends Table {
		
	private String tableName = "Flight";
	
	private String deleteSQL="DELETE FROM Flight WHERE scheduledDepartureDateTimeUTC <= timestamp('" + endDate + "') AND scheduledArrivalDateTimeUTC >= timestamp('" + beginDate + "')";	
	
	private String deleteDuplicateSQL = "DELETE FROM Flight WHERE flightId in (select atlasFlightId from AiropsMovement_temp where atlasFlightId is not null)";
	
	private String insertSQL = "INSERT INTO Flight (flightId, flightdate, leg, status, tailnumber, fromAirportId, toAirportId, requestedAircraftId, legType, \r\n" + 
			"    scheduledArrivalDateTimeUTC, scheduledDepartureDateTimeUTC, reservationId, estimatedFlightHours, estimatedDepartureTime, estimatedArrivalTime, \r\n" +
			"    flightNumber, requesterName, operatorId, createDate, rowVersion) \r\n" + 
			"SELECT a.atlasflightId as flightId, date(a.scheduledDepartureDateTimeLoc) flightDate, a.legNumber leg, 'booked' as status, \r\n" + 
			"       case when a.aircraftRegistration in ('KING','LGHT','MIDSIZE','SMID','SUPERMID','LARGE') then null else a.aircraftRegistration end tailnumber, \r\n" +
			//"       f.AirportId fromAirportId, t.AirportId toAirportId, \r\n" +
			"       fromAirportId, toAirportId, \r\n" +
			"       CASE\r\n" + 
			"            WHEN a.AircraftType like ('King Air%') or upper(a.aircraftRegistration) like '%KING AIR%' or at.sortorder = 4 \r\n" +
			"                   or upper(a.aircraftRegistration) like '%TURBOPROP%' or upper(a.aircraftRegistration) like '%KING AIR%' then 9973 -- KingAir / KA\r\n" + 
			"            WHEN a.AircraftType in ('Citation Encore', 'Hawker 400A' ) \r\n" +
			"                 or upper(a.aircraftRegistration) like '%LIGHT%' then 7 -- Light/CE \r\n" + 
			"            WHEN a.AircraftType in ('Citation Excel','Citation Sovereign','Gulfstream 150') or at.sortorder = 7  \r\n" + 
			"                 or upper(a.aircraftRegistration) like '%(MID%' or upper(a.aircraftRegistration) in ('CE-560-XL','MIDSIZE') then 28 -- Midsize / XL \r\n" + 
			"            WHEN a.AircraftType in ('Citation X') or at.sortorder = 15 \r\n" + 
			"                 or upper(a.aircraftRegistration) like '%SUPER MID%' or upper(a.aircraftRegistration) in ('CE-750') or at.sortorder = 9 then 33 -- SuperMid / CX \r\n" + 
			"            WHEN a.AircraftType in ('Challenger 601','Gulfstream IV SP') \r\n" + 
			"                 or upper(a.aircraftRegistration) like '%LARGE%' then 9624  -- Large \r\n" + 
			"            ELSE null \r\n" +
			"       END as requestedAircraftId, \r\n" + 
			"       a.legType, a.scheduledArrivalDateTimeUTC, a.scheduledDepartureDateTimeUTC,\r\n" + 
			"       a.airopsId reservationId,\r\n" + 
			"       (a.scheduledFlyingTime)/60 as estimatedFlightHours,\r\n" + 
			"       scheduledDepartureDateTimeLoc as estimatedDepartureTime, scheduledArrivalDateTimeLocal as estimatedArrivalTime, \r\n" + 
			"       a.airopsId as flightNumber, a.requesterName, \r\n" +
			"       ac.operatorId, \r\n" +
			"       a.bookedDateTime createDate, rowVersion \r\n" +
			"FROM AiropsMovement_temp a \r\n" + 
//			"LEFT JOIN Airport f on a.fromAirport = f.icaoCode\r\n" + 
//			"LEFT JOIN Airport t on a.toAirport = t.icaoCode\r\n" +
			"LEFT JOIN Aircraft ac on a.aircraftRegistration = ac.tailNumber\r\n" +
			"LEFT JOIN AircraftType at on ac.aircraftTypeId = at.aircraftTypeId\r\n" +
			"WHERE a.atlasFlightId is not null\r\n";
	
	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");
		
		String tempTableName = "AiropsMovement_temp";
		
		updateTargetTable (tableName, deleteSQL, deleteDuplicateSQL, insertSQL, tempTableName);
		
	}

}
