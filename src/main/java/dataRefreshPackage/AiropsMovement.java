package dataRefreshPackage;

import java.sql.SQLException;

public class AiropsMovement extends Table {
	
	private String tableName = "AiropsMovement";
	
	private String clientSQL = "SELECT DISTINCT\r\n" + 
	//private String clientSQL = "SELECT \r\n" +
			"    [FlightLegId] as airopsmovementId\r\n" + 
			"    ,case when [IsEmptyLeg] = 1 or [TripType] = 'Repo Leg' then null else [FlightLegId] end as atlasFlightId\r\n" + 
			//"    ,case when [Pax Count] = 0 then null else [FlightLegId] end as atlasFlightId\r\n" +
			"    ,[TripNumber] as airopsId\r\n" + 
			"    ,[LegNumber] as legNumber\r\n" +
			"    ,apf.[ID] fromAirportId\r\n" +
			"    ,apt.[ID] toAirportId\r\n" +		
			"    ,[Departure] fromAirport\r\n" + 
			"    ,[Arrival] toAirport\r\n" + 
			"    ,[ScheduledOutDateUTC] as scheduledDepartureDateTimeUTC\r\n" + 
			"    ,[ScheduledOutDateUTC] + [DepartureDate]-[DepartureDateUTC] as scheduledDepartureDateTimeLoc\r\n" +
			"    ,[ScheduledInDateUTC] as scheduledArrivalDateTimeUTC\r\n" + 
			"    ,[ScheduledInDateUTC] + [ArrivalDate]-[ArrivalDateUTC] as scheduledArrivalDateTimeLocal\r\n" +
			"    ,[ScheduledDepartureDateUTC] as scheduledTakeOffDateTimeUTC\r\n" + 
			"    ,[ScheduledDepartureDateUTC] + [DepartureDate]-[DepartureDateUTC] as scheduledTakeOffDateTimeLocal\r\n" +
			"    ,[ScheduledArrivalDateUTC] as scheduledLandingDateTimeUTC\r\n" + 
			"    ,[ScheduledArrivalDateUTC] + [ArrivalDate]-[ArrivalDateUTC] as scheduledLandingDateTimeLocal\r\n" +
			//"    ,CAST([EstimatedBlockDec] as decimal(10,1))*60 as scheduledBlockTime\r\n" +
			//"    ,CAST([EstimatedFlightDec] as decimal(10,1))*60 as scheduledFlyingTime\r\n" +
			"    ,DATEDIFF(minute,[ScheduledOutDateUTC], [ScheduledInDateUTC]) scheduledBlockTime\r\n" +
			"    ,DATEDIFF(minute,[ScheduledDepartureDateUTC], [ScheduledArrivalDateUTC]) scheduledFlyingTime\r\n" +
			"    ,[AircraftType] as aircraftType\r\n" + 
			"    ,f.[TailNumber] as aircraftRegistration\r\n" + 
			"    ,[OutBlocksUTC] as actualDepartureDateTimeUTC\r\n" + 
			"    ,[InBlocksUTC] as actualArrivalDateTimeUTC\r\n" + 
			"    ,[ActualDepartureDateUTC] as actualTakeOffDateTimeUTC\r\n" + 
			"    ,[ActualArrivalDateUTC] as actualLandingDateTimeUTC\r\n" + 
			"    ,[OutBlocksUTC] + [DepartureDate]-[DepartureDateUTC] as actualDepartureDateTimeLocal\r\n" + 
		    "    ,[InBlocksUTC]  + [ArrivalDate]-[ArrivalDateUTC] as actualArrivalDateTimeLocal\r\n" +
			"    ,[ActualDepartureDateLocal] as actualTakeOffDateTimeLocal\r\n" +
			"    ,[ActualArrivalDateLocal] as actualLandingDateTimeLocal\r\n" +
			//"    ,CAST([BlockTimeDec] as decimal(10,1))*60 as actualBlockTime\r\n" + 
			//"    ,CAST([ActualFlightDec] as decimal(10,1))*60 actualFlyingTime\r\n" + 
			"    ,DATEDIFF(minute,[OutBlocksUTC], [InBlocksUTC]) actualBlockTime\r\n" +
			"    ,DATEDIFF(minute,[ActualDepartureDateUTC], [ActualArrivalDateUTC]) actualFlyingTime\r\n" +			
			"    ,[IsEmptyLeg] as deadHead\r\n" + 
			"    ,[TripType] as legType\r\n" + 
			"    ,[Customer] as requesterName\r\n" + 
			"    ,[Pax Count] as paxCount\r\n" +			 
			"    ,[FlightLegGuidID] as FlightLegGuidID\r\n" +			
			"    ,[Created] as bookedDateTime\r\n" +
			"    ,[RowVersion] as rowversion\r\n" +
			"FROM [bi].[FlightDataAnalysis] f\r\n" + 
			"LEFT JOIN [bi].[AircraftAnalysis] a on f.[TailNumber] = a.[TailNumber]\r\n" + 
			"LEFT JOIN [dbo].[OperatorCertificate] o on a.[OperatorCertificateID] = o.[GuidID]\r\n" +
			"LEFT JOIN [dbo].[Airport] apf on f.[DepartureAirportID] = apf.[DirectoryGuidID] and apf.active = 1\r\n" +
			"LEFT JOIN [dbo].[Airport] apt on f.[ArrivalAirportID] = apt.[DirectoryGuidID] and apt.active = 1\r\n" +
			"WHERE f.[LegStatus] <> 'Cancelled' -- exclude Cancelled\r\n" + 
			"AND (o.[Name] = 'Gama Aviation LLC' or Upper(f.[TailNumber]) like 'GAMA%' or Upper(f.[TailNumber]) like '1P - BDR%')\r\n" +
			"AND [ArrivalDateUTC] >= '" + beginDate + "' \r\n" +
			"AND [DepartureDateUTC] <= '" + endDate +"'";

	private String loadInsertHeader = "INSERT INTO AiropsMovement_temp ( airopsmovementId, atlasFlightId, airopsId, legNumber,fromAirportId, toAirportId, fromAirport, toAirport,\r\n"
			+ "scheduledDepartureDateTimeUTC, scheduledDepartureDateTimeLoc, scheduledArrivalDateTimeUTC, scheduledArrivalDateTimeLocal, \r\n"
			+ "scheduledTakeOffDateTimeUTC, scheduledTakeOffDateTimeLocal, scheduledLandingDateTimeUTC, scheduledLandingDateTimeLocal, scheduledBlockTime, scheduledFlyingTime,\r\n"
			+ "aircraftType, aircraftRegistration, actualDepartureDateTimeUTC, actualArrivalDateTimeUTC, actualTakeOffDateTimeUTC, actualLandingDateTimeUTC,\r\n"
			+ "actualDepartureDateTimeLocal, actualArrivalDateTimeLocal, actualTakeOffDateTimeLocal, actualLandingDateTimeLocal, actualBlockTime, actualFlyingTime, deadHead, legType, \r\n"
			+ "requesterName, paxCount, FlightLegGuidID, bookedDateTime, rowVersion) \r\n"
			+ "VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
	
	private String deleteSQL = "DELETE FROM AiropsMovement WHERE scheduledDepartureDateTimeUTC <= timestamp('" + endDate + "') AND scheduledArrivalDateTimeUTC >= timestamp('" + beginDate + "')";	
	
	private String deleteDuplicateSQL = "DELETE FROM AiropsMovement WHERE airopsmovementId in (select airopsmovementId from AiropsMovement_temp)";
	
	private String insertSQL = "INSERT INTO AiropsMovement ( airopsmovementId, atlasFlightId, airopsId, legNumber, fromAirport, toAirport,\r\n" + 
			"    scheduledDepartureDateTimeUTC, scheduledDepartureDateTimeLoc, scheduledTakeOffDateTimeUTC, scheduledTakeOffDateTimeLocal, scheduledLandingDateTimeUTC,scheduledLandingDateTimeLocal,\r\n" + 
			"    scheduledArrivalDateTimeUTC, scheduledArrivalDateTimeLocal, scheduledBlockTime, scheduledFlyingTime, aircraftType, aircraftRegistration,\r\n" + 
			"    actualDepartureDateTimeUTC, actualDepartureDateTimeLocal, actualTakeOffDateTimeUTC, actualTakeOffDateTimeLocal,\r\n" + 
			"    actualLandingDateTimeUTC, actualLandingDateTimeLocal, actualArrivalDateTimeUTC, actualArrivalDateTimeLocal,\r\n" + 
			"    actualBlockTime, actualFlyingTime, legType,fromAirportId, toAirportId, FlightLegGuidID, rowVersion)\r\n" +
			"SELECT airopsmovementId, atlasFlightId, airopsId, legNumber, fromAirport, toAirport, \r\n" + 
			"    scheduledDepartureDateTimeUTC, scheduledDepartureDateTimeLoc, scheduledTakeOffDateTimeUTC, scheduledTakeOffDateTimeLocal, scheduledLandingDateTimeUTC, scheduledLandingDateTimeLocal, \r\n" +
			"    scheduledArrivalDateTimeUTC, scheduledArrivalDateTimeLocal, scheduledBlockTime, scheduledFlyingTime, aircraftType, aircraftRegistration,\r\n" + 
			"    actualDepartureDateTimeUTC, actualDepartureDateTimeLocal, actualTakeOffDateTimeUTC, actualTakeOffDateTimeLocal, \r\n" + 
			"    actualLandingDateTimeUTC, actualLandingDateTimeLocal, actualArrivalDateTimeUTC, actualArrivalDateTimeLocal, \r\n" + 
			//"    actualBlockTime, actualFlyingTime, legType, f.AirportId fromAirportId, t.AirportId toAirportId, a.FlightLegGuidID, rowVersion\r\n" + 
			"    actualBlockTime, actualFlyingTime, legType, fromAirportId, toAirportId, a.FlightLegGuidID, rowVersion\r\n" +
			"FROM AiropsMovement_temp a\r\n" + 
			//"LEFT JOIN Airport f on a.fromAirport= f.icaoCode\r\n" + 
			//"LEFT JOIN Airport t on a.toAirport= t.icaoCode\r\n" +
			"INNER JOIN Aircraft ac on a.aircraftRegistration = ac.tailNumber and ac.operatorId = 4";
	
	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");
		
		int rowsInsert = 0;
		rowsInsert = insertIntoTempTable(tableName, clientSQL, loadInsertHeader);

		String tempTableName = "AiropsMovement_temp";
		
		if (rowsInsert>0) {
			updateTargetTable (tableName, deleteSQL, deleteDuplicateSQL, insertSQL, tempTableName);			
		}
	}
}
