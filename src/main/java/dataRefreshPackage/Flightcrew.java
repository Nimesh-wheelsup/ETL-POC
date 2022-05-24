package dataRefreshPackage;

import java.sql.SQLException;

public class Flightcrew extends Table {
	
	private String tableName = "flightcrew";
	
	private String clientSQL = "select \r\n"+
			"    p.OrganizationUserID,\r\n"+
			"    p.CrewCode,\r\n"+
			"    p.baseAirportId,\r\n"+
			"    p.ICAO,\r\n"+
			"    p.[AircraftTypeID],\r\n"+
			"    at.[ShortName] AircraftTypeName,\r\n"+
			"    p.[TypeRatingId],\r\n"+
			"    tr.[Rating] as TypeRatingName,\r\n"+
			"    p.[CrewPositionTypeID],\r\n"+
			"    pt.name CrewPosition,	\r\n"+
			"    p.[Active],\r\n"+
			"    p.[OperatorCertificateID],\r\n"+
			"    op.[Name] OperatorCertificateName,\r\n"+
			"    p.[PersonnelGuidID],\r\n"+
			"    p.[FirstName],\r\n"+
			"    p.[MiddleName],\r\n"+
			"    p.[LastName]\r\n"+
			"FROM\r\n"+
			"(\r\n"+
			"    select \r\n"+
			"        u.[ID] as OrganizationUserID,\r\n"+
			"        p.[CrewCode],\r\n"+
			"        ap.[ID] as baseAirportId,\r\n"+
			"        ap.ICAO,\r\n"+
			"        a.[AircraftTypeID],\r\n"+
			"        a.[TypeRatingId],\r\n"+
			"        p.[Active],\r\n"+
			"        u.[OperatorCertificateID],\r\n"+
			"        p.[PersonnelGuidID],\r\n"+
			"        p.[FirstName],\r\n"+
			"        p.[MiddleName],\r\n"+
			"        p.[LastName],\r\n"+
			"        min (po.[CrewPositionTypeID]) as CrewPositionTypeID\r\n"+
			"    FROM [bi].[Personnel] p\r\n"+
			"    LEFT JOIN [dbo].[OrganizationUser] u on p.[PersonnelGuidID] = u.[GuidID] -- u.active = 1 (?)\r\n"+
			"    LEFT JOIN [dbo].[OrganizationUserDutyAssignment] a on u.[ID] = a.[OrganizationUserID]\r\n"+
			"    LEFT JOIN [dbo].[OrganizationUserDutyAssignmentPosition] po on a.[ID] = po.[OrganizationUserDutyAssignmentID] --and po.[CrewPositionTypeID] in (1,2)\r\n"+
			"    LEFT JOIN [dbo].[Airport] ap on p.[AirportID] = ap.[DirectoryGuidID] and ap.active = 1\r\n"+
			"    WHERE u.[OperatorCertificateID] = 4 -- (1,2,4) = (MA. WUPA, WU)\r\n"+
			"    GROUP BY \r\n"+
			"        u.[ID],\r\n"+
			"        p.[CrewCode],\r\n"+
			"        ap.[ID],\r\n"+
			"        ap.ICAO,\r\n"+
			"        a.[AircraftTypeID],\r\n"+
			"        a.[TypeRatingId],\r\n"+
			"        p.[Active],\r\n"+
			"        u.[OperatorCertificateID],\r\n"+
			"        p.[PersonnelGuidID],\r\n"+
			"        p.[FirstName],\r\n"+
			"        p.[MiddleName],\r\n"+
			"        p.[LastName]\r\n"+
			") p\r\n"+
			"LEFT JOIN [dbo].[AircraftType] at on p.AircraftTypeID = at.ID\r\n"+
			"LEFT JOIN [dbo].[TypeRating] tr on p.TypeRatingID = tr.ID\r\n"+
			"LEFT JOIN [dbo].[CrewPositionType] pt on p.CrewPositionTypeID = pt.ID\r\n"+
			"LEFT JOIN [dbo].[OperatorCertificate] op on p.[OperatorCertificateID] = op.ID\r\n"+
			"order by p.OrganizationUserID";

	private String loadInsertHeader = "INSERT INTO flightcrew_temp (OrganizationUserID, crewCode, baseAirportId, ICAO, AircraftTypeId, AircraftTypeName, \r\n"
			+ "TypeRatingId, TypeRatingName, CrewPositionId, CrewPosition, active, OperatorCertificateId, OperatorCertificateName, \r\n"
			+ "PersonnelGuidID, FirstName, MiddleName, LastName)"
			+ "VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private String deleteSQL = "DELETE FROM flightcrew";	
	
	//private String deleteDuplicateSQL = "";
	
	private String insertSQL = "insert into gama.flightcrew (crewId, crewCode, baseAirportId, aircraftTypeId, crewRank, qualification, active, \r\n" +
			"PersonnelGuidID, FirstName, MiddleName, LastName)\r\n" +
			"select OrganizationUserID as crewId, crewCode, baseAirportId, \r\n"+
			"    case\r\n"+
			"        when aircraftTypeId = 10091 and ACTypeCount = 2 then 9973 \r\n"+
			"        when aircraftTypeId is null and typeRatingName = 'CE-560XL' then 28\r\n"+
			"        when aircraftTypeId is null and typeRatingName = 'CE-750' then 33\r\n"+
			"        else AircraftTypeId \r\n"+
			"    end aircraftTypeId,\r\n"+
			"    CrewPosition as crewRank,\r\n"+
			"    case\r\n"+
			"        when aircraftTypeId = 10091 and ACTypeCount = 2 then 3 -- dual\r\n"+
			"        when aircraftTypeId = 10091 and ACTypeCount = 1 then 2 -- fusion\r\n"+
			"        when aircraftTypeId = 10092 and ACTypeCount = 1 then 1 -- proline\r\n"+
			"        else null\r\n"+
			"    end qualification,\r\n"+
			"    active, PersonnelGuidID, FirstName, MiddleName, LastName\r\n"+
			"from\r\n"+
			"(\r\n"+
			"    select OrganizationUserID, crewCode, baseAirportId, min(AircraftTypeId) AircraftTypeId, AircraftTypeName, TypeRatingId, TypeRatingName, \r\n"+
			"        min(CrewPositionId) CrewPositionId, CrewPosition, active,\r\n"+
			"        OperatorCertificateId, OperatorCertificateName, PersonnelGuidID, FirstName, MiddleName, LastName, count(*) ACTypeCount\r\n"+
			"    from\r\n"+
			"    (\r\n"+
			"        select distinct\r\n"+
			"            OrganizationUserID, crewCode, baseAirportId,\r\n"+
			"            case when AircraftTypeId = 9421 then 28 when AircraftTypeId = 10088 then 33 else AircraftTypeId end AircraftTypeId,\r\n"+
			"            case when AircraftTypeId = 9421 then 'CE-560' when  AircraftTypeId = 10088 then 'CE-750' else AircraftTypeName end AircraftTypeName,\r\n"+
			"            TypeRatingId, TypeRatingName, CrewPositionId, CrewPosition, active, OperatorCertificateId, OperatorCertificateName, PersonnelGuidID, \r\n"+
			"            FirstName, MiddleName, LastName\r\n"+
			"        from gama.flightcrew_temp\r\n"+
			"        where (AircraftTypeID in (28, 9421, 33, 10088, 9973, 10091, 10092) or TypeRatingName in ('BE-300','CE-560XL','CE-750'))\r\n"+
			"        -- and active = 1\r\n"+
			"    ) p\r\n"+
			"    group by OrganizationUserID\r\n"+
			") p";
	
	public void Refresh() throws SQLException {
		
		System.out.println("\r\n........... " + tableName + " ...........\r\n");
		
		int rowsInsert = 0;
		rowsInsert = insertIntoTempTable(tableName, clientSQL, loadInsertHeader);

		String tempTableName = "flightcrew_temp";
		
		if (rowsInsert>0) {
			updateTargetTable (tableName, deleteSQL, insertSQL, tempTableName);			
		}
	}
}
