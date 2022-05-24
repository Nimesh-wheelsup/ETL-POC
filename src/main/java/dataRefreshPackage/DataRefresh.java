package dataRefreshPackage;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;

public class DataRefresh {

    public static Connection clientConnection;
    public static Connection wuConnection;
    public static PrintStream out;
    public static String beginDate;
    public static String endDate;
    public static int hourToRunDaily = 4;

    public static void main(String[] args) throws SQLException, IOException, ParseException {

        // Open log file:
        //out = new PrintStream(new FileOutputStream("C:\\Repos\\Java\\ETL_DataRefresh_POC\\dataRefresh\\dataRefresh.log", false));		//local
        //out = new PrintStream(new FileOutputStream("..\\dataRefresh\\dataRefresh.log", false));		//AWS
        out = new PrintStream(new FileOutputStream("/app/dataRefresh/dataRefresh.log", false));		//Rackspace
        System.setOut(out);  // Direct console output to out above

        // Open and read parameter file:
        //FileReader fileReader = new FileReader("C:\\Repos\\Java\\ETL_DataRefresh_POC\\dataRefresh\\dataRefreshParam.txt"); 	//Local job
        //FileReader fileReader = new FileReader("..\\dataRefresh\\dataRefreshParam.txt"); 	//AWS job
        FileReader fileReader = new FileReader("/app/dataRefresh/dataRefreshParam.txt");		//Rackspace job

        BufferedReader bufferReader = new BufferedReader(fileReader);
        String connectionUrlClient = bufferReader.readLine();
        String connectionUrlWU = bufferReader.readLine();
        String username = bufferReader.readLine();
        String password = bufferReader.readLine();
        int daysBeforeHourly = Integer.parseInt(bufferReader.readLine());
        int daysAfterHourly = Integer.parseInt(bufferReader.readLine());
        int daysBeforeDaily = Integer.parseInt(bufferReader.readLine());
        int daysAfterDaily = Integer.parseInt(bufferReader.readLine());
        bufferReader.close();

        // Open connections:
        try {
            clientConnection = DriverManager.getConnection(connectionUrlClient);
            wuConnection = DriverManager.getConnection(connectionUrlWU, username, password);
        } catch (SQLException e) {
             e.printStackTrace(out);
            return;
        }

        // Decide job type and set up beginDate and endDate
        String jobType = "Hourly";
        int daysBefore = daysBeforeHourly;
        int daysAfter = daysAfterHourly;
        if(Calendar.getInstance().get(Calendar.HOUR_OF_DAY) == hourToRunDaily) {
            jobType = "Daily";
            daysBefore = daysBeforeDaily;
            daysAfter = daysAfterDaily;
        }

        SimpleDateFormat dateFormatUTC = new SimpleDateFormat("yyyy-MM-dd");
        Calendar beginCalendar = Calendar.getInstance();
        Calendar endCalendar = Calendar.getInstance();
        beginCalendar.add(Calendar.DATE, daysBefore);
        endCalendar.add(Calendar.DATE, daysAfter);
        beginDate = dateFormatUTC.format(beginCalendar.getTime());
        endDate = dateFormatUTC.format(endCalendar.getTime());

        System.out.println(Calendar.getInstance().getTime() + " "+ jobType + " Data Refresh started\r\n" +
                "\r\nRefresh data between " + beginDate + " and " + endDate);

        // Start data refresh
        Instant startTotal = Instant.now();
        Test test = new Test();
        /*AiropsMovement airopsMovement= new AiropsMovement();
        Flight flight = new Flight();

        Reservation reservation = new Reservation();
        AiropsMxEvents airopsMxEvents = new AiropsMxEvents();
        AiropsCrewDutyOrig airopsCrewDutyOrig = new AiropsCrewDutyOrig();
        AiropsCrewDuty airopsCrewDuty = new AiropsCrewDuty();
        AiropsCrewDutyMovement airopsCrewDutyMovement = new AiropsCrewDutyMovement();
        Crewdutydate crewdutydate = new Crewdutydate();
        Flightcrew flightcrew = new Flightcrew();*/

        test.Refresh();
        /*airopsMovement.Refresh();
        flight.Refresh();
        reservation.Refresh();
        flightcrew.Refresh();

        if(jobType == "Hourly") {
            airopsCrewDutyOrig.Refresh();
            airopsCrewDuty.Refresh();
            airopsCrewDutyMovement.Refresh();
            crewdutydate.Refresh();
        }

        airopsMxEvents.Refresh(); // need to be after airopsCrewDutyOrig.refresh to get no crew events
*/
        Instant endTotal = Instant.now();

        System.out.println("Total elasped time: " + Duration.between(startTotal, endTotal).toSeconds() + " seconds.");

        clientConnection.close();
        wuConnection.close();
        out.close();

    } // end main

}
