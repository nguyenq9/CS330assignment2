import java.util.Properties;
import java.util.Scanner;
import java.util.Arrays;

import javax.naming.spi.DirStateFactory.Result;

import java.io.FileInputStream;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class Demo {

    static Connection conn = null;

    public static void main(String[] args) throws Exception {
        // Get connection properties
        // String paramsFile = "ConnectionParameters_LabComputer.txt";
		String paramsFile = "ConnectionParameters_RemoteComputer.txt";
        if (args.length >= 1) {
            paramsFile = args[0];
        }
        Properties connectprops = new Properties();
        connectprops.load(new FileInputStream(paramsFile));

        try {
            // Get connection
            Class.forName("com.mysql.cj.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.println("Database connection is established.");

            // showCompanies();

            // Enter Ticker and TransDate, Fetch data for that ticker and date
            Scanner in = new Scanner(System.in);
            while (true) {
                System.out.print("Enter ticker and date (YYYY.MM.DD): ");
                String[] data = in.nextLine().trim().split("\\s+");
                if (data[0].isBlank()) {
                    break;
                }
                // showTickerDay(data[0], data[1]);
                // showCompanyName(data[0]);
                if (data.length == 1) {
                    showAllInfo(data[0]);
                } else {
                    showInfoBetweenDates(data[0], data[1], data[2]);
                }
            }
            System.out.println("Database connection closed.");
            conn.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
    }

    static void showInfoBetweenDates(String ticker, String start, String end) throws SQLException {
        int totalsplit = 0;
        PreparedStatement pstmt = conn.prepareStatement(
                "select Ticker, TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice " +
                "  from pricevolume " +
                "  where Ticker = ? and TransDate > ? and TransDate < ? order by TransDate DESC");

        pstmt.setString(1, ticker);
        pstmt.setString(2, start);
        pstmt.setString(3, end);
        ResultSet rs = pstmt.executeQuery();
        String[] currRow = new String[6];
        int tradingDays = 1;
        if (rs.next()) {
            currRow[0] = rs.getString(1).trim();
            currRow[1] = rs.getString(2).trim();
            currRow[2] = rs.getString(3).trim();
            currRow[3] = rs.getString(4).trim();
            currRow[4] = rs.getString(5).trim();
            currRow[5] = rs.getString(6).trim();
            tradingDays++;
        }

        while (rs.next()) {
            String[] nextRow = new String[6];
            nextRow[0] = rs.getString(1).trim();
            nextRow[1] = rs.getString(2).trim();
            nextRow[2] = rs.getString(3).trim();
            nextRow[3] = rs.getString(4).trim();
            nextRow[4] = rs.getString(5).trim();
            nextRow[5] = rs.getString(6).trim();
            
            double d1 = Double.parseDouble(nextRow[5]);
            double d2 = Double.parseDouble(currRow[2]);
            double val1 = Math.abs((d1 / d2) - 3.0);
            double val2 = Math.abs((d1 / d2) - 2.0);
            double val3 = Math.abs((d1 / d2) - 1.5);
            // System.out.println(Arrays.toString(currRow));
            // System.out.println(Arrays.toString(nextRow));
            // System.out.printf("%f %f %f %f %f \n\n", d1, d2, val1, val2, val3);

            // Checks for splits
            if (val3 < 0.15) {
                totalsplit++;
                System.out.printf("3:2 split on %s %11.2f --> %7.2f\n", nextRow[1], Double.parseDouble(nextRow[5]),
                Double.parseDouble(currRow[2]));
            } else if (val2 < 0.2) {
                totalsplit++;
                System.out.printf("2:1 split on %s %11.2f --> %7.2f\n", nextRow[1], Double.parseDouble(nextRow[5]),
                Double.parseDouble(currRow[2]));
            } else if (val1 < 0.3) {
                totalsplit++;
                System.out.printf("3:1 split on %s %11.2f --> %7.2f\n", nextRow[1], Double.parseDouble(nextRow[5]),
                Double.parseDouble(currRow[2]));
            } 
            currRow = nextRow;
            tradingDays++;
        }
        System.out.println(totalsplit + " splits in " + tradingDays + " trading days");
        System.out.println();


        pstmt.close();
    }

    static void showAllInfo(String ticker) throws SQLException {
        int totalsplit = 0;
        PreparedStatement pstmt = conn.prepareStatement(
                "select Ticker, TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice " +
                "  from pricevolume " +
                "  where Ticker = ? order by TransDate DESC");

        pstmt.setString(1, ticker);
        String[] currRow = new String[6];
        ResultSet rs = pstmt.executeQuery();
        int tradingDays = 0;
        if (rs.next()) {
            currRow[0] = rs.getString(1).trim();
            currRow[1] = rs.getString(2).trim();
            currRow[2] = rs.getString(3).trim();
            currRow[3] = rs.getString(4).trim();
            currRow[4] = rs.getString(5).trim();
            currRow[5] = rs.getString(6).trim();
            tradingDays++;
        }

        while (rs.next()) {
            String[] nextRow = new String[6];
            nextRow[0] = rs.getString(1).trim();
            nextRow[1] = rs.getString(2).trim();
            nextRow[2] = rs.getString(3).trim();
            nextRow[3] = rs.getString(4).trim();
            nextRow[4] = rs.getString(5).trim();
            nextRow[5] = rs.getString(6).trim();
            
            // double d1 = Double.parseDouble(nextRow[5]);
            // double d2 = Double.parseDouble(currRow[2]);
            // double val1 = Math.abs((d1 / d2) - 3.0);
            // double val2 = Math.abs((d1 / d2) - 2.0);
            // double val3 = Math.abs((d1 / d2) - 1.5);
            // // System.out.println(Arrays.toString(currRow));
            // // System.out.println(Arrays.toString(nextRow));
            // // System.out.printf("%f %f %f %f %f \n\n", d1, d2, val1, val2, val3);

            // // Checks for splits
            // if (val3 < 0.15) {
            //     totalsplit++;
            //     System.out.printf("3:2 split on %s %11.2f --> %7.2f\n", nextRow[1], Double.parseDouble(nextRow[5]),
            //     Double.parseDouble(currRow[2]));
            // } else if (val2 < 0.2) {
            //     totalsplit++;
            //     System.out.printf("2:1 split on %s %11.2f --> %7.2f\n", nextRow[1], Double.parseDouble(nextRow[5]),
            //     Double.parseDouble(currRow[2]));
            // } else if (val1 < 0.3) {
            //     totalsplit++;
            //     System.out.printf("3:1 split on %s %11.2f --> %7.2f\n", nextRow[1], Double.parseDouble(nextRow[5]),
            //     Double.parseDouble(currRow[2]));
            // } 
            System.out.println(Arrays.toString(nextRow));
            currRow = nextRow;
            tradingDays++;
        }
        System.out.println(totalsplit + " splits in " + tradingDays + " trading days");
        System.out.println();

        pstmt.close();
    }

    static void showTickerDay(String ticker, String date) throws SQLException {
        // Prepare query
        PreparedStatement pstmt = conn.prepareStatement(
                "select OpenPrice, HighPrice, LowPrice, ClosePrice " +
                "  from pricevolume " +
                "  where Ticker = ? and TransDate = ?");

        // Fill in the blanks
        pstmt.setString(1, ticker);
        pstmt.setString(2, date);
        ResultSet rs = pstmt.executeQuery();

        // Did we get anything? If so, output data.
        if (rs.next()) {
            System.out.printf("Open: %.2f, High: %.2f, Low: %.2f, Close: %.2f%n",
                    Double.parseDouble(rs.getString(1).trim()), 
                    Double.parseDouble(rs.getString(2).trim()),
                    Double.parseDouble(rs.getString(3).trim()),
                    Double.parseDouble(rs.getString(4).trim())
                    );
        } else {
            System.out.printf("Ticker %s, Date %s not found.%n", ticker, date);
        }
        pstmt.close();
    }
}