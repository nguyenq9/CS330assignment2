import java.io.FileInputStream;
import java.io.PrintWriter;
import java.security.cert.PKIXBuilderParameters;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import javax.swing.text.html.HTMLDocument.RunElement;

class NguyenAssignment2 {

    static class StockData {
        private String date;
        private double openPrice;
        private double highPrice;
        private double lowPrice;
        private double closePrice;

        public StockData(
                String date,
                double openPrice,
                double highPrice,
                double lowPrice,
                double closePrice) {
            this.date = date;
            this.openPrice = openPrice;
            this.highPrice = highPrice;
            this.lowPrice = lowPrice;
            this.closePrice = closePrice;
        }

        String getDate() {
            return date;
        }

        double getOpenPrice() {
            return openPrice;
        }

        double getHighPrice() {
            return highPrice;
        }

        double getLowPrice() {
            return lowPrice;
        }

        double getClosePrice() {
            return closePrice;
        }
    }

    static Connection conn;
    static final String prompt = "Enter ticker symbol [start/end dates]: ";

    public static void main(String[] args) throws Exception {
        // String paramsFile = "ConnectionParameters_LabComputer.txt";
        String paramsFile = "ConnectionParameters_RemoteComputer.txt";

        if (args.length >= 1) {
            paramsFile = args[0];
        }

        Properties connectprops = new Properties();
        connectprops.load(new FileInputStream(paramsFile));
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.println("Database connection is established");

            Scanner in = new Scanner(System.in);
            System.out.print(prompt);
            String input = in.nextLine().trim();

            while (input.length() > 0) {
                String[] params = input.split("\\s+");
                String ticker = params[0];
                String startdate = null, enddate = null;
                if (getName(ticker)) {
                    if (params.length >= 3) {
                        startdate = params[1];
                        enddate = params[2];
                    }
                    Deque<StockData> data = getStockData(ticker, startdate, enddate);
                    System.out.println();
                    System.out.println("Executing investment strategy");
                    doStrategy(ticker, data);
                }

                System.out.println();
                System.out.print(prompt);
                input = in.nextLine().trim();
            }

            // Close the database connection
            conn.close();
            System.out.println("Database connection closed.");
        } catch (SQLException ex) {
            System.out.printf(
                    "SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                    ex.getMessage(),
                    ex.getSQLState(),
                    ex.getErrorCode());
        }
    }

    static boolean getName(String ticker) throws SQLException {
        boolean found = false;
        PreparedStatement pstmt = conn.prepareStatement(
                "select Name " + "  from company " + "  where Ticker = ?");

        pstmt.setString(1, ticker);
        ResultSet results = pstmt.executeQuery();

        if (results.next()) {
            System.out.println(results.getString("Name"));
            found = true;
        } else {
            System.out.println(ticker + " not found in database.");
        }

        pstmt.close();
        return found;
    }


    static Deque<StockData> getStockData(String ticker, String start, String end)
            throws SQLException {
        Deque<StockData> result = new ArrayDeque<>();
        int totalsplit = 0;
        int tradingDays = 0;
        double divisor = 1.0;
        PreparedStatement pstmt = conn.prepareStatement(
                "select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice " +
                        "  from pricevolume " +
                        "  where Ticker = ? order by TransDate DESC");

        if (start != null) {
            pstmt = conn.prepareStatement(
                    "select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice " +
                            "  from pricevolume " +
                            "  where Ticker = ? and TransDate > ? and TransDate < ? order by TransDate DESC");
            tradingDays = 1;

            pstmt.setString(2, start);
            pstmt.setString(3, end);
        }

        pstmt.setString(1, ticker);
        ResultSet rs = pstmt.executeQuery();

        String[] currRow = new String[5];
        if (rs.next()) {
            currRow[0] = rs.getString(1).trim();
            currRow[1] = rs.getString(2).trim();
            currRow[2] = rs.getString(3).trim();
            currRow[3] = rs.getString(4).trim();
            currRow[4] = rs.getString(5).trim();
            tradingDays++;
        }

        String date = currRow[0];
        double open = Double.parseDouble(currRow[1]);
        double high = Double.parseDouble(currRow[2]);
        double low = Double.parseDouble(currRow[3]);
        double close = Double.parseDouble(currRow[4]);

        StockData newData = new StockData(
                    date,
                    open / divisor,
                    high / divisor,
                    low / divisor,
                    close / divisor);
            result.addFirst(newData);

        while (rs.next()) {
            String[] nextRow = new String[5];
            nextRow[0] = rs.getString(1).trim();
            nextRow[1] = rs.getString(2).trim();
            nextRow[2] = rs.getString(3).trim();
            nextRow[3] = rs.getString(4).trim();
            nextRow[4] = rs.getString(5).trim();

            date = nextRow[0];
            open = Double.parseDouble(nextRow[1]);
            high = Double.parseDouble(nextRow[2]);
            low = Double.parseDouble(nextRow[3]);
            close = Double.parseDouble(nextRow[4]);


            double d1 = Double.parseDouble(nextRow[4]);
            double d2 = Double.parseDouble(currRow[1]);
            double val1 = Math.abs((d1 / d2) - 3.0);
            double val2 = Math.abs((d1 / d2) - 2.0);
            double val3 = Math.abs((d1 / d2) - 1.5);
            if (val3 < 0.15) {
                totalsplit++;
                divisor = divisor * (3.0 / 2.0);
                System.out.printf(
                        "3:2 split on %s %.2f --> %.2f\n",
                        nextRow[0],
                        Double.parseDouble(nextRow[4]),
                        Double.parseDouble(currRow[1]));
            } else if (val2 < 0.2) {
                totalsplit++;
                divisor = divisor * 2.0;
                System.out.printf(
                        "2:1 split on %s %.2f --> %.2f\n",
                        nextRow[0],
                        Double.parseDouble(nextRow[4]),
                        Double.parseDouble(currRow[1]));
            } else if (val1 < 0.3) {
                totalsplit++;
                divisor = divisor * 3.0;
                System.out.printf(
                        "3:1 split on %s %.2f --> %.2f\n",
                        nextRow[0],
                        Double.parseDouble(nextRow[4]),
                        Double.parseDouble(currRow[1]));
            }
            newData = new StockData(
                    date,
                    open / divisor,
                    high / divisor,
                    low / divisor,
                    close / divisor);
            result.addFirst(newData);
            currRow = nextRow;
            tradingDays++;
        }

        System.out.println(
                totalsplit + " splits in " + tradingDays + " trading days");

        pstmt.close();

        return result;
    }

    static StockData getFromDeque(int i, Deque<StockData> data) {
        Iterator row = data.iterator();
        int j = i;
        if (data.size() < 1 || i > data.size()) {
            return null;
        }
        StockData d = null;
        while (row.hasNext() && j >= 0) {
            d = (StockData) row.next();
            j--;
        }
        return d;
    }

    static double averageClosePrice(double total, double count) {
        return total/count;
    }

    static boolean buy(int i, double avg, Deque<StockData> data, int numStock) {
        if (getFromDeque(i, data).getClosePrice() < avg && (getFromDeque(i, data).getClosePrice()/getFromDeque(i, data).getOpenPrice()) < 0.97000001) {
            return true;
        } else {
            return false;
        }
    }

    static boolean sell(int i, double avg, Deque<StockData> data, int numStock) {
        if (numStock >= 100 && getFromDeque(i, data).getOpenPrice() > avg && (getFromDeque(i, data).getOpenPrice()/getFromDeque(i-1, data).getClosePrice()) > 1.00999999) {
            return true;
        } else {
            return false;
        }
    }

    static void doStrategy(String ticker, Deque<StockData> data) {
        int transaction = 0;
        double netCash = 0.0;
        int numStock = 0;

        boolean run = false;
        if (data.size() >= 51) {
            run = true;
        }

        if (run) {
            double t = 0.0;
            double avg = 0.0;
            for (int i = 0; i < 50; i++) {
                t += getFromDeque(i, data).getClosePrice();
            }
            avg = averageClosePrice(t, 50);
            for (int i = 50; i < data.size()-1; i++) {
                double total = 0.0;
                
                for (int j = i; j > (i - 50); j--) {
                    total += getFromDeque(j, data).getClosePrice();
                }

                if (buy(i, avg, data, numStock)) {
                    numStock += 100;
                    netCash -= 8;
                    netCash -= 100.00*getFromDeque(i+1, data).getOpenPrice();
                    transaction++;
                } else if (sell(i, avg, data, numStock)) {
                    numStock -= 100;
                    netCash -= 8;
                    netCash += 100.00*(getFromDeque(i, data).getOpenPrice()+getFromDeque(i, data).getClosePrice())/2;
                    transaction++;
                }
                avg = averageClosePrice(total, 50);
            }
            netCash += numStock*getFromDeque(data.size(), data).getOpenPrice();
            numStock = 0;
        }
        System.out.println("Transactions executed: " + transaction);
        System.out.printf("Net cash: %.2f\n", netCash);
    }
}
