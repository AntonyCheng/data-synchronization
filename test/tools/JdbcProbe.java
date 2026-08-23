import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public final class JdbcProbe {
    private JdbcProbe() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: JdbcProbe <url> <username> <password>");
        }

        Driver driver = DriverManager.getDriver(args[0]);
        System.out.printf("driver=%s version=%d.%d%n",
            driver.getClass().getName(), driver.getMajorVersion(), driver.getMinorVersion());

        try (Connection connection = DriverManager.getConnection(args[0], args[1], args[2]);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select current_user, current_database(), version()")) {
            while (resultSet.next()) {
                System.out.printf("user=%s database=%s server=%s%n",
                    resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
            }
        }
    }
}
