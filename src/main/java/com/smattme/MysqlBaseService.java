package com.smattme;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Created by seun_ on 01-Mar-18.
 */
public class MysqlBaseService {

    private static Logger logger = LoggerFactory.getLogger(MysqlBaseService.class);

    static final String SQL_START_PATTERN = "-- start";
    static final String SQL_END_PATTERN = "-- end";

    /**
     * This is a utility function for connecting to a
     * database instance that's running on localhost at port 3306.
     * It will build a JDBC URL from the given parameters and use that to
     * obtain a connect from doConnect()
     *
     * @param username   database username
     * @param password   database password
     * @param database   database name
     * @param driverName the user supplied mysql connector driver class name. Can be empty
     * @return Connection
     * @throws ClassNotFoundException exception
     * @throws SQLException           exception
     */
    static Connection connect(String username, String password, String database, String driverName) throws ClassNotFoundException, SQLException {
        String url = "jdbc:mysql://localhost:3306/" + database + "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false";
        String driver = (Objects.isNull(driverName) || driverName.isEmpty()) ? "com.mysql.cj.jdbc.Driver" : driverName;
        return doConnect(driver, url, username, password);
    }

    /**
     * This is a utility function that allows connecting
     * to a database instance identified by the provided jdbcURL
     * The connector driver name can be empty
     *
     * @param username   database username
     * @param password   database password
     * @param jdbcURL    the user supplied JDBC URL. It's used as is. So ensure you supply the right parameters
     * @param driverName the user supplied mysql connector driver class name
     * @return Connection
     * @throws ClassNotFoundException exception
     * @throws SQLException           exception
     */
    static Connection connectWithURL(String username, String password, String jdbcURL, String driverName) throws ClassNotFoundException, SQLException {
        String driver = (Objects.isNull(driverName) || driverName.isEmpty()) ? "com.mysql.cj.jdbc.Driver" : driverName;
        return doConnect(driver, jdbcURL, username, password);
    }

    /**
     * This will attempt to connect to a database using
     * the provided parameters.
     * On success it'll return the java.sql.Connection object
     *
     * @param driver   the class name for the mysql driver to use
     * @param url      the url of the database
     * @param username database username
     * @param password database password
     * @return Connection
     * @throws SQLException           exception
     * @throws ClassNotFoundException exception
     */
    private static Connection doConnect(String driver, String url, String username, String password) throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, username, password);
        logger.debug("DB Connected Successfully");
        return connection;
    }

    /**
     * This is a utility function to get the names of all
     * the tables that're in the database supplied
     *
     * @param database the database name
     * @param stmt     Statement object
     * @return List\<String\>
     * @throws SQLException exception
     */
    static List<String> getAllTables(String database, Statement stmt) throws SQLException {
        List<String> table = new ArrayList<>();
        ResultSet rs;
        rs = stmt.executeQuery("SELECT\n" +
                "    table_name,\n" +
                "    GROUP_CONCAT(referenced_table_name) AS `ref_table`\n" +
                "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE\n" +
                "WHERE table_schema like '" + database + "'\n" +
                "GROUP BY table_name;");

        boolean rerun = true;
        while (rerun) {
            rerun = false;
            rs.beforeFirst();
            while (rs.next()) {
                //Table already added.
                if (table.contains(rs.getString("table_name")))
                    continue;

                //No dependency or dependent table already added.
                if (rs.getString("ref_table") == null
                        || !missingDepForTable(rs.getString("ref_table"),table))
                    table.add(rs.getString("table_name"));
                    //Rerun because dependency not added yet.
                else
                    rerun = true;
            }
        }
        return table;
    }

    /**
     * Returns false if all dependent tables
     * are already in list. Otherwise it returns
     * true.
     *
     * @param ref_table The referenced tables
     * @param table List of added tables
     * @return boolean
     */
    private static boolean missingDepForTable(String ref_table, List<String> table) {
        for (String s : ref_table.split(","))
            if(!table.contains(s))
                return true;

        return false;
    }

    /**
     * This function is an helper function
     * that'll generate a DELETE FROM database.table
     * SQL to clear existing table
     *
     * @param database database
     * @param table    table
     * @return String sql to delete the all records from the table
     */
    static String getEmptyTableSQL(String database, String table) {
        String safeDeleteSQL = "SELECT IF( \n" +
                "(SELECT COUNT(1) as table_exists FROM information_schema.tables \n" +
                "WHERE table_schema='" + database + "' AND table_name='" + table + "') > 1, \n" +
                "'DELETE FROM " + table + "', \n" +
                "'SELECT 1') INTO @DeleteSQL; \n" +
                "PREPARE stmt FROM @DeleteSQL; \n" +
                "EXECUTE stmt; DEALLOCATE PREPARE stmt; \n";

        return "\n" + MysqlBaseService.SQL_START_PATTERN + "\n" +
                safeDeleteSQL + "\n" +
                "\n" + MysqlBaseService.SQL_END_PATTERN + "\n";
    }

    /**
     * This function is an helper function
     * that'll return all routines in database
     *
     * @param database database
     * @return Map containing the name as key and the type as value.
     * @throws SQLException exception
     */
    public static Map<String, String> getAllRoutines(String database, Statement stmt) throws SQLException {
        List<String> names = new LinkedList<>();
        Map<String, String> routines = new HashMap();
        ResultSet rs = stmt.executeQuery("SELECT SPECIFIC_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION FROM `information_schema`.`ROUTINES` WHERE ROUTINE_SCHEMA='" + database + "';");
        while (rs.next())
            names.add(rs.getString("SPECIFIC_NAME"));

        boolean rerun = true;
        while (rerun) {
            rerun = false;
            rs.beforeFirst();

            while (rs.next()) {
                if (missingLink(routines.keySet(), names, rs.getString("ROUTINE_DEFINITION"))) {
                    rerun = true;
                    continue;
                }
                if (!routines.containsKey(rs.getString("SPECIFIC_NAME")))
                    routines.put(rs.getString("SPECIFIC_NAME"), rs.getString("ROUTINE_TYPE"));
            }
        }
        return routines;
    }

    /**
     * Checks if an existing routinename
     * is called by the routine but hasn't
     * been added to the set yet.
     *
     * @param added             the routines allready added
     * @param existing          the list of all existing routines
     * @param routineDefinition the definition of the routine
     * @return boolean
     */
    private static boolean missingLink(Set<String> added, List<String> existing, String routineDefinition) {
        for (String s : existing)
            if (routineDefinition.contains(s) && !added.contains(s))
                return true;

        return false;
    }

    /**
     * This is a utility function to get the names of all
     * the views that're in the database supplied
     *
     * @param database the database name
     * @param stmt     Statement object
     * @return List\<String\>
     * @throws SQLException exception
     */
    static List<String> getAllViews(String database, Statement stmt) throws SQLException {
        List<String> views = new ArrayList<>();
        ResultSet rs;
        rs = stmt.executeQuery("SELECT\n" +
                "    VIEW_NAME,\n" +
                "    GROUP_CONCAT(VIEWS.TABLE_NAME) as ref_views\n" +
                "FROM INFORMATION_SCHEMA.VIEW_TABLE_USAGE\n" +
                "LEFT JOIN INFORMATION_SCHEMA.VIEWS on VIEWS.TABLE_NAME = VIEW_TABLE_USAGE.TABLE_NAME\n"+
                "WHERE VIEW_SCHEMA like '" + database + "'\n" +
                "GROUP BY VIEW_NAME;");

        boolean rerun = true;
        while (rerun) {
            rerun = false;
            rs.beforeFirst();
            while (rs.next()) {
                //Table already added.
                if (views.contains(rs.getString("VIEW_NAME")))
                    continue;

                //No dependency or dependent table already added.
                if (rs.getString("ref_views") == null
                        || !missingDepForTable(rs.getString("ref_views"),views))
                    views.add(rs.getString("VIEW_NAME"));
                    //Rerun because dependency not added yet.
                else
                    rerun = true;
            }
        }
        return views;
    }
}
