package com.christianmeiners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Created by seun_ on 24-Feb-18.
 */
public class MysqlExportService {

    private Statement stmt;
    private String database;
    private String generatedSql = "";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String LOG_PREFIX = "java-mysql-exporter";
    private String dirName = "java-mysql-exporter-temp";
    private String sqlFileName = "";
    private String zipFileName = "";
    private Properties properties;
    private File generatedZipFile;

    public static final String EMAIL_HOST = "EMAIL_HOST";
    public static final String EMAIL_PORT = "EMAIL_PORT";
    public static final String EMAIL_USERNAME = "EMAIL_USERNAME";
    public static final String EMAIL_PASSWORD = "EMAIL_PASSWORD";
    public static final String EMAIL_SUBJECT = "EMAIL_SUBJECT";
    public static final String EMAIL_MESSAGE = "EMAIL_MESSAGE";
    public static final String EMAIL_FROM = "EMAIL_FROM";
    public static final String EMAIL_TO = "EMAIL_TO";

    public static final String DB_NAME = "DB_NAME";
    public static final String DB_USERNAME = "DB_USERNAME";
    public static final String DB_PASSWORD = "DB_PASSWORD";

    public static final String TEMP_DIR = "TEMP_DIR";
    public static final String ADD_IF_NOT_EXISTS = "ADD_IF_NOT_EXISTS";

    public static final String ZIP_EXPORT_FILE = "ZIP_EXPORT_FILE";
    public static final String PRESERVE_GENERATED_FILE = "PRESERVE_GENERATED_FILE";
    public static final String EXCLUDE_VIEWS = "EXCLUDE_VIEWS";
    public static final String EXCLUDE_ROUTINES = "EXCLUDE_ROUTINES";

    /**
     * @deprecated Name changed to {@link #PRESERVE_GENERATED_FILE}
     */
    public static final String PRESERVE_GENERATED_ZIP = "PRESERVE_GENERATED_FILE";

    /**
     * @deprecated This is deprecated in favour of the same option available
     * in the {@link MysqlImportService} class.
     */
    public static final String DROP_TABLES = "DROP_TABLES";

    /**
     * @deprecated This is deprecated in favour of the same option available
     * in the {@link MysqlImportService} class.
     */
    public static final String DELETE_EXISTING_DATA = "DELETE_EXISTING_DATA";


    public static final String JDBC_CONNECTION_STRING = "JDBC_CONNECTION_STRING";
    public static final String JDBC_DRIVER_NAME = "JDBC_DRIVER_NAME";
    public static final String SQL_FILE_NAME = "SQL_FILE_NAME";


    public MysqlExportService(Properties properties) {
        this.properties = properties;
    }

    /**
     * This function will check if the required minimum
     * properties are set for database connection and exporting
     *
     * @return bool
     */
    private boolean isValidateProperties() {
        return properties != null &&
                properties.containsKey(DB_USERNAME) &&
                properties.containsKey(DB_PASSWORD) &&
                (properties.containsKey(DB_NAME) || properties.containsKey(JDBC_CONNECTION_STRING));
    }

    /**
     * This function will check if all the minimum
     * required email properties are set,
     * that can facilitate sending of exported
     * sql to email
     *
     * @return bool
     */
    private boolean isEmailPropertiesSet() {
        return properties != null &&
                properties.containsKey(EMAIL_HOST) &&
                properties.containsKey(EMAIL_PORT) &&
                properties.containsKey(EMAIL_USERNAME) &&
                properties.containsKey(EMAIL_PASSWORD) &&
                properties.containsKey(EMAIL_FROM) &&
                properties.containsKey(EMAIL_TO);
    }

    /**
     * This function will return true
     * or false based on the availability
     * or absence of a custom output sql
     * file name
     *
     * @return bool
     */
    private boolean isSqlFileNamePropertySet() {
        return properties != null &&
                properties.containsKey(SQL_FILE_NAME);
    }

    /**
     * This function will return true
     * or false based on the value set
     * for {@link #ZIP_EXPORT_FILE}
     *
     * @return bool
     */
    private boolean doZipExport() {
        return Boolean.parseBoolean(
                properties.containsKey(ZIP_EXPORT_FILE) ?
                        properties.getProperty(ZIP_EXPORT_FILE, Boolean.TRUE.toString()) :
                        Boolean.TRUE.toString());
    }

    /**
     * This function will return true
     * or false based on the value set
     * for {@link #EXCLUDE_VIEWS}
     *
     * @return bool
     */
    private boolean excludeViews() {
        return Boolean.parseBoolean(
                properties.containsKey(EXCLUDE_VIEWS) ?
                        properties.getProperty(EXCLUDE_VIEWS, Boolean.TRUE.toString()) :
                        Boolean.TRUE.toString());
    }

    /**
     * This function will return true
     * or false based on the value set
     * for {@link #EXCLUDE_ROUTINES}
     *
     * @return bool
     */
    private boolean excludeRoutines() {
        return Boolean.parseBoolean(
                properties.containsKey(EXCLUDE_ROUTINES) ?
                        properties.getProperty(EXCLUDE_ROUTINES, Boolean.TRUE.toString()) :
                        Boolean.TRUE.toString());
    }

    /**
     * This will generate the SQL statement
     * for creating the table supplied in the
     * method signature
     *
     * @param table the table concerned
     * @return String
     * @throws SQLException exception
     */
    private String getTableInsertStatement(String table) throws SQLException {

        StringBuilder sql = new StringBuilder();
        ResultSet rs;
        boolean addIfNotExists = Boolean.parseBoolean(properties.containsKey(ADD_IF_NOT_EXISTS) ? properties.getProperty(ADD_IF_NOT_EXISTS, Boolean.TRUE.toString()) : Boolean.TRUE.toString());


        if (table != null && !table.isEmpty()) {
            rs = stmt.executeQuery("SHOW CREATE TABLE " + table + ";");
            while (rs.next()) {
                String qtbl = rs.getString(1);
                String query = rs.getString(2);
                sql.append("\n\n--");
                sql.append("\n").append(MysqlBaseService.SQL_START_PATTERN).append("  table dump : ").append(qtbl);
                sql.append("\n--\n\n");

                if (addIfNotExists) {
                    query = query.trim().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                }

                sql.append(query).append(";\n\n");
            }

            sql.append("\n\n--");
            sql.append("\n").append(MysqlBaseService.SQL_END_PATTERN).append("  table dump : ").append(table);
            sql.append("\n--\n\n");
        }

        return sql.toString();
    }

    /**
     * This function will generate the insert statements needed
     * to recreate the table under processing.
     *
     * @param table the table to get inserts statement for
     * @return String generated SQL insert
     * @throws SQLException exception
     */
    private String getDataInsertStatement(String table) throws SQLException {

        StringBuilder sql = new StringBuilder();

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + ";");

        //move to the last row to get max rows returned
        rs.last();
        int rowCount = rs.getRow();

        //there are no records just return empty string
        if (rowCount <= 0) {
            return sql.toString();
        }

        sql.append("\n--").append("\n-- Inserts of ").append(table).append("\n--\n\n");

        //temporarily disable foreign key constraint
        sql.append("\n/*!40000 ALTER TABLE `").append(table).append("` DISABLE KEYS */;\n");

        sql.append("\n--\n")
                .append(MysqlBaseService.SQL_START_PATTERN).append(" table insert : ").append(table)
                .append("\n--\n");

        sql.append("INSERT INTO `").append(table).append("`(");

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        //generate the column names that are present
        //in the returned result set
        //at this point the insert is INSERT INTO (`col1`, `col2`, ...)
        for (int i = 0; i < columnCount; i++) {
            sql.append("`")
                    .append(metaData.getColumnName(i + 1))
                    .append("`, ");
        }

        //remove the last whitespace and comma
        sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1).append(") VALUES \n");

        //now we're going to build the values for data insertion
        rs.beforeFirst();
        while (rs.next()) {
            sql.append("(");
            for (int i = 0; i < columnCount; i++) {

                int columnType = metaData.getColumnType(i + 1);
                int columnIndex = i + 1;

                //this is the part where the values are processed based on their type
                if (Objects.isNull(rs.getObject(columnIndex))) {
                    sql.append("").append(rs.getObject(columnIndex)).append(", ");
                } else if (columnType == Types.INTEGER || columnType == Types.TINYINT || columnType == Types.BIT) {
                    sql.append(rs.getInt(columnIndex)).append(", ");
                } else {

                    String val = rs.getString(columnIndex);
                    //escape the single quotes that might be in the value
                    val = val.replace("'", "\\'");

                    sql.append("'").append(val).append("', ");
                }
            }

            //now that we're done with a row
            //let's remove the last whitespace and comma
            sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1);

            //if this is the last row, just append a closing
            //parenthesis otherwise append a closing parenthesis and a comma
            //for the next set of values
            if (rs.isLast()) {
                sql.append(")");
            } else {
                sql.append("),\n");
            }
        }

        //now that we are done processing the entire row
        //let's add the terminator
        sql.append(";");

        sql.append("\n--\n")
                .append(MysqlBaseService.SQL_END_PATTERN).append(" table insert : ").append(table)
                .append("\n--\n");

        //enable FK constraint
        sql.append("\n/*!40000 ALTER TABLE `").append(table).append("` ENABLE KEYS */;\n");

        return sql.toString();
    }

    /**
     * This function returns the create
     * statement for the given routine.
     *
     * @param name name of the routine
     * @param type type of the routine
     * @return String
     * @throws SQLException exception
     */
    private String getRoutineCreateStatement(String name, String type) throws SQLException {
        StringBuilder sql = new StringBuilder();
        ResultSet rs = stmt.executeQuery("SHOW CREATE " + type + " " + name + ";");
        rs.next();

        if(rs.getString("Create " + type)==null) {
            logger.warn("User \""+properties.getProperty(DB_USERNAME)+"\" has no right to read Create Statement for "+type+" "+name);
            return "";
        }

        sql.append("\n\n--")
                .append("\n")
                .append(MysqlBaseService.SQL_START_PATTERN)
                .append("  ")
                .append(type.toLowerCase())
                .append(" dump : ")
                .append(name)
                .append("\n--\n\n")
                .append(rs.getString("Create " + type))
                .append((rs.getString("Create " + type).endsWith(";") ? "" : ";"))
                .append("\n\n--\n")
                .append(MysqlBaseService.SQL_END_PATTERN)
                .append("  ")
                .append(type.toLowerCase())
                .append(" dump : ")
                .append(name);
        sql.append("\n--\n\n");
        return sql.toString();
    }

    /**
     * This is the entry function that'll
     * coordinate getTableInsertStatement() and getDataInsertStatement()
     * for every table in the database to generate a whole
     * script of SQL
     *
     * @return String
     * @throws SQLException exception
     */
    private String exportToSql() throws SQLException {

        StringBuilder sql = new StringBuilder();
        sql.append("--");
        sql.append("\n-- Generated by mysql-backup4j");
        sql.append("\n-- https://github.com/SeunMatt/mysql-backup4j");
        sql.append("\n-- Date: ").append(new SimpleDateFormat("d-M-Y H:m:s").format(new Date()));
        sql.append("\n--");

        //these declarations are extracted from HeidiSQL
        sql.append("\n\n/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;")
                .append("\n/*!40101 SET NAMES utf8 */;")
                .append("\n/*!50503 SET NAMES utf8mb4 */;")
                .append("\n/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;")
                .append("\n/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;");


        //for every table in the database, get the table creation and data insert statement
        for (String s : MysqlBaseService.getAllTables(database, stmt)) {
            try {
                sql.append(getTableInsertStatement(s.trim()));
                sql.append(getDataInsertStatement(s.trim()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        //get the routines that are in the database
        for (Map.Entry<String, String> stringStringEntry : MysqlBaseService.getAllRoutines(database, stmt).entrySet()) {
            try {
                sql.append(getRoutineCreateStatement(stringStringEntry.getKey(), stringStringEntry.getValue()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        //get the routines that are in the database
        for (String view : MysqlBaseService.getAllViews(database, stmt)) {
            try {
                sql.append(getRoutineCreateStatement(view.trim(), "VIEW"));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        sql.append("\n/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;")
                .append("\n/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;")
                .append("\n/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;");

        this.generatedSql = sql.toString();
        return sql.toString();
    }

    /**
     * This is the entry point for exporting
     * the database. It performs validation and
     * the initial object initializations,
     * database connection and setup
     * before ca
     *
     * @throws IOException            exception
     * @throws SQLException           exception
     * @throws ClassNotFoundException exception
     */
    public void export() throws IOException, SQLException, ClassNotFoundException {

        //check if properties is set or not
        if (!isValidateProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return;
        }

        //connect to the database
        database = properties.getProperty(DB_NAME);
        String jdbcURL = properties.getProperty(JDBC_CONNECTION_STRING, "");
        String driverName = properties.getProperty(JDBC_DRIVER_NAME, "");

        Connection connection;

        if (jdbcURL.isEmpty()) {
            connection = MysqlBaseService.connect(properties.getProperty(DB_USERNAME), properties.getProperty(DB_PASSWORD),
                    database, driverName);
        } else {
            if (jdbcURL.contains("?")) {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1, jdbcURL.indexOf("?"));
            } else {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1);
            }
            logger.debug("database name extracted from connection string: " + database);
            connection = MysqlBaseService.connectWithURL(properties.getProperty(DB_USERNAME), properties.getProperty(DB_PASSWORD),
                    jdbcURL, driverName);
        }

        stmt = connection.createStatement();

        //generate the final SQL
        String sql = exportToSql();

        //create a temp dir to store the exported file for processing
        dirName = properties.getProperty(MysqlExportService.TEMP_DIR, dirName);
        File file = new File(dirName);
        if (!file.exists()) {
            boolean res = file.mkdir();
            if (!res) {
//                logger.error(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
                throw new IOException(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
            }
        }

        File sqlFolder;

        //zip the file
        if (doZipExport()) {
            //write the sql file out
            sqlFolder = new File(dirName + "/sql");
            if (!sqlFolder.exists()) {
                boolean res = sqlFolder.mkdir();
                if (!res) {
                    throw new IOException(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
                }
            }

            sqlFileName = getSqlFilename();
            FileOutputStream outputStream = new FileOutputStream(sqlFolder + "/" + sqlFileName);
            outputStream.write(sql.getBytes());
            outputStream.close();

            zipFileName = dirName + "/" + sqlFileName.replace(".sql", ".zip");
            generatedZipFile = new File(zipFileName);
            ZipUtil.pack(sqlFolder, generatedZipFile);
        } else {
            sqlFolder = new File(dirName);
            sqlFileName = getSqlFilename();
            FileOutputStream outputStream = new FileOutputStream(sqlFolder.getAbsoluteFile() + "/" + sqlFileName);
            outputStream.write(sql.getBytes());
            outputStream.close();
        }

        //mail the zipped file if mail settings are available
        if (isEmailPropertiesSet()) {
            boolean emailSendingRes = EmailService.builder()
                    .setHost(properties.getProperty(EMAIL_HOST))
                    .setPort(Integer.valueOf(properties.getProperty(EMAIL_PORT)))
                    .setToAddress(properties.getProperty(EMAIL_TO))
                    .setFromAddress(properties.getProperty(EMAIL_FROM))
                    .setUsername(properties.getProperty(EMAIL_USERNAME))
                    .setPassword(properties.getProperty(EMAIL_PASSWORD))
                    .setSubject(properties.getProperty(EMAIL_SUBJECT, sqlFileName.replace(".sql", "").toUpperCase()))
                    .setMessage(properties.getProperty(EMAIL_MESSAGE, "Please find attached database backup of " + database))
                    .setAttachments(new File[]{new File((doZipExport() ? zipFileName : sqlFolder + "/" + sqlFileName))})
                    .sendMail();

            if (emailSendingRes) {
                logger.debug(LOG_PREFIX + ": " + (doZipExport() ? "Zip" : "SQL") + " File Sent as Attachment to Email Address Successfully");
            } else {
                logger.error(LOG_PREFIX + ": Unable to send " + (doZipExport() ? "zipped" : "") + " file as attachment to email. See log debug for more info");
            }
        }

        //clear the generated temp files
        clearTempFiles(Boolean.parseBoolean(properties.getProperty(PRESERVE_GENERATED_FILE, Boolean.FALSE.toString())));

    }

    /**
     * This function will delete all the
     * temp files generated ny the library
     * unless it's otherwise instructed not to do
     * so by the preserveFile variable
     *
     * @param preserveFile bool
     */
    public void clearTempFiles(boolean preserveFile) {

        //delete the temp sql file
        if (doZipExport()) {
            File sqlFile = new File(dirName + "/sql/" + sqlFileName);
            if (sqlFile.exists()) {
                boolean res = sqlFile.delete();
                logger.debug(LOG_PREFIX + ": " + sqlFile.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
            } else {
                logger.debug(LOG_PREFIX + ": " + sqlFile.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
            }

            File sqlFolder = new File(dirName + "/sql");
            if (sqlFolder.exists()) {
                boolean res = sqlFolder.delete();
                logger.debug(LOG_PREFIX + ": " + sqlFolder.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
            } else {
                logger.debug(LOG_PREFIX + ": " + sqlFolder.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
            }

            //only execute this section if the
            //file is not to be preserved

            if (!preserveFile) {
                //delete the zipFile
                File zipFile = new File(zipFileName);
                if (zipFile.exists()) {
                    boolean res = zipFile.delete();
                    logger.debug(LOG_PREFIX + ": " + zipFile.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
                } else {
                    logger.debug(LOG_PREFIX + ": " + zipFile.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
                }

                //delete the temp folder
                File folder = new File(dirName);
                if (folder.exists()) {
                    boolean res = folder.delete();
                    logger.debug(LOG_PREFIX + ": " + folder.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
                } else {
                    logger.debug(LOG_PREFIX + ": " + folder.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
                }
            }
        } else if (!preserveFile) {
            File sqlFile = new File(dirName + sqlFileName);
            if (sqlFile.exists()) {
                boolean res = sqlFile.delete();
                logger.debug(LOG_PREFIX + ": " + sqlFile.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
            } else {
                logger.debug(LOG_PREFIX + ": " + sqlFile.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
            }

            File sqlFolder = new File(dirName);
            if (sqlFolder.exists()) {
                boolean res = sqlFolder.delete();
                logger.debug(LOG_PREFIX + ": " + sqlFolder.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
            } else {
                logger.debug(LOG_PREFIX + ": " + sqlFolder.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
            }
        }

        logger.debug(LOG_PREFIX + ": generated temp files cleared successfully");
    }

    /**
     * This will get the final output
     * sql file name.
     *
     * @return String
     */
    public String getSqlFilename() {
        return isSqlFileNamePropertySet() ? properties.getProperty(SQL_FILE_NAME) + ".sql" :
                new SimpleDateFormat("d_M_Y_H_mm_ss").format(new Date()) + "_" + database + "_database_dump.sql";
    }

    public String getSqlFileName() {
        return sqlFileName;
    }

    public String getGeneratedSql() {
        return generatedSql;
    }

    public File getGeneratedZipFile() {
        if (generatedZipFile != null && generatedZipFile.exists()) {
            return generatedZipFile;
        }
        return null;
    }

    public File getGeneratedFile() {
        if (doZipExport()) {
            if (generatedZipFile != null && generatedZipFile.exists())
                return generatedZipFile;
        } else {
            File file = new File(dirName + File.separatorChar + getSqlFilename());
            if (file.exists())
                return file;
        }
        return null;
    }
}
