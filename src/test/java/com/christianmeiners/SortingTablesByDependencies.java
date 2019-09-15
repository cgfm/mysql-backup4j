package com.christianmeiners;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SortingTablesByDependencies {
    public static void main(String[] args) {
//        testOutput();
//        System.out.println();
//        testViewsOutput();

        testFilecreation();

    }

    private static void testFilecreation() {
        Properties properties = new Properties();
        properties.setProperty(MysqlExportService.DB_NAME, "belegungsplan");
        properties.setProperty(MysqlExportService.DB_USERNAME, "root");
        properties.setProperty(MysqlExportService.DB_PASSWORD, "password");

        properties.setProperty(MysqlExportService.PRESERVE_GENERATED_FILE, "true");
        properties.setProperty(MysqlExportService.ADD_IF_NOT_EXISTS, "true");

        properties.setProperty(MysqlExportService.PRESERVE_GENERATED_FILE, "true");
        properties.setProperty(MysqlExportService.ADD_IF_NOT_EXISTS, "true");
        properties.setProperty(MysqlExportService.TEMP_DIR, "export");
        properties.setProperty(MysqlExportService.ZIP_EXPORT_FILE, "false");

        MysqlExportService mysqlExportService = new MysqlExportService(properties);
        try {
            mysqlExportService.export();

            File file = mysqlExportService.getGeneratedFile();
            if(file!=null)
                Runtime.getRuntime().exec("explorer.exe /select, " + file.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void testOutput() {
        Connection connection = null;
        Statement stmt = null;
        try {
            connection = MysqlBaseService.connect("root", "password",
                    "belegungsplan", "");

            stmt = connection.createStatement();

            for (String belegungsplan : MysqlBaseService.getAllTables("belegungsplan", stmt)) {
                System.out.println(belegungsplan);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private static void testViewsOutput() {
        Connection connection = null;
        Statement stmt = null;
        try {
            connection = MysqlBaseService.connect("root", "password",
                    "belegungsplan", "");

            stmt = connection.createStatement();

            for (String belegungsplan : MysqlBaseService.getAllViews("belegungsplan", stmt)) {
                System.out.println(belegungsplan);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void testRoutines() {
        Connection connection = null;
        Statement stmt = null;
        try {
            connection = MysqlBaseService.connect("root", "password",
                    "belegungsplan", "");

            stmt = connection.createStatement();

            for (String belegungsplan : MysqlBaseService.getAllRoutines("belegungsplan", stmt).keySet()) {
                System.out.println(belegungsplan);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
