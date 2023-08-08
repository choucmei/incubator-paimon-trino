package org.apache.paimon.trino;

import io.trino.spi.type.SqlTime;

import java.sql.*;
import java.util.Properties;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static java.lang.String.format;

public class TrinoJdbcQuery {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:trino://127.0.0.1:8080/paimon/default";
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
//        properties.setProperty("password", "admin");
//        properties.setProperty("SSL", "false");
        Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        query(statement, "show create table tb_all_type");
        query(statement, "select p,p0,p3,p6,p7,q,q0,q3,q6,q12,q13,r,r0,r3,r6,r12,r13 from tb_all_type ");
    }



    public static void query(Statement statement, String sql) throws SQLException {
        ResultSet rs = statement.executeQuery(sql);
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                System.out.println(rs.getObject(i + 1));
            }
        }
    }
}
