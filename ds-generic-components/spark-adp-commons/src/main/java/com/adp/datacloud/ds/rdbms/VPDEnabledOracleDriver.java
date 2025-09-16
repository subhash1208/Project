package com.adp.datacloud.ds.rdbms;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import oracle.jdbc.driver.OracleConnection;
import org.apache.log4j.Logger;

import oracle.jdbc.driver.OracleDriver;

public class VPDEnabledOracleDriver extends OracleDriver {

    private Logger logger = Logger.getLogger(VPDEnabledOracleDriver.class);

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Connection connect(String url, Properties props) throws SQLException {
        ConnectionSpy connect = new ConnectionSpy(super.connect(url, props));
        if (connect.isConnectionAvailable()) {
            // for java end to end tracing
            // https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/JDBC-standards-support.html#GUID-1987FAC4-E93A-49A5-9EB4-A78B465E6938
            // https://jira.service.tools-pi.com/browse/DCPL-9921
            if (props.stringPropertyNames().contains("MODULE")) {
                connect.setClientInfo("OCSID.MODULE", props.get("MODULE").toString());
            }
            if (props.stringPropertyNames().contains("ACTION")) {
                connect.setClientInfo("OCSID.ACTION", props.get("ACTION").toString());
            }
            connect.setClientInfo("OCSID.CLIENTID", "DXR2");


            if (props.stringPropertyNames().contains("vpd_key")) {
                logger.debug("vpd_key being set is:" + props.get("vpd_key"));
                String vpd_key = (String) props.get("vpd_key");
                String setVpdCall = "call sp_set_vpd_ctx('" + vpd_key.trim() + "','queryonly')";
                CallableStatement callableStatement = connect.prepareCall(setVpdCall);
                callableStatement.execute();
            }
            if (props.stringPropertyNames().contains("vpdCall")) {
                String setVpdCall = (String) props.get("vpdCall");
                logger.debug("vpdCall statement being executed: " + setVpdCall);
                CallableStatement callableStatement = connect.prepareCall(setVpdCall);
                callableStatement.execute();
            }
            return connect;

        }
        throw new SQLException("connection is null");
    }

}
