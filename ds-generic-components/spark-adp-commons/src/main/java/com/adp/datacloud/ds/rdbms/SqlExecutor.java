package com.adp.datacloud.ds.rdbms;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Scanner;

public class SqlExecutor {

	private Connection con;
	CallableStatement cmt = null;

	public SqlExecutor(String connstr, String username, String password) {

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(connstr, username, password);
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	public void executeProc(String path) {
		DatabaseMetaData mtdt;
		try {
			mtdt = con.getMetaData();
			String username = mtdt.getUserName();
			String batchsqls = "";
			Scanner infile = new Scanner(new File(path));
			Statement stmt = con.createStatement();

			while (infile.hasNextLine()) {

				String sql = infile.nextLine();
				batchsqls = batchsqls + sql.replaceAll("USER_ID", username);

			}

			String[] sprocs = null;
			int start_pos = 0, last_pos = 0;
			int no_of_procs = batchsqls.split("BEGIN").length;
			for (int i = 0; i < no_of_procs - 1; i++) {
				start_pos = batchsqls.indexOf("BEGIN", start_pos);
				last_pos = batchsqls.indexOf("END", start_pos + 1);
				String sql = batchsqls.substring(start_pos, last_pos + 4);
				stmt.executeUpdate(sql);
				System.out.println(sql);
				batchsqls = batchsqls.replace(sql, "");

			}

			if (batchsqls.length() > 3) {
				sprocs = batchsqls.split(";");
				for (int i = 0; i < sprocs.length; i++) {

					if (sprocs[i].toUpperCase().startsWith("EXEC") || sprocs[i].toUpperCase().startsWith("EXECUTE")) {

						if (sprocs[i].toUpperCase().startsWith("EXEC"))
							stmt.executeUpdate(sprocs[i].trim().toUpperCase().replace("EXEC", "BEGIN") + "; END;");
						else
							stmt.executeUpdate(sprocs[i].trim().toUpperCase().replace("EXECUTE", "BEGIN") + "; END;");

					} else {
						stmt.executeUpdate(sprocs[i]);
					}
					System.out.println(sprocs[i]);
				}
			}

			infile.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}

	}

	public static void main(String args[]) {

		if (args.length < 4) {
			System.out.println(
					"Number of arguments should be 4 -connection string , -username , -password ,-sqlfile path  "
							+ args.length);
			System.exit(0);
		}

		String connstr = args[0].trim();
		String username = args[1].trim();
		String password = args[2].trim();
		String sqlpath = args[3].trim();
		SqlExecutor connect = new SqlExecutor(connstr, username, password);
		connect.executeProc(sqlpath);
		System.out.println("Execution completed");
	}
}