package com.adp.datacloud.ds.rdbms;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

/**
 * Map-reduce based program to retrieve data using Free-form SQLs. This is best
 * suited for "per-client" data retrievals from RDBMS to HDFS.
 * 
 * This may be deprecated in the future with better alternatives using spark
 * 
 * @author Manoj Oleti
 *
 */
public class FreeFormQueryExtractor {

	private static final Logger logger = Logger.getLogger(FreeFormQueryExtractor.class);

	final static String HDFS_INPUT_SQL_FILE_PATH = "RDBMS_EXTRACT_INPUT.sql";

	public static class Column {
		private String columnName;
		private Integer sqlType;
		private Integer precision;
		private Integer scale;

		public Column(String columnName, Integer sqlType, Integer precision, Integer scale) {
			this.columnName = columnName;
			this.sqlType = sqlType;
			this.precision = precision;
			this.scale = scale;
		}

		public String getColumnName() {
			return columnName;
		}

		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public Integer getPrecision() {
			return precision;
		}

		public void setPrecision(Integer precision) {
			this.precision = precision;
		}

		public Integer getScale() {
			return scale;
		}

		public void setScale(Integer scale) {
			this.scale = scale;
		}

		public Integer getSqlType() {
			return sqlType;
		}

		public void setSqlType(Integer sqlType) {
			this.sqlType = sqlType;
		}
	}

	public static class IdentityMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Each Unique ID is directly written back as both key & value
			context.write(value, value);
		}
	}

	public static class IdDrivenExtractReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		private static Connection connection = null;
		private static final char DELIMITER = '~';

		private String clean(Object value, ResultSet rs) throws SQLException {
			if (rs.wasNull() || value == null) // value will never be null.. but still
				return "\\N";
			return ("" + value).replaceAll("" + DELIMITER, " ").replaceAll("[\\p{Cc}\\p{Cf}]", ""); // To remove all
																									// invisible control
																									// characters
		}

		private String getRowAsDelimitedString(ResultSet rs, List<Column> columnsMeta, final char DELIM)
				throws SQLException {
			StringBuilder sb = new StringBuilder();
			if (columnsMeta.size() == 1) {
				String column = columnsMeta.get(0).getColumnName();
				sb.append(clean(rs.getString(column), rs));
			} else {
				for (int columnIndex = 0; columnIndex < columnsMeta.size(); columnIndex++) {
					String column = columnsMeta.get(columnIndex).getColumnName();
					switch (columnsMeta.get(columnIndex).getSqlType()) {
					case java.sql.Types.VARCHAR:
						sb.append(clean(rs.getString(column), rs));
						break;
					case java.sql.Types.CHAR:
						sb.append(clean(rs.getString(column), rs));
						break;
					case java.sql.Types.NUMERIC:
						// Can still be integer based. Check precision
						if (columnsMeta.get(columnIndex).getPrecision() == 38) {
							sb.append(clean(rs.getInt(column), rs));
						} else {
							sb.append(clean(rs.getDouble(column), rs));
						}
						break;
					case java.sql.Types.INTEGER:
						sb.append(clean(rs.getInt(column), rs));
						break;
					case java.sql.Types.DATE:
						sb.append(clean(rs.getDate(column), rs)); // TODO: Revisit Format
						break;
					case java.sql.Types.TIMESTAMP:
						sb.append(clean(rs.getTimestamp(column), rs)); // TODO: Revisit Format
						break;
					default:
						sb.append(clean(rs.getObject(column), rs));
					}
					sb.append(DELIM);
				}
			}
			return sb.toString() + "\n";
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			StringWriter writer = new StringWriter();
			IOUtils.copy(Thread.currentThread().getContextClassLoader().getResourceAsStream(HDFS_INPUT_SQL_FILE_PATH),
					writer, "UTF-8");
			String sqlTemplate = writer.toString();

			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			} catch (ClassNotFoundException e) {
				logger.error("Class not found " + e.getMessage());
				throw new IOException(e.getMessage());
			}

			String jdbcURL = context.getConfiguration().get("DB_CONNECTION_STRING");
			String username = context.getConfiguration().get("DB_USERNAME");
			String password = context.getConfiguration().get("DB_PASSWORD");

			try {
				connection = DriverManager.getConnection(jdbcURL, username, password);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Cannot create DB connection for user " + username + " " + e.getMessage());
				throw new IOException("Cannot create DB connection for user " + username + " " + e.getMessage());
			}

			String sql = null;

			for (Text uniqueID : values) {
				sql = sqlTemplate.replaceAll("UNIQUE_ID", uniqueID.toString());
				Statement stmt = null;
				ResultSet rs = null;
				List<Column> columnsMeta = new ArrayList<Column>();
				try {
					stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					rs = stmt.executeQuery(sql);
					ResultSetMetaData rsm = rs.getMetaData();
					int numColumns = rsm.getColumnCount();

					if (columnsMeta.isEmpty()) {
						for (int i = 1; i <= numColumns; i++) {
							columnsMeta.add(new Column(rsm.getColumnName(i), rsm.getColumnType(i), rsm.getPrecision(i),
									rsm.getScale(i)));
						}
					}

					while (rs.next()) {
						String rowDat = this.getRowAsDelimitedString(rs, columnsMeta, DELIMITER);
						if (rs.isLast()) {
							// For last row in result set we are removing \n from the end which is causing a
							// total null record in output
							String token = rowDat.substring(0, (rowDat.length() - 1));
							sb.append(token);
						} else {
							sb.append(rowDat);
						}
					}
				} catch (SQLException e) {
					logger.error("SQL exception occured " + e.getMessage() + " while executing " + sql);
					throw new IOException(e.getMessage() + "\n" + sql);
				} finally {
					try {
						if (rs != null)
							rs.close();
						if (stmt != null)
							stmt.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}

			try {
				connection.close();
			} catch (Exception e) {
				logger.info("Cannot close connection");
			}

			// This is to make sure we are not adding result sets which have no rows i.e no
			// data retrieved for the client
			if (sb.length() != 0) {
				result.set(sb.toString());
				context.write(null, result);
			}
		}
	}

	public static void main(String[] allArgs) throws Exception {

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();

		if (args == null || !(args.length == 3 || args.length == 4)) {
			logger.info(
					"Usage : hadoop jar <jar_name> <class_name> -libjars /var/lib/sqoop/ojdbc6.jar <sql_file_path> <input_unique_id_file_hdfs_location> <output_hdfs_location> <num_reduce_tasks>");
			System.exit(1);
		}

		try {
			Map<String, String> props = System.getenv();

			if (props.get("DB_CONNECTION_STRING") != null)
				conf.set("DB_CONNECTION_STRING", props.get("DB_CONNECTION_STRING"));
			else
				throw new Exception("Missing property DB_CONNECTION_STRING");

			if (props.get("DB_USERNAME") != null)
				conf.set("DB_USERNAME", props.get("DB_USERNAME"));
			else
				throw new Exception("Missing property DB_USERNAME");

			if (props.get("DB_PASSWORD") != null)
				conf.set("DB_PASSWORD", props.get("DB_PASSWORD"));
			else
				throw new Exception("Missing property DB_PASSWORD");

		} catch (Exception e) {
			logger.error("Cannot load RDBMS connection properties - " + e.getMessage(), e);
			System.exit(1);
		}

		logger.info("Starting Job");

		Job job = Job.getInstance(conf, "RDBMS Data Extract");
		job.setJarByClass(FreeFormQueryExtractor.class);
		String outputFilePath = null;
		String inputFilePath = null;
		String sqlFile = null;

		int numReduceTasks = 4;

		sqlFile = args[0];
		fs.copyFromLocalFile(false, true, new Path(sqlFile), new Path(HDFS_INPUT_SQL_FILE_PATH));
		inputFilePath = args[1];
		outputFilePath = args[2];
		if (args.length > 3 && args[3] != null && !args[3].equals("")) {
			numReduceTasks = Integer.parseInt(args[3]);
		}

		// The SQL file which contains the query to be executed by Reducer
		job.addFileToClassPath(new Path(HDFS_INPUT_SQL_FILE_PATH));
		// Identity Mapper is used.
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdDrivenExtractReducer.class);

		job.setNumReduceTasks(numReduceTasks);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);
		// Input should come from HDFS flat file (containing the list of unique IDs)
		FileInputFormat.addInputPath(job, new Path(inputFilePath));
		// Output also will be to HDFS file which will be later on imported to Hive
		FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}