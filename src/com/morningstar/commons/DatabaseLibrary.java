package com.morningstar.commons;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.robotframework.javalib.annotation.ArgumentNames;
import org.robotframework.javalib.annotation.RobotKeyword;
import org.robotframework.javalib.annotation.RobotKeywords;


@RobotKeywords
public class DatabaseLibrary
{
	public static final String ROBOT_LIBRARY_SCOPE = "GLOBAL";
	public static final String ROBOT_LIBRARY_VERSION = "0.0.1";
	private Connection connection = null;
	@RobotKeyword()
	@ArgumentNames({"DbType","Env"})
	public void connectToDatabase(String DbType,String Env)
			throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
			{
		if(DbType.equalsIgnoreCase("GoldenData")){
			switch(Env){
			case "Dev":
				setConnection(DBPool.getConnection(Database.GoldenDataQA));
				break;
			}
		}
		if(DbType.equalsIgnoreCase("vertica")){
			switch(Env){
			case "Dev":
				setConnection(DBPool.getConnection(Database.VerticaQA));
				break;
			case "Staging":
				setConnection(DBPool.getConnection(Database.VerticaSTG));
				break;
			case "Production":
				setConnection(DBPool.getConnection(Database.VerticaPROD));
				break;
			}
			if(this.connection == null){
				System.out.println("Your current enviroment choosing is: "+Env+" Please type 'Dev','Staging','Production' instead!");
			}
		}
		if(DbType.equalsIgnoreCase("Netezza")){
			switch(Env){
			case "Dev":
				setConnection(DBPool.getConnection(Database.NetezzaQA));
				break;
			case "Staging":
				setConnection(DBPool.getConnection(Database.NetezzaSTG));
				break;
			case "Production":
				setConnection(DBPool.getConnection(Database.NetezzaPROD));
				break;
			}
			if(this.connection == null){
				System.out.println("Your current enviroment choosing is: "+Env+" Please type 'Dev','Staging','Production' instead!");
			}
		}
		
		if(this.connection == null){
			System.out.println("[ERROR]Connect to DB meet trouble, message: "+new SQLException().getMessage());
		}
			}

	@RobotKeyword()
	public void disconnectFromDatabase() throws SQLException
	{
		getConnection().close();
		if(!this.connection.isClosed()){
			System.out.println("[ERROR]Close connection meet trouble: " + new SQLException().getMessage());
		}else{
			System.out.println("[INFO]DB connection has closed successfully!");
		}
	}

	@RobotKeyword()
	@ArgumentNames({"tableName"})
	public void tableMustExist(String tableName) throws SQLException, DatabaseLibraryException
	{
		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getTables(null, null, tableName, null);
		try {
			if (!rs.next()) {
				throw new DatabaseLibraryException("The table (" + tableName + 
						") was not found!");
			}
		} finally {
			rs.close(); 
		} 
		rs.close();
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName"})
	public void tableMustNotBeExist(String tableName) throws SQLException, DatabaseLibraryException
	{
		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getTables(null, null, tableName, null);
		try {
			if (rs.next()) {
				throw new DatabaseLibraryException("The table (" + tableName + 
						") can be found,but we expected it disappear!");
			}
		} finally {
			rs.close(); 
		} 
		rs.close();
	}

	@RobotKeyword
	@ArgumentNames({"tableName"})
	public void tableMustBeEmpty(String tableName) throws SQLException, DatabaseLibraryException
	{
		tableMustContainNumberOfRows(tableName, "0");
	}
	
	@RobotKeyword
	@ArgumentNames({"tableName"})
	public void tableMustNotBeEmpty(String tableName) throws SQLException, DatabaseLibraryException
	{
		long num = getNumberOfRows(tableName);
		if(num == 0){
			throw new DatabaseLibraryException("Tested table: " +tableName+" is empty, but we expected it isn't empty!");
		}
	}

	@RobotKeyword
	@ArgumentNames({"tableName"})
	public void deleteAllRowsFromTable(String tableName) throws SQLException
	{
		String sql = "DELETE FROM " + tableName;	    
		Statement stmt;
		stmt = getConnection().createStatement();
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			System.out.println("[ERROR]Delete all rows in table ("+ tableName + ") meet trouble, error message" + e.getMessage());
		} finally {
			stmt.close();
		}
	}

	@RobotKeyword
	@ArgumentNames({"tableName","expectRowNumValue"})
	public void tableMustContainNumberOfRows(String tableName, String expectRowNumValue) throws SQLException, DatabaseLibraryException
	{
		long rowNum = Long.valueOf(expectRowNumValue).longValue();	    
		long num = getNumberOfRows(tableName);
		if (num != rowNum) {
			throw new DatabaseLibraryException("Expecting " + rowNum + 
					" rows, but fetched: " + num);
		}
	}

	@RobotKeyword()
	@ArgumentNames({"tableName","expectRowNumValue"})
	public void tableMustContainMoreThanNumberOfRows(String tableName, String expectRowNumValue) throws SQLException, DatabaseLibraryException
	{
		long rowNum = Long.valueOf(expectRowNumValue).longValue();
		long num = getNumberOfRows(tableName);
		if (num <= rowNum) {
			throw new DatabaseLibraryException("Expecting more than" + rowNum + 
					" rows, but fetched: " + num);
		}
	}

	@RobotKeyword()
	@ArgumentNames({"tableName","expectRowNumValue"})
	public void tableMustContainLessThanNumberOfRows(String tableName, String expectRowNumValue) throws SQLException, DatabaseLibraryException
	{
		long rowNum = Long.valueOf(expectRowNumValue).longValue();
		long num = getNumberOfRows(tableName);
		if (num >= rowNum) {
			throw new DatabaseLibraryException("Expecting less than" + rowNum + 
					" rows, but fetched: " + num);
		}
	}
	
	@RobotKeyword()
	@ArgumentNames({"firstTableName","secondTableName"})
	public void tablesMustContainSameAmountOfRows(String firstTableName, String secondTableName) throws SQLException, DatabaseLibraryException
	{
		long firstNum = getNumberOfRows(firstTableName);
		long secondNum = getNumberOfRows(secondTableName);
		if (firstNum != secondNum) {
			throw new DatabaseLibraryException(
					"Expecting same amount of rows, but table " + 
							firstTableName + " has " + firstNum + 
							" rows while table " + secondTableName + " has " + 
							secondNum + " rows!");
		}
	}
		
	@RobotKeyword()
	@ArgumentNames({"columnNames", "tableName", "expectedValues" ,"rowNumValue","whereClause"})
	public void checkContentForRowIdentifiedByRownumAndWhereClause(String columnNames, String tableName, String expectedValues, String rowNumValue ,String whereClause) throws SQLException, DatabaseLibraryException
	{
		int rowNum = Integer.valueOf(rowNumValue).intValue(); 
		String sqlString = "select " + columnNames + " from " + tableName + 
				" where " + whereClause;

		String[] columns = columnNames.split(",");
		String[] values = expectedValues.split("\\|");

		Statement stmt = getConnection().createStatement();
		try {
			ResultSet rs = stmt.executeQuery(sqlString);
			int count = 0;
			while (rs.next()) {
				count ++;
				if (count == rowNum)
				{
					for(int i = 0; i < columns.length; i++) {
						String fieldValue = rs.getString(columns[i]);
						System.out.println(columns[i] + " -> " + fieldValue);
						if (values[i].isEmpty()) {
							values[i] = "";
						}
						if (!fieldValue.equals(values[i])) {
							throw new DatabaseLibraryException("Value matching failed, actual value in DB: '" + 
									fieldValue + "'. Expected: '" + values[i] + 
									"'");
						}
					}
				}
			}
			if (count == 0) {
				throw new DatabaseLibraryException(
						"No row fetched by given where-clause for statement,current SQL is: " + 
								sqlString);
			}
			if (count != rowNum) {
				throw new DatabaseLibraryException(
						"Given rownum is out of range in DB side actually,please re-type a new static row number,current SQL is: " + sqlString);
			}
		}
		finally
		{
			stmt.close(); 
		} 
		stmt.close();
	}
		
	@RobotKeyword()
	@ArgumentNames({"columnNames", "tableName", "expectedValues" ,"rowNumValue","whereClause"})
	public String readSingleValueFromTableDefaultSort(String tableName, String columnName, String whereClause) throws SQLException, DatabaseLibraryException
			{
		ArrayList<String> list = new ArrayList<String>();
		String fieldStr;
		String ret = "";
		String sql = "select " + columnName + " from " + tableName + " where " + whereClause;
		if(columnName.contains(",")){
			throw new DatabaseLibraryException("This function only support only one column in SQL!");
		}
//		Statement stmt = getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		Statement stmt = getConnection().createStatement();
		try {
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next()){
				fieldStr = rs.getString(columnName);
				list.add(fieldStr);
			}
			rs.close();
			if(list.size()==0){
				throw new DatabaseLibraryException("No data fetched after executing SQL: "+sql);
			}
			if(list.size() == 1){
				ret = list.get(0);
			}
			if(list.size() > 1){
				ret = list.get(0);
			}
		}
		finally {
			stmt.close();
		}
		return ret;
	}

	@RobotKeyword()
	@ArgumentNames({"levelName"})
	public void transactionIsolationLevelMustBe(String levelName) throws SQLException, DatabaseLibraryException
	{
		String transactionName = getTransactionIsolationLevel();
		if (!transactionName.equals(levelName)) {
			throw new DatabaseLibraryException(
					"Expected Transaction Isolation Level: " + levelName + 
					" Level found: " + transactionName);
		}
	}
	
	@RobotKeyword()
	public String getTransactionIsolationLevel() throws SQLException
	{
		String ret = "";
		int transactionIsolation = getConnection().getTransactionIsolation();
		switch (transactionIsolation)
		{
		case 0: 
			ret = "TRANSACTION_NONE";
			break;

		case 1: 
			ret = "TRANSACTION_READ_UNCOMMITTED";
			break;
			
		case 2: 
			ret = "TRANSACTION_READ_COMMITTED";
			break;

		case 4: 
			ret = "TRANSACTION_REPEATABLE_READ";
			break;

		case 8: 
			ret = "TRANSACTION_SERIALIZABLE";
		}
		return ret;
	}

	@RobotKeyword()
	@ArgumentNames({"tableName","expectedKey"})
	public void checkPrimaryKeyColumnsForTable(String tableName, String expectedKey) throws SQLException, DatabaseLibraryException
	{
		String key = getPrimaryKeyColumnsForTable(tableName).toLowerCase();
		expectedKey = expectedKey.toLowerCase();
		if (!expectedKey.equals(key)) {
			throw new DatabaseLibraryException("Expected promary key is: " + 
					expectedKey + " but actual Key is: " + key);
		}
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName"})
	public String getPrimaryKeyColumnsForTable(String tableName) throws SQLException
	{
		String ret = "";
		String primaryKeyColumnName;
		int primaryKeySequenceNumber;
		Map<Integer,String> map = new HashMap<Integer,String>();
		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getPrimaryKeys(null, null, tableName);
		while(rs.next()){
			primaryKeySequenceNumber = rs.getInt("KEY_SEQ");
			primaryKeyColumnName = rs.getString("COLUMN_NAME");
//过滤重复的KEY_SEQ			
			map.put(primaryKeySequenceNumber, primaryKeyColumnName);
		}
		if(map.isEmpty()){
			ret = "NULL";
		}else{
			for(Entry<Integer,String> entry:map.entrySet()){
				ret = ret + entry.getValue().toString()+",";
			}
		}		
		if(rs != null){
			rs.close();
		}
		if (ret.length() > 0 && !ret.equals("NULL")) {
			ret = ret.substring(0, ret.length() - 1);
		}
		return ret;
	}
	
	@RobotKeyword()
	@ArgumentNames({"sqlString"})
	public void executeSql(String sqlString) throws SQLException
	{
		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sqlString);
		} finally {
			stmt.close();
		}
	}
	
	@RobotKeyword()
	@ArgumentNames({"fileName"})
	public void executeSqlFromFile(String fileName) throws SQLException, DatabaseLibraryException, IOException
	{
		getConnection().setAutoCommit(false);
		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);
		String sql = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			if (!line.toLowerCase().startsWith("rem"))
			{
				if (!line.startsWith("#"))
				{
					sql = sql + line;
				} 
			}
		}
		try
		{
			if (sql.endsWith(";")) {
				sql = sql.substring(0, sql.length() - 1);
				System.out.println("Executing: " + sql);
				executeSql(sql);
			}else{
				System.out.println("Executing: " + sql);
				executeSql(sql);
			}
		} catch (SQLException e) {
			System.out.println("ERROR MESSAGE: " + e.getMessage());
			sql = "";
			br.close();
			getConnection().rollback();
			getConnection().setAutoCommit(true);
			throw new DatabaseLibraryException("Error executing: " + sql + 
					" Execution from file rolled back!");
		}finally{
			getConnection().commit();
			getConnection().setAutoCommit(true);
			br.close();
		}		
	}

	@RobotKeyword()
	@ArgumentNames({"fileName"})
	public void executeSqlFromFileIgnoreErrors(String fileName) throws SQLException, IOException, DatabaseLibraryException
	{
		getConnection().setAutoCommit(false);
		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);

		String sql = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			if (!line.toLowerCase().startsWith("rem"))
			{
				if (!line.startsWith("#"))
				{
					sql = sql + line;
				} 
			}
		}
		try
		{
			if (sql.endsWith(";")) {
				sql = sql.substring(0, sql.length() - 1);
				System.out.println("Executing: " + sql);
				executeSql(sql);
			}else{
				System.out.println("Executing: " + sql);
				executeSql(sql);
			}
		} catch (SQLException e) {
			System.out.println("ERROR MESSAGE: "+e.getMessage());
			System.out.println("Error executing: " + sql);
			sql = "";
		}finally{
			getConnection().commit();
			getConnection().setAutoCommit(true);
			br.close();
		}		
	}

	
	public void verifyNumberOfRowsMatchingWhereClause(String tableName, String expectedRowNumValue , String where) throws SQLException, DatabaseLibraryException
	{
		long rowNum = Long.valueOf(expectedRowNumValue).longValue();
		long num = getNumberOfRows(tableName, where);
		if (num != rowNum) {
			throw new DatabaseLibraryException("Expecting row number is: " + rowNum + 
					" rows, while fetched: " + num);
		}
	}

	public void rowShouldNotExistInTable(String tableName, String whereClause)
			throws SQLException, DatabaseLibraryException
			{
		String sql = "select * from " + tableName + " where " + whereClause;
		Statement stmt = getConnection().createStatement();
		try {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				throw new DatabaseLibraryException("Row exists (but should not) for where-clause: " + 
						whereClause + " in table: " + tableName);
			}
		}
		finally {
			stmt.close(); } stmt.close();
			}

	@RobotKeyword()
	@ArgumentNames({"sqlString", "fileName","separator"})
	public void storeSqlQueryResultToFile(String sqlString, String fileName, String separator)
			throws SQLException, IOException
	{
		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sqlString);
			ResultSet rs = stmt.getResultSet();
			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			FileWriter fstream = new FileWriter(fileName);
			BufferedWriter writer = new BufferedWriter(fstream);
			while (rs.next()) {
				for (int i = 1; i <= numberOfColumns; i++) {
					rs.getString(i);
					writer.write(rs.getString(i) + separator.trim());
				}
				writer.write("\n");
			}
			writer.close();
			System.out.println("DB response data writing finished,file generated in: "+fileName);
		}
		finally {
			stmt.close();
		}
	}

	@RobotKeyword()
	@ArgumentNames({"sqlString", "fileName","separator"})
	public void compareSqlQueryResultWithFile(String sqlString, String fileName, String separator)
			throws SQLException, DatabaseLibraryException, FileNotFoundException
			{
		Statement stmt = getConnection().createStatement();
		int numDiffs = 0;
		int maxDiffs = 10;
		String diffs = "";
		try {
			stmt.execute(sqlString);
			ResultSet rs = stmt.getResultSet();
			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			FileReader fr = new FileReader(fileName);
			BufferedReader br = new BufferedReader(fr);

			int row = 0;
			while ((rs.next()) && (numDiffs < maxDiffs)) {
				String actRow = "";
				row++;
				for (int i = 1; i <= numberOfColumns; i++) {
					actRow = actRow + rs.getString(i) + separator;
				}
				String expRow = br.readLine();
				if (!actRow.equals(expRow)) {
					numDiffs++;
					diffs = diffs + "Row " + row + " does not match:\nexp: " + expRow + "\nact: " + actRow + "\n";
				}
			}
			if ((br.ready()) && (numDiffs < maxDiffs)) {
				numDiffs++;
				diffs = diffs + "More rows in expected file than in query result\n";
			}
			br.close();
		} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			numDiffs++;
			diffs = diffs + "Fewer rows in expected file than in query result\n";

			stmt.close();
			if (numDiffs > 0) throw new DatabaseLibraryException(diffs);
		}
		finally{
			stmt.close();
			if (numDiffs > 0) 
			{ 
				throw new DatabaseLibraryException(diffs);
		    }
		}
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName"})
	public int howManyColumnsInThisTable(String tableName) throws SQLException{
		int columnCount = 0;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		columnCount = rs.getMetaData().getColumnCount();
		rs.close();
		pstmt.close();
		return columnCount;
	}

	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	
	public String iWantToKnowTheColumnNameOfGivenColumnIndexNumber(String tableName,int columnNumber) throws SQLException{
		String columnName = null;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		columnName = rs.getMetaData().getColumnLabel(columnNumber);
		rs.close();
		pstmt.close();
		return columnName;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public String iWantToKnowTheColumnDataTypeNameOfGivenColumnIndexNumber(String tableName,int columnNumber) throws SQLException{
		String dataTypeName = null;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		dataTypeName = rs.getMetaData().getColumnTypeName(columnNumber);
		rs.close();
		pstmt.close();
		return dataTypeName;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public int iWantToKnowTheColumnDisplaySizeOfGivenColumnIndexNumber(String tableName,int columnNumber) throws SQLException{
		int columnDisplaySize = 0;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		columnDisplaySize = rs.getMetaData().getColumnDisplaySize(columnNumber);
		rs.close();
		pstmt.close();
		return columnDisplaySize;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsNullable(String tableName,int columnNumber) throws SQLException{
		boolean isNullable;
		int result;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		result = rs.getMetaData().isNullable(columnNumber);
		rs.close();
		pstmt.close();
		if(result == 1){
			isNullable = true;
		}else{
			isNullable = false;
		}
		return isNullable;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsAutoIncrement(String tableName,int columnNumber) throws SQLException{
		boolean isAutoIncrement;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		isAutoIncrement = rs.getMetaData().isAutoIncrement(columnNumber);
		rs.close();
		pstmt.close();
		return isAutoIncrement;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsCaseSensitive(String tableName,int columnNumber) throws SQLException{
		boolean isCaseSensitive;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		isCaseSensitive = rs.getMetaData().isCaseSensitive(columnNumber);
		rs.close();
		pstmt.close();
		return isCaseSensitive;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsCurrency(String tableName,int columnNumber) throws SQLException{
		boolean isCurrency;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		isCurrency = rs.getMetaData().isCurrency(columnNumber);
		rs.close();
		pstmt.close();
		return isCurrency;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsReadOnly(String tableName,int columnNumber) throws SQLException{
		boolean isReadOnly;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		isReadOnly = rs.getMetaData().isReadOnly(columnNumber);
		rs.close();
		pstmt.close();
		return isReadOnly;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsSearchable(String tableName,int columnNumber) throws SQLException{
		boolean isSearchable;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		isSearchable = rs.getMetaData().isSearchable(columnNumber);
		rs.close();
		pstmt.close();
		return isSearchable;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsSigned(String tableName,int columnNumber) throws SQLException{
		boolean isSigned;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		isSigned = rs.getMetaData().isSigned(columnNumber);
		rs.close();
		pstmt.close();
		return isSigned;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsWritable(String tableName,int columnNumber) throws SQLException{
		boolean isWritable;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		isWritable = rs.getMetaData().isWritable(columnNumber);
		rs.close();
		pstmt.close();
		return isWritable;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnNumber"})
	public boolean checkingIfThisColumnInTableIsDefinitelyWritable(String tableName,int columnNumber) throws SQLException{
		boolean isDefinitelyWritable;
		String sql = "SELECT * FROM " + tableName +" LIMIT 1";
		PreparedStatement pstmt = getConnection().prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();
		isDefinitelyWritable = rs.getMetaData().isDefinitelyWritable(columnNumber);
		rs.close();
		pstmt.close();
		return isDefinitelyWritable;
	}
	
	@RobotKeyword()
	@ArgumentNames({"sqlString"})
	public String returnTheResultAfterExecutingSql(String sqlString) throws SQLException{
		String result = null;
		Statement stmt = getConnection().createStatement();
		try {
			ResultSet rs = stmt.executeQuery(sqlString);
			int columnIndex = 1;
			while(rs.next()){
				result = rs.getString(columnIndex);
				columnIndex++;
			}
			rs.close();
		} finally {
			stmt.close();
		}
		return result;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnName","limitCount"})
	public List<String> returnTheResultListAfterExecutingSqlNoWhereClause(String tableName,String columnName,int limitCount) throws SQLException{
		List<String> resultList = new ArrayList<String>();
		Statement stmt = getConnection().createStatement();
		String sqlString = null;
		if(limitCount == 0)
		{
			sqlString = "SELECT " + columnName + " FROM " + tableName;
		}else{
			sqlString = "SELECT " + columnName + " FROM " + tableName + " LIMIT "+limitCount;
		}
		try {
			ResultSet rs = stmt.executeQuery(sqlString);
			while(rs.next()){
				int size = rs.getMetaData().getColumnCount();
				for (int i = 1; i <= size; i++) {
					resultList.add("".equals(rs.getString(i)) ? null : rs.getString(i));
				}
			}
			rs.close();
		} finally {
			stmt.close();
		}
		return resultList;
	}
	
	@RobotKeyword()
	@ArgumentNames({"tableName","columnName","whereClause","limitCount"})
	public List<String> returnTheResultListAfterExecutingSqlWhereClause(String tableName,String columnName,String whereClause,int limitCount) throws SQLException{
		List<String> resultList = new ArrayList<String>();
		Statement stmt = getConnection().createStatement();
		String sqlString = null;
		if(limitCount == 0)
		{
			sqlString = "SELECT " + columnName + " FROM " + tableName + "WHERE " + whereClause;
		}else{
			sqlString = "SELECT " + columnName + " FROM " + tableName + "WHERE " + whereClause + " LIMIT "+limitCount;
		}
		try {
			ResultSet rs = stmt.executeQuery(sqlString);
			while(rs.next()){
				int size = rs.getMetaData().getColumnCount();
				for (int i = 1; i <= size; i++) {
					resultList.add("".equals(rs.getString(i)) ? null : rs.getString(i));
				}
			}
			rs.close();
		} finally {
			stmt.close();
		}
		return resultList;
	}
	
	private void setConnection(Connection connection)
	{
		this.connection = connection;
	}

	private Connection getConnection() {
		if (this.connection == null) {
			throw new IllegalStateException("No connection open. Did you forget to run 'Connect To Database' before?");
		}
		return this.connection;
	}

	private long getNumberOfRows(String tableName) throws SQLException {
		return getNumberOfRows(tableName, null);
	}
	
	private long getNumberOfRows(String tableName, String whereClause) throws SQLException{
		long num = 0;
		String sql = "select count(*) from " + tableName;
		if (whereClause != null) {
			sql = sql + " where " + whereClause;
		}
		Statement stmt = getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		if(rs.next()){
			String numStr = rs.getString(1);
			num = Long.valueOf(numStr);
		}
		return num;		
	}
}