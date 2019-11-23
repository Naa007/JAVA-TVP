import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

public class TableValuedParameterExample {
  
	// tvp properties 
    private boolean isTVP = true;
    private String TABLE_TVP;
    private SQLServerDataTable sourceDataTable;
    private SQLServerPreparedStatement sqlPstmt_tvp = null;
  
    Connection conn = null;
    private PreparedStatement sqlPstmt = null;
    int numberOfRecords = 100000;
    int batchSize = 500;
    int counter = 0;
   // DATABASE TABLE
  /*  CREATE TABLE TVPEXAMPLE(
    		[name] [nchar](10) NULL,
    		[age] [numeric](2, 0) NULL,
    		[sex] [nchar](10) NULL,
    		[address] [nchar](10) NULL,
    		[pin] [numeric](7, 0) NULL
    	) ON [PRIMARY]
    	GO */
    
    /**
     * Gets the SQL insert statement
     *
     * @return SQL insert statement
     */
    private String getInsertStmt()
    {
        String insertStmt = "INSERT INTO TVPEXAMPLE (name,age,sex,address,pin) values ( ?, ?, ?, ?, ?);set  nocount on ";
        System.out.println("insert statement: " + insertStmt);
        return insertStmt;
    }
    /**
     * Gets the SQL TVP insert statement 
     *
     * @return      SQL insert statement
     */
    private String getInsertStmtTVP()
    {	// TVP Batching step 2
    	this.TABLE_TVP = "TVPEXAMPLE_TVP";
        String insertStmt = "INSERT INTO TVPEXAMPLE SELECT * FROM  ?; set nocount on";
        System.out.println("TVP insert statement: " + insertStmt);
        return insertStmt;
    }
    
    public void prepareInsert() throws SQLException {
    	if(isTVP) {
    		this.sqlPstmt_tvp = (SQLServerPreparedStatement) this.conn.prepareStatement(getInsertStmtTVP());
    		initTVP();
    	}else {
    		this.sqlPstmt = this.conn.prepareStatement(getInsertStmt());
    	}
    }
    
    public Connection getConnection() {
    	try {
    		 Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    		 // set your connection props
//			 conn = DriverManager.getConnection("url","username", "password");
			 // step 1
			 conn.setAutoCommit(false);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		return conn;
    }
   
    public void publishAction() throws SQLException {
    	long startTime = System.currentTimeMillis();
    	prepareInsert();
    	if(isTVP) {
    		// filling statically, in real time we need to iterate the data and create Object array and pass it to
    		// sourceDataTable
    		Object[] list = {"XYZ","20","male", "HYD", "500072"};
    		while(counter < numberOfRecords  ) {
    			counter++;
    			// TVP Batching step 5
				sourceDataTable.addRow(list);
    			if(counter % batchSize == 0) {
    				execute();
    			}
    		}
    		
    	} else {
    		
    		while(counter < numberOfRecords ) {
    			this.sqlPstmt.setString(1, "XYZ");
    			this.sqlPstmt.setInt(2, 20);
    			this.sqlPstmt.setString(3, "male");
    			this.sqlPstmt.setString(4, "HYD");
    			this.sqlPstmt.setInt(5, 50072);
    			
    			// JDBC Batching step 2
    			this.sqlPstmt.addBatch();
    			
    			counter++;
				if(counter % batchSize == 0) {
    				execute();
    			}
    		}
    	}
    	if (counter % batchSize !=0) {
    		executeLeftOver();
    	}
    	
    	long endTime = System.currentTimeMillis();
    	if(isTVP) {
    		System.out.println("Time Taken for TVP execution : " + (endTime - startTime) + " (ms)");
    	} else {
    		System.out.println("Time Taken for Batch execution : " + (endTime - startTime) + " (ms)");
    	}
    }

    /**
	 * This method is used to execute the TVP object into DB
	 * @throws SQLServerException
	 * @throws SQLTimeoutException
	 */
	public void execute() throws SQLTimeoutException {
		try {
			if(isTVP) {
				this.sqlPstmt_tvp.setStructured(1, TABLE_TVP, sourceDataTable);
				this.sqlPstmt_tvp.execute();
				// you need to clear source data table after execution and reinitialize
				sourceDataTable.clear();
				//System.out.println("TVP BatchExcution : " + counter / batchSize);
				initTVP();
			} else {
				// JDBC Batching step 3
				this.sqlPstmt.executeBatch();
				// JDBC Batching step 4
				this.sqlPstmt.clearBatch();
				//System.out.println("JDBC BatchExcution : " + counter / batchSize);
			}
			this.conn.commit();
		} catch (SQLException sqlException) {
			System.err.println(sqlException.getMessage());
		}
	}
	
	  /**
		 * This method is used to execute the TVP object into DB
		 * @throws SQLServerException
		 * @throws SQLTimeoutException
		 */
		public void executeLeftOver() throws SQLTimeoutException {
			try {
				if(isTVP) {
					// TVP Batching step 6
					this.sqlPstmt_tvp.setStructured(1, TABLE_TVP, sourceDataTable);
					// TVP Batching step 7
					this.sqlPstmt_tvp.execute();
					// you need to clear source data table after execution and reinitialize
					sourceDataTable.clear();
					initTVP();
					//System.out.println("TVP LeftOver Execution");
				} else {
					this.sqlPstmt.executeBatch();
					this.sqlPstmt.clearBatch();
					//System.out.println("JDBC LeftOver Execution");
				}
				this.conn.commit();
			} catch (SQLException sqlException) {
				System.err.println(sqlException.getMessage());
			}
		}

	public void closedown() throws SQLException {
		if(this.sqlPstmt_tvp != null) {
			this.sqlPstmt_tvp.close();
			this.sqlPstmt_tvp = null;
		}
		if(this.sqlPstmt != null) {
			this.sqlPstmt.close();
			this.sqlPstmt = null;
		}
	}
	
	public static void main(String[] args) {
		TableValuedParameterExample tvp = new TableValuedParameterExample();
		try {
			tvp.getConnection();
			tvp.publishAction();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * This method is used to create equivalent table object in java for TVP 
	 */
	private void initTVP() {
		
		try {
			// TVP Batching step 3
			// create an EQUALENT TVP OBJECT of TABLE in Database
			/* CREATE TYPE TVPEXAMPLE_TVP as TABLE(
    		    		[name] [nchar](10) NULL,
    		    		[age] [numeric](2, 0) NULL,
    		    		[sex] [nchar](10) NULL,
    		    		[address] [nchar](10) NULL,
    		    		[pin] [numeric](7, 0) NULL
    		    	) */
			
			// TVP Batching step 4
			// We need to maintain column orders same 
			sourceDataTable = new SQLServerDataTable();
			
			sourceDataTable.addColumnMetadata("name", java.sql.Types.NVARCHAR);
			sourceDataTable.addColumnMetadata("age", java.sql.Types.NUMERIC);
			sourceDataTable.addColumnMetadata("sex", java.sql.Types.NVARCHAR);
			sourceDataTable.addColumnMetadata("address", java.sql.Types.NVARCHAR);
			sourceDataTable.addColumnMetadata("pin", java.sql.Types.NUMERIC);
			
		}catch(SQLException e) {
			System.err.println(e.getMessage());
		}
	}
}

