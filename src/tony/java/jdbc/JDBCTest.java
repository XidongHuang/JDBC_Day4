package tony.java.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLClientInfoException;

import org.junit.Test;
import org.omg.CORBA.Current;

import com.mysql.jdbc.interceptors.ServerStatusDiffInterceptor;

import java.sql.PreparedStatement;

import java.sql.Statement;

public class JDBCTest {
	static int count = 0;
	@Test
	public void testBatch(){
		
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		String sql = null;
		
		
		try {
			
			connection = JDBCTools.getConnection();
			JDBCTools.beginTx(connection);
			
			sql = "INSERT INTO customers VALUES(?,?,?)";
			preparedStatement = connection.prepareStatement(sql);
			
			long begin = System.currentTimeMillis();
			for(int i = 0; i<100000;i++){
				preparedStatement.setInt(1, i+1);
				preparedStatement.setString(2, "name_"+i);
				preparedStatement.setString(3, "location"+i);
				count++;
				System.out.println(count);
				//"积攒" SQL 
				preparedStatement.addBatch();
				
				//当"积攒" 到一定程度，就统一的执行一次. 并且清空先前"积攒" 的SQL
				if((i+1) %50000 == 0){
					preparedStatement.executeBatch();
					preparedStatement.clearBatch();
				}

				
			}
			
			//若总条数不是批量数值的整数倍，则还需要在额外地执行一次
			if(10000 % 300 != 0){
				preparedStatement.executeBatch();
				preparedStatement.clearBatch();
			}
			
			
			long end = System.currentTimeMillis();
			System.out.println("Time: " + (end - begin));//159898
			JDBCTools.commit(connection);
		} catch(Exception e){
			e.printStackTrace();
			JDBCTools.roolback(connection);
		} finally {
			// TODO: handle finally clause
			JDBCTools.releaseDB(null, preparedStatement, connection);
		
		}
		
		
		
		
	}
	
	
	
	@Test
	public void testBatchWithPreparedStatement() {
//		static int count = 0;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		String sql = null;
		
		
		try {
			
			connection = JDBCTools.getConnection();
			JDBCTools.beginTx(connection);
			sql = "INSERT INTO customers VALUES(?,?,?)";
			preparedStatement = connection.prepareStatement(sql);
			
			long begin = System.currentTimeMillis();
			for(int i = 0; i<1000;i++){
				preparedStatement.setInt(1, i+1);
				preparedStatement.setString(2, "name_"+i);
				preparedStatement.setString(3, "location"+i);
				System.out.println(sql);
				preparedStatement.executeUpdate();
			}
			long end = System.currentTimeMillis();
			System.out.println("Time: " + (end - begin));//159898
			JDBCTools.commit(connection);
		} catch(Exception e){
			e.printStackTrace();
			JDBCTools.roolback(connection);
		} finally {
			// TODO: handle finally clause
			JDBCTools.releaseDB(null, preparedStatement, connection);
		
		}
		
		
		
	}
	
	
	
	
	/**
	 * 向Oracle 的Customer 数据表中插入10万条记录
	 * 测试如何插入用时最短
	 * 1. 使用Statement 	 
	 */
	
	@Test
	public void testBatchWithStatement() {
		
		Connection connection = null;
		Statement statement = null;
		String sql = null;
		
		
		try {
			
			connection = JDBCTools.getConnection();
			JDBCTools.beginTx(connection);
			statement = connection.createStatement();
			
			long begin = System.currentTimeMillis();
			for(int i = 0; i<1000;i++){
				sql = "INSERT INTO customers VALUES("+(i+1)+",'name_"+i+"','location_"+i + "');";
				System.out.println(sql);
				statement.executeUpdate(sql);
				
			}
			long end = System.currentTimeMillis();
			System.out.println("Time: " + (end - begin));//79257
			JDBCTools.commit(connection);
		} catch(Exception e){
			e.printStackTrace();
			JDBCTools.roolback(connection);
		} finally {
			// TODO: handle finally clause
			JDBCTools.releaseDB(null, statement, connection);
		
		}
		
		
		
	}

}
