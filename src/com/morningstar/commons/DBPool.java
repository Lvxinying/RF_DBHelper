package com.morningstar.commons;

import java.sql.Connection;   

import org.apache.log4j.Logger;
  
//import javax.sql.DataSource;  
  
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DBPool {

	/**C3P0 DBPool
	 * @author Stefan.Hou
	 */
	
	private static Logger log = Logger.getLogger(DBPool.class);
	
	private static ComboPooledDataSource ds;  
	  
    private static ThreadLocal<Connection> tl = new ThreadLocal<Connection>(); 

    public static ComboPooledDataSource getDataSource() {  
        return ds;  
    }  

//初始化DB的配置    
    private static void iniDbConfig(String DbName){
    	ds = new ComboPooledDataSource(DbName);
    }
    
    
    public static Connection getConnection(Database database){
    	Connection conn = null;
    	switch(database){
//SQL-SERVER
    	case GoldenDataQA:
    		conn = connectToGoldenData();
    		if(conn != null)
    		System.out.println("[WARN]Connect to GoldenData need to change password every two months!");
    		break;
//Vertica    		
    	case VerticaQA:
    		conn = connectToVerticaQA();
    		break;
    	case VerticaSTG:
    		conn = connectToVerticaSTG();
    		break;
    	case VerticaPROD:
    		conn = connectToVerticaPROD();
    		break;
//Netteza
    	case NetezzaQA:
    		conn = connectToNetezzaQA();
    		break;
    	case NetezzaSTG:
    		conn = connectToNetezzaSTG();
    		break;
    	case NetezzaPROD:
    		conn = connectToNetezzaPROD();
    		break;
    	}
    	if(conn != null){
    		
    	}
    	if(conn != null){
    		log.info("Connect to DB success!");
    	}else{
    		log.error("Getting trouble to connect to DB!");
    	}
    	return conn;
    }  
  
    public static void startTransaction() {  
        try {  
// 得到当前线程上绑定连接开启事务  
            Connection conn = tl.get();  
                conn = ds.getConnection();  
                tl.set(conn);  
//取消AutoCommit设置            
            conn.setAutoCommit(false);  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }  
 
//commit之前的所有SQL语句    
    public static void commitTransaction() {  
        try {  
            Connection conn = tl.get();    
                conn.commit();   
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }  
  
    public static Connection closeConnection() {         
    	try {  
            Connection conn = tl.get();    
                conn.close();   
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        } finally {  
// 千万注意，解除当前线程上绑定的链接（从threadlocal容器中移除对应当前线程的链接）           
        	tl.remove();   
        }
		return null;
    }
    
    
    
//连接数据库
//MsSQL    
    private static Connection connectToGoldenData(){
        Connection connGoldenData = tl.get();  
    	try {
    			iniDbConfig("GoldenData");
    			connGoldenData = ds.getConnection();  
                tl.set(connGoldenData);  
            return connGoldenData;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToMsSQL2(){
        Connection connMsSQL2 = tl.get();  
    	try {
    			iniDbConfig("MsSql2");   
            	connMsSQL2 = ds.getConnection();  
                tl.set(connMsSQL2);   
            return connMsSQL2;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToMsSQL3(){
        Connection connMsSQL3 = tl.get();  
    	try {
    			iniDbConfig("MsSql3");    
            	connMsSQL3 = ds.getConnection();  
                tl.set(connMsSQL3);   
            return connMsSQL3;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToMsSQL4(){
        Connection connMsSQL4 = tl.get();  
    	try {
    			iniDbConfig("MsSql4");    
            	connMsSQL4 = ds.getConnection();  
                tl.set(connMsSQL4);  
            return connMsSQL4;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToMsSQL5(){
        Connection connMsSQL5 = tl.get();  
    	try {
    			iniDbConfig("MsSql5");   
            	connMsSQL5 = ds.getConnection();  
                tl.set(connMsSQL5);    
            return connMsSQL5;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
//MySQL
    private static Connection connectToMySQL1(){
        Connection connMySQL1 = tl.get();
    	try {
    			iniDbConfig("MySQL1");
            	connMySQL1 = ds.getConnection();  
                tl.set(connMySQL1);   
            return connMySQL1;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToMySQL2(){
        Connection connMySQL2 = tl.get();
    	try {
    			iniDbConfig("MySQL2");  
            	connMySQL2 = ds.getConnection();  
                tl.set(connMySQL2);  
            return connMySQL2;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToMySQL3(){ 
        Connection connMySQL3 = tl.get();
    	try {
    			iniDbConfig("MySQL3"); 
            	connMySQL3 = ds.getConnection();  
                tl.set(connMySQL3);   
            return connMySQL3;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
//Vertica
    private static Connection connectToVerticaQA(){ 
        Connection connVerticaQA = tl.get();
    	try {  
    			iniDbConfig("VerticaQA");
    			connVerticaQA = ds.getConnection();  
                tl.set(connVerticaQA);   
            return connVerticaQA;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToVerticaSTG(){ 
        Connection connVerticaSTG = tl.get();
    	try {  
    			iniDbConfig("VerticaSTG");
    			connVerticaSTG = ds.getConnection();  
                tl.set(connVerticaSTG);   
            return connVerticaSTG;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToVerticaPROD(){
        Connection connVerticaPROD = tl.get();
    	try {  
    			iniDbConfig("VerticaPROD");  
    			connVerticaPROD = ds.getConnection();  
                tl.set(connVerticaPROD);   
            return connVerticaPROD;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }  
    }
    
    private static Connection connectToNetezzaQA(){
        Connection connNetezzaQA = tl.get();
    	try {  
    			iniDbConfig("NetezzaQA");  
    			connNetezzaQA = ds.getConnection();  
                tl.set(connNetezzaQA);   
            return connNetezzaQA;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }    	
    }
    
    private static Connection connectToNetezzaSTG(){
        Connection connNetezzaSTG = tl.get();
    	try {  
    			iniDbConfig("NetezzaSTG");  
    			connNetezzaSTG = ds.getConnection();  
                tl.set(connNetezzaSTG);   
            return connNetezzaSTG;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }    	
    }
    
    private static Connection connectToNetezzaPROD(){
        Connection connNetezzaPROD = tl.get();
    	try {  
    			iniDbConfig("NetezzaPROD");  
    			connNetezzaPROD = ds.getConnection();  
                tl.set(connNetezzaPROD);   
            return connNetezzaPROD;  
        } catch (Exception e) {  
            throw new RuntimeException(e);  
        }    	
    }
}
