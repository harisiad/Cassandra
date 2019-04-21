/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ConnectionManager;

import CassandraJoins_time_measure.*;
import InputManager.InputMgr;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.antlr.runtime.RecognitionException;
import CassandraJoinsParser.Parser;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * @author Alex
 */
public class AppHandler 
{
    private static Parser _qParser;
    private static String _hostname;
    private static long _rowNum;
    private final static long DEFAULT_ROW_NUMBERS = 300000;
    
    private AppHandler(){}
    
    
    /**
     * Gets the last primary key from list of primary keys.
     * @param conn
     * @param keyspace
     * @param table
     * @return key -> primary key of table
     */
    private static String getPrimaryKey(Cluster conn, String keyspace, String table)
    {
        String key;
        List<ColumnMetadata> primKeys;
        
        primKeys = conn.getMetadata().getKeyspace(keyspace).getTable(table).getPrimaryKey();
        
        key = primKeys.get(primKeys.size() - 1).getName();
        
        return key;
    }
    
    private static String[] getPrimaryKeys(Cluster conn, String keyspace, String[] tables)
    {
        String[] primKeys = new String[tables.length];
        int iter = 0;
        
        for (String tbl : tables)
        {
            primKeys[iter++] = getPrimaryKey(conn, keyspace, tbl);
        }
        
        return primKeys;
    }
    
    private static void getTimes()
    {
        CassandraJoins.getTimes().keySet().forEach((key) -> 
        {
            System.out.println
            (
                "Times for " + key + " Milliseconds: " +
                CassandraJoins.getTimes().get(key).getMillis() + " Seconds: " +
                CassandraJoins.getTimes().get(key).getSeconds()+ " Minutes: " +
                CassandraJoins.getTimes().get(key).getMinutes() + " Hours: " +
                CassandraJoins.getTimes().get(key).getHours()
            );
        });
    }
    
    private static void setRowNum(long rowNum)
    {
        _rowNum = rowNum;
    }
    
    private static long getRowNum()
    {
        return _rowNum;
    }
    
    private static void executeJoin(Cluster connection) throws CassandraJoinsException
    {
        String[] tables;
        String[] primKeys;
        String[] conditions1;
        String[] conditions2;
        String[] columns;
        String operator;
        String keyspace;
        String tmpTableName = "CJ_Temp_Table";
        
        if (_qParser.getTableArray().length == 1)
        {
            keyspace = _qParser.getKeyspace();
            columns = _qParser.getColumns().length == 0 ? null : _qParser.getColumns();
            tables = _qParser.getTableArray();
            primKeys = getPrimaryKeys(connection, keyspace, tables);
            conditions1 = _qParser.getConditionsList(tables[0]);
            operator = _qParser.getConditionalsOperartor();

            CassandraJoins.join
            (
                connection, 
                keyspace,
                tables[0],primKeys[0],columns,conditions1,
                null,null,null,null,
                getRowNum(),
                tmpTableName,
                operator
            );
        }
        else if (_qParser.getTableArray().length > 1)
        {
            keyspace = _qParser.getKeyspace();
            columns = _qParser.getColumns().length == 0 ? null : _qParser.getColumns();
            tables = _qParser.getTableArray();
            primKeys = getPrimaryKeys(connection, keyspace, tables);
            conditions1 = _qParser.getConditionsList(tables[0]);
            conditions2 = _qParser.getConditionsList(tables[1]);
            operator = _qParser.getConditionalsOperartor();

            CassandraJoins.join
            (
                connection, 
                keyspace,
                tables[0],primKeys[0],columns,conditions1,
                tables[1],primKeys[1],columns,conditions2,
                getRowNum(),
                tmpTableName,
                operator
            );
        }
    }
    
    public static void RunApp(String hostname) throws RecognitionException, InvalidRequestException
    {
        _hostname = hostname;
        setRowNum(DEFAULT_ROW_NUMBERS);
        
        Cluster connection = null;
        
        WelcomePrint();
        
        InputMgr.UsrInput();
        
        try
        {
            connection = CassandraJoins.connect(_hostname);
            
            while(InputMgr.UsrInput())
            {
                _qParser = new Parser(InputMgr.getUserInput());
                
                _qParser.BaseParser();
                
                executeJoin(connection);
                
                getTimes();
            }
                        
        }
        catch (RecognitionException e)
        {
            Logger.getLogger(AppHandler.class.getName()).log(Level.SEVERE, e.getMessage(), e);
        } catch (CassandraJoinsException ex) 
        {
            Logger.getLogger(AppHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
            else
            {
                String errMsg = "Conncetion to " + _hostname + " has not been established";
                Logger.getLogger(AppHandler.class.getName()).log(Level.SEVERE, errMsg);
            }
        }
    }

    private static void WelcomePrint() {
        System.out.println("##############################");
        System.out.println("# Welcome to Cassandra Joins #");
        System.out.println("##############################");
    }
}
