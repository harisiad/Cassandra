/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ConnectionManager;

import CassandraJoins_time_measure.*;
import InputManager.InputMgr;
import com.datastax.driver.core.Cluster;
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
        String firstTable;
        String secondTable;
        String joinFieldFirstTable;
        String joinFieldSecondTable;
        String[] conditionsFirstTable;
        String[] conditionsSecondTable;
        String[] columns;
        String operator;
        String keyspace;
        String tmpTableName = "CJ_Temp_Table";
        
        try
        {
            keyspace = _qParser.getKeyspace();
            columns = _qParser.getColumns().length == 0 ? null : _qParser.getColumns();
            tables = _qParser.getTableArray();
            firstTable = _qParser.getTableArray()[0];
            secondTable = _qParser.getTableArray()[1];
            joinFieldFirstTable = _qParser.getJoinFields()[0];
            joinFieldSecondTable = _qParser.getJoinFields()[1];
            conditionsFirstTable = _qParser.getConditionsList(firstTable);
            conditionsSecondTable = _qParser.getConditionsList(secondTable);
            operator = _qParser.getConditionalsOperartor();

            CassandraJoins.join
            (
                connection, 
                keyspace,
                tables[0],joinFieldFirstTable,columns,conditionsFirstTable,
                tables[1],joinFieldSecondTable,columns,conditionsSecondTable,
                getRowNum(),
                tmpTableName,
                operator
            );
        }
        catch (ArrayIndexOutOfBoundsException | 
                NullPointerException ex)
        {
            System.out.println(ex.getMessage());
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
            
            while(InputMgr.ExitApplication())
            {
                _qParser = new Parser(InputMgr.getUserInput());
                
                _qParser.BaseParser();
                
                executeJoin(connection);
                
                getTimes();
                
                InputMgr.UsrInput();
            }
        }
        catch (RecognitionException e)
        {
            Logger.getLogger(AppHandler.class.getName()).log(Level.SEVERE, e.getMessage(), e);
        } 
        catch (CassandraJoinsException ex) 
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
