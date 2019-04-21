/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CassandraJoinsParser;

import com.sun.media.jfxmedia.logging.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CqlLexer;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * TODO 1. Use indexOf to get first occurrence of OR and AND. Check the predecessor and use to split there.
 * @author Alex
 */

public class Parser 
{
    private String _query;
    
    /* SELECT clause members */
    private String _keyspace;
    private String[] _qTable;
    private String _qWhereClause;
    private String[] _qColumns;
    Map<String, String> _qConditionals;
    
    /* TODO In case different statement implementations are needed */
    public enum StmtT {
        SELECT,
        NONE
    };
    
    /**
     * Constructor for Parser class
     * Initializes query string
     * Initializes Lists that keep tables, columns, conditions
     * @param query
     */
    public Parser(String query)
    {
        this._query = query;
        this._qConditionals = new HashMap<>();
    }
    
    /**
     * Getter for query private member
     * @return query
    */
    public String getQuery()
    {
        return this._query;
    }
    
    /**
     * Set string array with table names
     */
    public void setTableArray()
    {
        String q = this.getQuery().toLowerCase();
        String[] fromSplit = q.split("( from | FROM )");
        String[] whereSplit = fromSplit[1].split("( where | WHERE )");
        String[] tableSplit = whereSplit[0].split(",");
        
        for (int i = 0; i < tableSplit.length; i++)
        {
            tableSplit[i] = tableSplit[i].replaceFirst(getKeyspace() + ".", "").trim();
            if (tableSplit[i].contains(";"))
            {
                tableSplit[i] = tableSplit[i].replaceFirst(";", "");
            }
        }
        _qTable = tableSplit;
    }
    
    /**
     * Get string array with table names
     * @return tables
     */
    public String[] getTableArray()
    {
        return _qTable;
    }
    
    /**
     * Set string array with columns from query
     */
    public void setColumns()
    {
        String[] empty = {};
        String q = getQuery().toLowerCase();
        String[] fromSplit = q.split("( from | FROM )");
        String[] selectSplit = fromSplit[0].split("(select |SELECT )");
        String rawColumns = selectSplit[1].trim();
        
        if (rawColumns.equals("*"))
        {
            _qColumns = empty;
        }
        else if (! rawColumns.contains(","))
        {
            String[] result = {
                rawColumns
            };
            _qColumns = result;
        }
        else
        {
            String[] result = rawColumns.split(",");
            for (int i = 0; i < result.length; i++)
            {
                result[i] = result[i].trim();
            }
            _qColumns = result;
        }
    }
    
    /**
     * Get string array with columns parsed from the query
     * @return qColumns
     */
    public String[] getColumns() throws NullPointerException
    {
        try
        {
            return _qColumns;
        }
        catch (NullPointerException e)
        {
            return null;
        }
    }
    
    private Boolean isEntireBoolTableTrue(Boolean[] arg)
    {
        Boolean result = true;
        
        for (Boolean isFalse : arg)
        {
            if (! isFalse)
            {
                result = false;
            }
        }
        
        return result;
    }
    
    private int getTableIndex(Boolean[] arg)
    {
        int result = 0;
        
        for (Boolean isTrue : arg)
        {
            if (isTrue)
            {
                break;
            }
            ++result;
        }
        
        return result;
    }
    
    /**
     * Creates Map to store conditionals depending on the table that they are referred to.
     * @throws RecognitionException 
     */
    public void setConditions() throws RecognitionException
    {
        String q = this.getQuery();
        
        try
        {
            String[] whereSplit = q.split("( where | WHERE )");
            String wholeConditions = whereSplit[1].replace(";", "").trim();
            Boolean[] containsTable = new Boolean[getTableArray().length];
            String[] tables = new String[getTableArray().length];
            int count = 0;
            int tableIdx = 0;
            
            for (int i = 0; i < getTableArray().length; i++)
            {
                containsTable[i] = false;
            }
            
            /**
             * Handle below situations
             * 1.a=.. and 1.b=..
             * 1,a=.. and 2.b=..
             * 1.a=.. and 2.b=.. and 1.c=..
             */
            for (String table : getTableArray())
            {
                if (wholeConditions.contains(table))
                {
                    containsTable[count++] = true;
                }
            }
            
            if (isEntireBoolTableTrue(containsTable) && containsTable.length > 1)
            {
                if (wholeConditions.toLowerCase().contains(" and "))
                {
                    tables = wholeConditions.split("( and | AND )");
                }
                else if (wholeConditions.toLowerCase().contains(" or "))
                {
                    tables = wholeConditions.split("( or | OR )");
                }
                
                for (String cond : tables)
                {
                    String[] table = getTableArray();
                    String strippedCond = cond.split(table[tableIdx] + ".")[1];
                    _qConditionals.put(table[tableIdx], strippedCond);
                    tableIdx++;
                }
            }
            else
            {
                for(int i = 0; i < containsTable.length; i++)
                {
                    if (containsTable[i])
                    {
                        _qConditionals.put(getTableArray()[i], wholeConditions.replaceAll(getTableArray()[i] + ".", ""));
                    }
                }
            }
        }
        catch (Exception e)
        {
            Logger.logMsg(Logger.ERROR, "Exception logged... Please check and correct error...");
        }
    }
    
    /**
     * Get conditions of a certain table that is mapped inside the Map _qConditionals
     * @param tableName
     * @return String Array of the table 
     */
    public String[] getConditionsList(String tableName)
    {
        String condition = _qConditionals.get(tableName);
        
        if (condition == null)
        {
            return null;
        }
        
        String[] result = 
        {
            condition
        };
                
        return result;
    }
    
    /**
     * Set elements for conditions
     */
    public void setWhereClaue()
    {
        String q = this.getQuery();
        String[] whereSplit = q.split("( where | WHERE )");
        try
        {
            String result = whereSplit[1];

            result = result.replace(";", "").trim();

            _qWhereClause = result;
        }
        catch (ArrayIndexOutOfBoundsException exArIndxOOB)
        {
            Logger.logMsg(Logger.WARNING, "No where clause has been found.");
            _qWhereClause = new String();
        }
    }
    
    /**
     * Get element from list (for conditions, tables, columns lists)
     * @return List
     */
    public String getWhereClause()
    {
        return this._qWhereClause;
    }
    
    /**
     * Get keyspace member
     * @return keyspace
     */
    public String getKeyspace()
    {
        return this._keyspace ;
    }
    
    /**
     * Set keyspace member
     * @param keyspace
     */
    private void setKeyspace(String keyspace)
    {
        this._keyspace = keyspace;
    }
    
    /**
     * Returns the statement type, relative to the enumeration types of StmtT
     * TODO In case different statement implementations are needed
     * @param p
     * @return 
     */
    public StmtT getStatment(ParsedStatement p)
    {
        if (p.getClass().getDeclaringClass() == SelectStatement.class)
        {
            return StmtT.SELECT;
        }
        
        return StmtT.NONE;
    }
    
    /**
     * Returns Conditionals Operator
     * It must be = or CONTAINS operator
     * @return op operator = or CONTAINS
     */
    public String getConditionalsOperartor()
    {
        String op = "=";
        
        if (_qWhereClause.regionMatches(true, 0, "contains", 0, _qWhereClause.length()))
        {
            op = "CONTAINS";
        }
        
        return op;
    }
    
    /**
     * The holy fucking grail
     * @throws RecognitionException 
     */
    public void BaseParser() throws RecognitionException, InvalidRequestException
    {
        StmtT stmtQ;
        ANTLRStringStream stringStream = new ANTLRStringStream(getQuery());
        CqlLexer lexer = new CqlLexer(stringStream); 
        CommonTokenStream token = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(token);
        
        ParsedStatement pStmt = parser.query();
        
        stmtQ = getStatment(pStmt);
        
        switch (stmtQ)
        {
            case SELECT:
            {
                SelectStatement.RawStatement sts = (SelectStatement.RawStatement) pStmt;
                SelectHandler(sts);
                break;
            }
            /* TODO In case different statement implementations are needed */
            default:
            {
                System.out.println("[PARSER]: Error : Wrong stament NONE statement found...");
                break;
            }
        }
    }
    
    /**
     * Handles a SELECT statement.
     * Sets private class members with parsed query
     * Sets keyspace
     * Sets columns
     * Sets table names
     * Sets conditions
     * @param rawStmt 
     */
    private void SelectHandler(SelectStatement.RawStatement rawStmt)
    {
//      System.out.println(rawStmt.keyspace());
//      System.out.println("Keyspace: " + rawStmt.keyspace());
//      System.out.println("Column family: " + rawStmt.columnFamily());
//      rawStmt.selectClause.forEach(sl -> System.out.println("select: " + sl.selectable.toString()));
//      rawStmt.whereClause.relations.forEach(c -> System.out.println(c));

        setKeyspace(rawStmt.keyspace());
        
        setColumns();
        setTableArray();
        setWhereClaue();
        try 
        {
            setConditions();
        } 
        catch (RecognitionException ex) 
        {
            java.util.logging.Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
