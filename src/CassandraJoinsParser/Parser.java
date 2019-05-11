/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CassandraJoinsParser;

import com.sun.media.jfxmedia.logging.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CqlLexer;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * TODO 1. Use indexOf to get first occurrence of OR and AND. Check the predecessor and use to split there. [DONE]
 * TODO 2. Create field and getter for join fields (String[] _qJoinFields). [CRITICAL] [DONE]
 * TODO 3. Functional testing for conditionals and join fields. [CRITICAL] [DONE]
 * TODO 4. Rename _setConditionals to createConditionalToTableMapping [DONE]
 * TODO 5. Rename setCondtiotionals to createExpressionTree [DONE]
 * TODO 6. Rename _qConditionals to _qTableToConditionalsMapping, change Map<String, String> to Map<String, ArrayList<String>> [DONE]
 * TODO 6.5 Change implementation of createConditionalToTableMapping to fill correctly _qTableToConditionalsMapping and being executed after Expression Tree creation. [DONE]
 * TODO 6.5.1 Strip all conditions from table. (JOIN FIELDS AND CONDITIONALS) [CRITICAL] [DONE]
 * TODO 7. Refactor setConditionals [HIGH]
 * TODO 8. Create unit tests for new functionalities [HIGH]
 * @author Alex
 */

public class Parser 
{
    private String _query;
    
    /* SELECT clause members */
    private String _keyspace;
    private String[] _qTable;
    private String[] _qJoinFields;
    private String _qJoinClause;
    private String _qWhereClause;
    private String[] _qColumns;
    Map<String, ArrayList<String>> _qTableToConditionalsMapping;
    ExpressionTree _qExpressionTree;
    
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
        this._qTableToConditionalsMapping = new HashMap<>();
        this._qExpressionTree = new ExpressionTree();
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
     * Initialize join fields
     */
    public void initializeJoinFields() throws ParserException
    {
        _qJoinClause = _qExpressionTree.getJoinFields();
        _qJoinFields = _qJoinClause.split("(?i)(\\s*=\\s*|\\s*contains\\s*)");
        for (int i = 0; i < _qJoinFields.length; i++)
        {
            for (String table : getTableArray())
            {
                if (_qJoinFields[i].contains(table + "."))
                {
                    _qJoinFields[i] = _qJoinFields[i].replace(table + ".", "").trim();
                }
            }
        }
        
        if (_qJoinClause.isEmpty())
        {
            throw new ParserException("No join statement has been found inside the query. Please insert join statement in order to perform a join operation.");
        }
    }
    
    /**
     * Get Join Fields
     * @return String array with join fields that have been parsed.
     */
    public String[] getJoinFields()
    {
        return _qJoinFields;
    }
    
    public String getJoinClause()
    {
        return _qJoinClause;
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
    
    /**
     * Creation of the Expression Tree which describes the where clause of the query.
     */
    public void createExpressionTree()
    {
        final String EMPTY_OPERATOR = "";
        final String AND_OPERATOR = " AND ";
        final String OR_OPERATOR = " OR ";
        String[] splitConditionals;
        String wholeConditionals = getWhereClause().replace(";", "").trim();
        String remainingWholeConditionals = wholeConditionals;
        String conditionalToBeInserted;
        int andIdx = 0;
        int orIdx = 0;
        
        while (! remainingWholeConditionals.isEmpty())
        {
            andIdx = indexOfOperator(remainingWholeConditionals, AND_OPERATOR);
            orIdx = indexOfOperator(remainingWholeConditionals, OR_OPERATOR);
            
            if (orIdx < andIdx)
            {
                splitConditionals = remainingWholeConditionals.split("( or | OR )", 2);
                conditionalToBeInserted = splitConditionals[0];
                
                _qExpressionTree.insertClause(conditionalToBeInserted, OR_OPERATOR);
                
                remainingWholeConditionals = splitConditionals[1];
                
                if (checkForLastClause(remainingWholeConditionals))
                {
                    _qExpressionTree.insertClause(splitConditionals[1], EMPTY_OPERATOR);
                    remainingWholeConditionals = "";
                }
            }
            else if (andIdx < orIdx)
            {
                splitConditionals = remainingWholeConditionals.split("( and | AND )", 2);
                conditionalToBeInserted = splitConditionals[0];
                
                _qExpressionTree.insertClause(conditionalToBeInserted, AND_OPERATOR);
                
                remainingWholeConditionals = splitConditionals[1];
                
                if (checkForLastClause(remainingWholeConditionals))
                {
                    _qExpressionTree.insertClause(splitConditionals[1], EMPTY_OPERATOR);
                    remainingWholeConditionals = "";
                }
            }
            else
            {
                _qExpressionTree.insertClause(wholeConditionals, EMPTY_OPERATOR);
                remainingWholeConditionals = "";
            }
        }
        
    }
    
    /**
     * Returns the last conditional clause of the query's where clause.
     * @param clause
     * @return true if it is the last conditional, false otherwise.
     */
    private boolean checkForLastClause(String clause)
    {
        final int EXISTANCE_THRESHOLD = clause.length();
        
        return (indexOfOperator(clause, " AND ") == EXISTANCE_THRESHOLD) &&
                (indexOfOperator(clause, " OR ") == EXISTANCE_THRESHOLD);
    }
    
    /**
     * Calculates the index of the operator given and returns the first occurrence of this operator inside the conditional string.
     * @param conditional
     * @param operator
     * @return index of the operator
     */
    private int indexOfOperator(String conditional, String operator)
    {
        int operatorIdx = conditional.length();
        
        if (conditional.toUpperCase().contains(operator))
        {
            operatorIdx = conditional.toUpperCase().indexOf(operator);
        }
        
        return operatorIdx;
    }
    
    /**
     * Initializes the _qTableToConditionalsMapping map field with empty ArrayLists.
     */
    private void initializeMap()
    {
        for (String table : getTableArray())
        {
            _qTableToConditionalsMapping.put(table, new ArrayList<>());
        }
    }
    
    /**
     * Creates Map to store conditionals depending on the table that they are referred to.
     */
    public void createTableToConditionalsMapping()
    {
        _qExpressionTree.getExpressionTree().forEach(node -> 
        {
            if (node instanceof ExpressionTree.ClauseNode)
            {
                for(String table : getTableArray())
                {
                    if (((ExpressionTree.ClauseNode) node).getClause().contains(table + ".") &&
                        ! ((ExpressionTree.ClauseNode) node).getIsJoin())
                    {
                        ArrayList<String> tmpToInsert = _qTableToConditionalsMapping.get(table);
                        
                        tmpToInsert.add(((ExpressionTree.ClauseNode) node).getClause().replace(table + ".", "").trim());
                        
                        _qTableToConditionalsMapping.put(table, tmpToInsert);
                    }
                }
            }
        });
    }
    
    /**
     * Get conditions of a certain table that is mapped inside the Map _qTableToConditionalsMapping
     * @param tableName
     * @return String Array of the table 
     */
    public String[] getConditionsList(String tableName)
    {
        ArrayList<String> condition = _qTableToConditionalsMapping.get(tableName);
        String[] result = new String[condition.size()];
        int iter = 0;
        
        if (condition.isEmpty())
        {
            return null;
        }
        
        for (String cond : condition)
        {
            result[iter] = cond;
            iter++;
        }
                
        return result;
    }
    
    /**
     * Set elements for conditions
     * @throws CassandraJoinsParser.ParserException
     */
    public void setWhereClaue() throws ParserException
    {
        String q = this.getQuery();
        String[] whereSplit = q.split("( where | WHERE )");

        try
        {
            String result = whereSplit[1];

            result = result.replace(";", "").trim();

            _qWhereClause = result;
        }
        catch (ArrayIndexOutOfBoundsException ex)
        {
            throw new ParserException("No where statement was found inside query. Please use where statement inside query along with a join operation to be performed.", ex);
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
        
        if (_qJoinClause.regionMatches(true, 0, "contains", 0, _qJoinClause.length()))
        {
            op = "CONTAINS";
        }
        
        return op;
    }
    
    /**
     * The holy fucking grail
     * @throws RecognitionException 
     */
    public void BaseParser() throws RecognitionException
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
        try
        {
            setKeyspace(rawStmt.keyspace());
            System.out.println(rawStmt.columnFamily());
            setColumns();
            setTableArray();
            setWhereClaue();
            initializeMap();
            createExpressionTree();
            initializeJoinFields();
            createTableToConditionalsMapping();
        }
        catch (ParserException e)
        {
            System.out.println(e.getMessage());
        }
    }
}
