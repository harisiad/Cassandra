/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CassandraJoinsParser;


//import CassandraJoinsParser.Parser;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.antlr.runtime.RecognitionException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Alex
 */
public class ParserTest {

    /**
     * Test of getTableArray method, of class Parser.
     * 
     * Test queries:
     * 1. select * from a.table1;
     * 2. select * from a.table1, a.table2;
     * 
     * Expected Results:
     * 1. table1
     * 2. table1, table2
     */
    @Test
    public void testGetTableArray() {
        System.out.println("Testing getTableArray...");
        String[] testQueries = 
        {
            "select * from a.table1;",
            "select * from a.table1, a.table2;"
        };
        
        for (String q : testQueries)
        {
            Parser parserInstance = new Parser(q);
            try
            {
                parserInstance.BaseParser();
                if (q.equals("select * from a.table1;"))
                {
                    String[] expResult = 
                    {
                        "table1"
                    };
                    int expResultLength = expResult.length;
                    String[] result = parserInstance.getTableArray();
                    //Assert length of parsing result
                    assertEquals(expResultLength, result.length);
                    // Assert parsing result
                    assertArrayEquals(expResult, result);
                }
                else if (q.equals("select * from a.table1, a.table2;"))
                {
                    String[] expResult = 
                    {
                        "table1",
                        "table2"
                    };
                    int expResultLength = expResult.length;
                    String[] result = parserInstance.getTableArray();
                    //Assert length of parsing result
                    assertEquals(expResultLength, result.length);
                    // Assert parsing result
                    assertArrayEquals(expResult, result);
                }
            }
            catch (RecognitionException ex) 
            {
                Logger.getLogger(ParserTest.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println(ex.getMessage());
            }
        }
    }
    
    /**
     * Test of getColumns method, of class Parser.
     *
     * Test queries:
     * 1. select * from a.table1;
     * 2. select name from a.table1, a.table2;
     * 3. select name,age from a.table1, a.table2
     * 
     * Expected Results:
     * 1. Empty String Array
     * 2. {"name"}
     * 3. {"name","age"}
     */
    @Test
    public void testGetColumns() {
        System.out.println("Testing getColumns...");
        String[] testQueries = 
        {
            "select * from a.table1;",
            "select name from a.table1, a.table2;",
            "select name,age from a.table1, a.table2;"
        };
        
        for (String q : testQueries)
        {
            Parser parserInstance = new Parser(q);
            try
            {
                parserInstance.BaseParser();
                if (q.equals("select * from a.table1;"))
                {
                    String[] expResult = {};
                    int expResultLength = expResult.length;
                    String[] result = parserInstance.getColumns();
                    //Assert length of parsing result
                    assertEquals(expResultLength, result.length);
                    // Assert parsing result
                    assertArrayEquals(expResult, result);
                }
                else if (q.equals("select name from a.table1, a.table2;"))
                {
                    String[] expResult = 
                    {
                        "name"
                    };
                    int expResultLength = expResult.length;
                    String[] result = parserInstance.getColumns();
                    //Assert length of parsing result
                    assertEquals(expResultLength, result.length);
                    // Assert parsing result
                    assertArrayEquals(expResult, result);
                }
                else if (q.equals("select name,age from a.table1, a.table2;"))
                {
                    String[] expResult = 
                    {
                        "name",
                        "age"
                    };
                    int expResultLength = expResult.length;
                    String[] result = parserInstance.getColumns();
                    //Assert length of parsing result
                    assertEquals(expResultLength, result.length);
                    // Assert parsing result
                    assertArrayEquals(expResult, result);
                }
            }
            catch (RecognitionException ex) 
            {
                Logger.getLogger(ParserTest.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println(ex.getMessage());
            }
        }
    }
    
    
    /**
     * Test of get conditions method, of class Parser.
     * 
     * Test queries:
     * 1. select * from a.table1 where table1.name = 'Alex';
     * 2. select name from a.table1, a.table2 where table1.name = table2.name;
     * 3. select name,age from a.table1, a.table2 where table1.name = table2.name and table1.age = table2.age;
     * 
     * Expected Results:
     * 1. name = 'Alex'
     * 2. name = name
     * 3. name = name and age = age
     */
//    @Test
//    public void testGetConditions() {
//        System.out.println("Testing getConditionsList...");
//        String[] testQueries = 
//        {
//            "select * from a.table1 where table1.name = 'Alex';",
//            "select name from a.table1, a.table2 where table1.name = table2.name;",
//            "select name,age from a.table1, a.table2 where table1.name = table2.name and table1.age = table2.age;"
//        };
//        
//        for (String q : testQueries)
//        {
//            Parser parserInstance = new Parser(q);
//            try
//            {
//                parserInstance.BaseParser();
//                if (q.equals("select * from a.table1 where table1.name = 'Alex';"))
//                {
//                    String[] expResult = { "name = 'Alex'" };
//                    int expResultLength = expResult.length;
//                    String[] result = parserInstance.getConditionsList();
//                    //Assert length of parsing result
//                    assertEquals(expResultLength, result.length);
//                    // Assert parsing result
//                    assertArrayEquals(expResult, result);
//                }
//                else if (q.equals("select name from a.table1, a.table2 where table1.name = table2.name;"))
//                {
//                    String[] expResult = { "name = name" };
//                    int expResultLength = expResult.length;
//                    String[] result = parserInstance.getConditionsList();
//                    //Assert length of parsing result
//                    assertEquals(expResultLength, result.length);
//                    // Assert parsing result
//                    assertArrayEquals(expResult, result);
//                }
//                else if (q.equals("select name,age from a.table1, a.table2 where table1.name = table2.name and table1.age = table2.age;"))
//                {
//                    String[] expResult = { "name = name and age = age" };
//                    int expResultLength = expResult.length;
//                    String[] result = parserInstance.getConditionsList();
//                    //Assert length of parsing result
//                    assertEquals(expResultLength, result.length);
//                    // Assert parsing result
//                    assertArrayEquals(expResult, result);
//                }
//            }
//            catch (RecognitionException ex) 
//            {
//                Logger.getLogger(ParserTest.class.getName()).log(Level.SEVERE, null, ex);
//                System.out.println(ex.getMessage());
//            }
//        }
//    }
    
    /**
     * Test of get keyspace method, of class Parser.
     * 
     * Test queries:
     * 1. select * from a.table1 where table1.name = 'Alex';
     * 2. select * from b.table1 where table1.name = 'Alex';
     * 
     * Expected Results:
     * 1. a
     * 2. b
     */
    @Test
    public void testGetKeyspace() 
    {
        System.out.println("Testing getKeyspace...");
        String[] testQueries = 
        {
            "select * from a.table1 where table1.name = 'Alex';",
            "select * from b.table1 where table1.name = 'Alex';"
        };
        
        for (String q : testQueries)
        {
            Parser parserInstance = new Parser(q);
            try
            {
                parserInstance.BaseParser();
                if (q.equals("select * from a.table1 where table1.name = 'Alex';"))
                {
                    String expResult = "a";
                    int expResultLength = expResult.length();
                    String result = parserInstance.getKeyspace();
                    //Assert length of parsing result
                    assertEquals(expResultLength, result.length());
                    // Assert parsing result
                    assertEquals(expResult, result);
                }
                else if (q.equals("select * from b.table1 where table1.name = 'Alex'"))
                {
                    String expResult = "b";
                    int expResultLength = expResult.length();
                    String result = parserInstance.getKeyspace();
                    //Assert length of parsing result
                    assertEquals(expResultLength, result.length());
                    // Assert parsing result
                    assertEquals(expResult, result);
                }                
            }
            catch (RecognitionException ex) 
            {
                Logger.getLogger(ParserTest.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println(ex.getMessage());
            }
        }
    }
    
    @Test
    public void testGetConditionsList()
    {
        System.out.println("Testing New Conditionals...");
        String[] testQueries = 
        {
            "select * from a.table1 where table1.name = 'Alex';",
            "select * from a.table1, a.table2 where table1.name = 'Alex' AND table2.name = 'Alex';",
            "select * from a.table1 where table1.name = 'Alex' AND table1.surname = 'Charisiadis';",
            "select * from a.table1 where table1.name = 'Alex' AND table1.rampand = 1;"
        };
        
        for (String q : testQueries)
        {
            Parser parserInstance = new Parser(q);
            try
            {
                parserInstance.BaseParser();
                if (q.equals(testQueries[0]))
                {
                    System.out.println("Testing first conditionals query...");
                    String[] expResult = { "table1.name = 'Alex'" };
                    
                    String[] result = parserInstance.getConditionsList(parserInstance.getTableArray()[0]);
                    
                    assertArrayEquals(expResult, result);
                }
                else if (q.equals(testQueries[1]))
                {
                    System.out.println("Testing second conditionals query...");
                    String[] expResult = { "table1.name = 'Alex'" };
                                        
                    String[] result = parserInstance.getConditionsList(parserInstance.getTableArray()[0]);
                    
                    assertArrayEquals(expResult, result);
                    
                    expResult = new String[] { "table2.name = 'Alex'" };
                    
                    result = parserInstance.getConditionsList(parserInstance.getTableArray()[1]);
                    
                    assertArrayEquals(expResult, result);
                }
                else if (q.equals(testQueries[2]))
                {
                    System.out.println("Testing third conditionals query...");
                    String[] expResult = { "table1.name = 'Alex' AND table1.surname = 'Charisiadis'" };
                                        
                    String[] result = parserInstance.getConditionsList(parserInstance.getTableArray()[0]);
                    
                    assertArrayEquals(expResult, result);
                }
                else if (q.equals(testQueries[3]))
                {
                    System.out.println("Testing fourth conditionals query...");
                    String[] expResult = { "table1.name = 'Alex' AND table1.rampand = 1" };
                                        
                    String[] result = parserInstance.getConditionsList(parserInstance.getTableArray()[0]);
                    
                    assertArrayEquals(expResult, result);
                }
            }
            catch (RecognitionException ex) 
            {
                Logger.getLogger(ParserTest.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println(ex.getMessage());
            }
        }
    }
}
