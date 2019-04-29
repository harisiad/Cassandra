/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CassandraJoinsParser;

import org.antlr.runtime.RecognitionException;
import CassandraJoins_time_measure.*;
import ConnectionManager.AppHandler;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * @author Alexandros Charisiadis
 */
public class Tester 
{
    /**
     * @param args the command line arguments
     * @throws org.antlr.runtime.RecognitionException
     * @throws org.apache.cassandra.exceptions.InvalidRequestException
     * @throws CassandraJoins.CassandraJoinsException
     */
    public static void main(String[] args) throws RecognitionException, InvalidRequestException 
    {
        String query = "select * from moviedb.events1, moviedb.events2 where events1.event_type in ('concert', 'opera') AND events2.event_type in ('theater', 'movie');";
        String host = "localhost";
        
        System.out.println("Opening connection to Cassandra localhost database");
        
//        ExpressionTree tree = new ExpressionTree();
//        
//        String[][] condTests1 = 
//        {
//            {
//                "table1.xbf = axv.reuity",
//                " OR "
//            },
//            {
//                "a = b",
//                " OR "
//            },
//            {
//                "a = 1",
//                " AND "
//            },
//            {
//                "b = 2",
//                " OR "
//            },
//            {
//                "c = 1",
//                ""
//            }
//        };
//        
//        for (int i = 0; i < condTests1.length; i++)
//        {
//            tree.insertClause(condTests1[i][0], condTests1[i][1]);
//        }
//        
//        tree.printTree();
//        
//        for (int i = 0; i < 100; i++)
//        {
//            System.out.print("-");
//        }
//        
//        System.out.println("");
//        
//        tree = new ExpressionTree();
//        
//        String[][] condTests2 =
//        {
//            {
//                "table1.xbf = axv.rdasg",
//                ""
//            }
//        };
//        
//        for (int i = 0; i < condTests2.length; i++)
//        {
//            tree.insertClause(condTests2[i][0], condTests2[i][1]);
//        }
//        
//        tree.printTree();
        
        AppHandler.RunApp(host);
    }
}
