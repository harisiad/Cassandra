/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CassandraJoinsParser;

import org.antlr.runtime.RecognitionException;
import ConnectionManager.AppHandler;
import CassandraJoins_time_measure.*;
import com.datastax.driver.core.Cluster;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO 1. Find a way to load binaries from Fountouris project. [DONE]
 * TODO 2. Add CassandraJoins Time Measurements in the project. Check out output of results.[DONE]
 * TODO 2.5. Tinker indexjoin and sortmergejoin methods to return object for time measurements. [DONE]
 * TODO 3. Start creating new class for data structure. (Possibly simple array)
 * TODO 4. Create new class for gather stats worker (dBAnalyst)
 * TODO 5. Change implementation of CassandraJoins to accept movie.movieid = producedby.movie conditionals
 * TODO 6. Use column functions for join function. [DONE]
 * TODO 7. Auto generate temp table name. [DONE] [FUTURE]
 * TODO 8. Change implementation of CassandraJoins to handle differently table.column='value' clauses.
 * TODO 8a. Try out if movie.movieid = producedby.movie on CassandraJoins works if just putting in the whereclause fields just the column name to declare equation. [DONE]
 * TODO 9. Upload to github. [DONE]
 * @author Alexandros Charisiadis
 */
public class Tester 
{
    /**
     * @param args the command line arguments
     * @throws org.antlr.runtime.RecognitionException
     * @throws CassandraJoins.CassandraJoinsException
     */
    public static void main(String[] args) throws RecognitionException 
    {
        String query = "select * from moviedb.events1, moviedb.events2 where events1.event_type in ('concert', 'opera') AND events2.event_type in ('theater', 'movie');";
        String host = "localhost";
        
        System.out.println("Opening connection to Cassandra localhost database");
        
        AppHandler.RunApp(host);
//       Cluster conn = CassandraJoins.connect(host);
//       try 
//       {
////           Session session = conn.connect();
//          
////           ResultSet rs = session.execute("select * from moviedb.movie where movieid = '0151230';");
//           //rs.forEach(row -> System.out.println(row.toString()));
//           
////           session.close();
//           
//           //Parser p = new Parser("select * from moviedb.movie, moviedb.producedby where movie.movieid = '0129884';");
//            Parser p = new Parser("select * from moviedb.movie, moviedb.producedby where events1.event_type in ('concert', 'opera');");
//           
//            p.BaseParser();
//           
//           //String[] conditions = p.getConditionsList();
//            String[] cond = {
//               "movie.movieid=producedby.movieid"
//            };
//            
//            System.out.println("Starting join operation...");
//            CassandraJoins.join
//            (
//                conn,
//                p.getKeyspace(), 
//                p.getTableArray()[0],
//                "movieid",
//                null , 
//                cond, 
//                p.getTableArray()[1],
//                "movieid", 
//                null, 
//                cond,
//                300000,
//                "movie_producedby2",
//                "="
//            );
//
//            System.out.println("Join operation completed...");
//
//            for (String key : CassandraJoins.getTimes().keySet())
//            {
//                System.out.println(
//                        "Times for " + key + " Milliseconds: " +
//                        CassandraJoins.getTimes().get(key).getMillis() + " Seconds: " +
//                        CassandraJoins.getTimes().get(key).getSeconds()+ " Minutes: " +
//                        CassandraJoins.getTimes().get(key).getMinutes() + " Hours: " +
//                        CassandraJoins.getTimes().get(key).getHours()
//                );
//            }
//       }
//       catch (RecognitionException e)
//       {
//           System.out.println(e.getMessage());
//       } 
//       catch (CassandraJoinsException ex) 
//       {
//           Logger.getLogger(Tester.class.getName()).log(Level.SEVERE, null, ex);
//       }
//       finally
//       {
//           conn.close();
//       }
    }
}
