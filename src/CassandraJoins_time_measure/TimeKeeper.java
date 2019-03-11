/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CassandraJoins_time_measure;

/**
 *
 * @author Alex
 */

/* Needs redesign, class _Timer doens't suit the implementation. */
public class TimeKeeper 
{
    private final long _millis;
    private final long _seconds;
    private final long _minutes;
    private final long _hours;
    
    public TimeKeeper(long millis, long seconds, long minutes, long hours)
    {
        _millis = millis;
        _seconds = seconds;
        _minutes = minutes;
        _hours = hours;
    }
    
    public long getMillis()
    {
        return _millis;
    }
    
    public long getSeconds()
    {
        return _seconds;
    }
    
    public long getMinutes()
    {
        return _minutes;
    }
    
    public long getHours()
    {
        return _hours;
    }
}
