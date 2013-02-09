/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package duplicatefinder;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSet;
import com.mysql.jdbc.Statement;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author unicorn
 */
public class Operations {

   // JDBC driver name and database URL
   static boolean isTableExist = false;
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
   static final String DB_URL = "jdbc:mysql://localhost/test3";
   // Database credentials
   static String TABLE_NAME = "gases1";
   static  String USER = "root";
   static  String PASS = "ddm";

   Connection conn = null;
   Statement stmt = null;



   public boolean query(String selection,String groupby,String resultgroup,int count,int selectitems,String sortorder)
    {
        try
        {
            BufferedWriter out = null;
            out = new BufferedWriter(new FileWriter("result.csv"));
            
            Class.forName("com.mysql.jdbc.Driver");
            //Open a connection
            conn = (Connection) DriverManager.getConnection(DB_URL,USER,PASS);
            //Execute a query
            stmt = (Statement) conn.createStatement();
            String sql;
            
            sql ="SELECT " +selection + " FROM "+ TABLE_NAME+" G, " +
                       "(SELECT " + groupby +  " FROM "+TABLE_NAME+" GROUP BY " + groupby +
                       " HAVING COUNT(*) >= "+ count+ " )AS Q WHERE "+resultgroup + "ORDER BY G."+sortorder;

            ResultSet rs = (ResultSet) stmt.executeQuery(sql);
            
            //Extract data from result set
            out.write(selection+"\n");

            while (rs !=null && rs.next())
            {
                for(int i = 0;i < selectitems ;i++)
                {
                    out.write(rs.getString(i+1));
                    if(i != selectitems -1)
                    {
                        out.write(",");
                    }
                }
                out.write("\n");                    
            }

            System.out.println("Done...");

            out.close();
            rs.close();
            stmt.close();
            conn.close();

            return true;
        } catch (IOException ex)
        {
            Logger.getLogger(Operations.class.getName()).log(Level.SEVERE, null, ex);
        } catch(ClassNotFoundException e)
        {
            e.printStackTrace();
        } catch(SQLException e)
        {
            e.printStackTrace();
        }finally
        {
                //finally block used to close resources
            try
            {
                if (stmt != null)
                {
                    stmt.close();
                }
            } catch (SQLException se2) 
            {
            } 
            try
            {
                if (conn != null) 
                {
                    conn.close();
                }
            } catch (SQLException se) 
            {
                se.printStackTrace();
            } 
        }
        return false;
   }

   public boolean loadData(String path,String importSettings)
    {
        try {
                Class.forName("com.mysql.jdbc.Driver");
                //Open a connection
                conn = (Connection) DriverManager.getConnection(DB_URL,USER,PASS);
                //Execute a query
                stmt = (Statement) conn.createStatement();

                stmt = (Statement) conn.createStatement(
                                ResultSet.TYPE_SCROLL_SENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
                String sql;

                sql = "LOAD DATA LOCAL INFILE \'" +path +"\' INTO TABLE "
                            + TABLE_NAME + " FIELDS TERMINATED BY \',\' ENCLOSED BY"
                            + " \'\"\' LINES TERMINATED BY \'\\r\\n\' ("
                            + importSettings +")";

               stmt.executeUpdate(sql);
               stmt.close();
               conn.close();

               return true;


        }
        catch (SQLException ex) 
        {
            Logger.getLogger(Operations.class.getName()).log(Level.SEVERE, null, ex);
        }catch(ClassNotFoundException e)
        {
            e.printStackTrace();
        } finally 
        {
            try
            {
                if (stmt != null) 
                {
                    stmt.close();
                }
            } catch (SQLException se2) 
            {
            }
            try 
            {
                if (conn != null) 
                {
                    conn.close();
                }
            } catch (SQLException se) 
            {
                se.printStackTrace();
            } 
        }
        return false;

   }

    public boolean clearData()
    {
        if(isTableExist)
        {
            try 
            {
                 Class.forName("com.mysql.jdbc.Driver");

                 conn = (Connection) DriverManager.getConnection(DB_URL,USER,PASS);

                 stmt = (Statement) conn.createStatement();
                 stmt = (Statement) conn.createStatement(
                                ResultSet.TYPE_SCROLL_SENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);

               stmt.executeUpdate("DELETE FROM "+TABLE_NAME);
               stmt.close();
               conn.close();

               return true;

            } catch (SQLException ex) 
            {
                Logger.getLogger(Operations.class.getName()).log(Level.SEVERE, null, ex);
            }catch(ClassNotFoundException e)
            {
                e.printStackTrace();
            } finally 
            {
                    //finally block used to close resources
                try
                {
                    if (stmt != null) 
                    {
                        stmt.close();
                    }
                } catch (SQLException se2) 
                {
                } 
                try 
                {
                    if (conn != null) 
                    {
                        conn.close();
                    }
                } catch (SQLException se) 
                {
                    se.printStackTrace();
                }
            }
        }
        return false;
   }


    public boolean createTable(String fields)
    {
        if(!isTableExist)
        {
            try 
            {
                 Class.forName("com.mysql.jdbc.Driver");
                 conn = (Connection) DriverManager.getConnection(DB_URL,USER,PASS);
                 stmt = (Statement) conn.createStatement();

                 stmt = (Statement) conn.createStatement(
                                    ResultSet.TYPE_SCROLL_SENSITIVE,
                                    ResultSet.CONCUR_UPDATABLE);

                 String sql;

                 sql = "CREATE TABLE "+TABLE_NAME +"("+ fields +")";
                 stmt.executeUpdate(sql);
                 isTableExist = true;
                 stmt.close();
                 conn.close();

                 return true;
                 
            } catch (SQLException ex)
            {
                Logger.getLogger(Operations.class.getName()).log(Level.SEVERE, null, ex);
            }catch(ClassNotFoundException e)
            {
                e.printStackTrace();
            } finally
            {
                    try
                    {
                        if (stmt != null) 
                        {
                            stmt.close();
                        }
                    } catch (SQLException se2) 
                    {
                    }
                    try
                    {
                        if (conn != null) 
                        {
                            conn.close();
                        }
                    } catch (SQLException se)
                    {
                        se.printStackTrace();
                    }
                }
        }
        return false;

   }

    public boolean deleteTable()
    {
        if(isTableExist)
        {
            try 
            {
                    Class.forName("com.mysql.jdbc.Driver");
                    conn = (Connection) DriverManager.getConnection(DB_URL,USER,PASS);
                    stmt = (Statement) conn.createStatement();

                    stmt = (Statement) conn.createStatement(
                                    ResultSet.TYPE_SCROLL_SENSITIVE,
                                    ResultSet.CONCUR_UPDATABLE);

                    String sql;

                    sql = "DROP TABLE "+TABLE_NAME;
                    stmt.executeUpdate(sql);
                    stmt.close();
                    conn.close();

                    return true;


            } catch (SQLException ex) 
            {
                Logger.getLogger(Operations.class.getName()).log(Level.SEVERE, null, ex);
            }catch(ClassNotFoundException e)
            {
                e.printStackTrace();
            } finally
            {
                    try 
                    {
                        if (stmt != null)
                        {
                            stmt.close();
                        }
                    } catch (SQLException se2)
                    {
                    }
                    try 
                    {
                        if (conn != null)
                        {
                            conn.close();
                        }
                    } catch (SQLException se)
                    {
                        se.printStackTrace();
                    }
                }
       
        }
         return false;
   }

    public int getEntryCount()
    {
        int entries = 0;
        try {

                Class.forName("com.mysql.jdbc.Driver");
                conn = (Connection) DriverManager.getConnection(DB_URL,USER,PASS);
                stmt = (Statement) conn.createStatement();
                String sql;

               sql ="SELECT COUNT(*) FROM "+ TABLE_NAME;

               ResultSet rs = (ResultSet) stmt.executeQuery(sql);

               while (rs !=null && rs.next()) {

                 entries  = Integer.parseInt(rs.getString(1));
                }

                rs.close();
                stmt.close();
                conn.close();

                return entries;


        } catch(ClassNotFoundException e)
        {
            e.printStackTrace();
        } catch(SQLException e)
        {
            e.printStackTrace();
        }finally 
        {
                //finally block used to close resources
                try
                {
                    if (stmt != null)
                    {
                        stmt.close();
                    }
                } catch (SQLException se2)
                {
                } 
                try 
                {
                    if (conn != null)
                    {
                        conn.close();
                    }
                } catch (SQLException se)
                {
                    se.printStackTrace();
                }
          }
        return 0;
   }

    public static void setPassword(String pass)
    {
        Operations.PASS = pass;
    }

     public static void setUsername(String user)
    {
        Operations.USER = user;
    }
}
