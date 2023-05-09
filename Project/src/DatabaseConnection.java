/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
/**
 *
 * @author Igbob
 */
public class DatabaseConnection {
    private static Connection conn = null;
    
    public DatabaseConnection(){
        try {
            Properties prop = new Properties();
            try(InputStream data = new FileInputStream("config.properties")) {
                prop.load(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            String url = prop.getProperty("dvdrental_DB_PROTO") + "://" + prop.getProperty("dvdrental_DB_HOST") + ":" + prop.getProperty("dvdrental_DB_PORT") + "/" + prop.getProperty("dvdrental_DB_NAME");
            String user = prop.getProperty("dvdrental_DB_USERNAME");
            String password = prop.getProperty("dvdrental_DB_PASSWORD");
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connection successful.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public ResultSet displayactor() {
        ResultSet rs = null;
        try {
            //Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3307/u22566954_u22492616_sakila", "root", "password");
            Statement s = conn.createStatement();
            String qry = "SELECT * FROM actor;";
            rs = s.executeQuery(qry);

        } catch (SQLException e) {

            e.printStackTrace();

        }
        return rs;
    }
    
    public ResultSet displayStaff(){
        ResultSet rs = null;
        try {
            Statement s = conn.createStatement();
            String query = "select staff.first_name, staff.last_name, address.address, address.address2, address.district, city.city, address.postal_code , address.phone, (SELECT address.address " +
            "FROM store INNER JOIN address ON address.address_id = store.address_id WHERE store.store_id = staff.store_id) AS StoreAddress, staff.active from ((address INNER JOIN staff ON staff.address_id = address.address_id) INNER JOIN city on address.city_id = city.city_id)";
            rs = s.executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }
    
    public ResultSet filterStaff(String name){
        ResultSet rs = null;
        try {
            Statement s = conn.createStatement();
            String query = "select staff.first_name, staff.last_name, address.address, address.address2, address.district, city.city, address.postal_code , address.phone, (SELECT address.address FROM store INNER JOIN address ON address.address_id = store.address_id WHERE store.store_id = staff.store_id) AS StoreAddress, staff.active from ((address INNER JOIN staff ON staff.address_id = address.address_id) INNER JOIN city on address.city_id = city.city_id) " +
            "where first_name LIKE '"+ name + "%'";
            rs = s.executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return rs;
    }
    
    public ResultSet displayFilm(){
        ResultSet rs = null;
        try {
            Statement s = conn.createStatement();
            String query = "SELECT * FROM film";
            rs = s.executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }
    
    public void insertFilm(String t, String d, int y, int lID, int olID, int r, double rr, int l, double rep, String rate, String spe){
        int num;
        try {
            PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement(
                    "INSERT INTO film(title, description, release_year, language_id, original_language_id, rental_duration, rental_rate, length, replacement_cost, rating, special_features, last_update) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");
            pstmt.setString(1, t);
            pstmt.setString(2, d);
            pstmt.setInt(3, y);
            pstmt.setInt(4, lID);
            pstmt.setInt(5, olID);
            pstmt.setInt(6, r);
            pstmt.setDouble(7, rr);
            pstmt.setInt(8, l);
            pstmt.setDouble(9, rep);
            pstmt.setString(10, rate);
            pstmt.setString(11, spe);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis()); 
            pstmt.setTimestamp(12, timestamp);
            num = pstmt.executeUpdate();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public ResultSet displayClient(){
        ResultSet rs = null;
        try {
            Statement s = conn.createStatement();
            String query = "SELECT * FROM customer";
            rs = s.executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }
    
    public void insertClient(int s, String fn, String sn, String e, int a, int active){
        try {
            PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement("INSERT INTO customer(store_id, first_name, last_name, email, address_id, active, create_date, last_update) VALUES(?,?,?,?,?,?,?,?)");
            pstmt.setInt(1, s);
            pstmt.setString(2, fn);
            pstmt.setString(3, sn);
            pstmt.setString(4, e);
            pstmt.setInt(5, a);
            pstmt.setInt(6, active);
            java.util.Date currentDate = new java.util.Date();
            java.sql.Date date = new java.sql.Date(currentDate.getTime());
            pstmt.setDate(7, date);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis()); 
            pstmt.setTimestamp(8, timestamp);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (SQLException excep) {
            excep.printStackTrace();
        }
    }
    
    public void deleteClient(int id){
        try {
            PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement("DELETE FROM customer WHERE customer_id = ?;");
            pstmt.setInt(1, id);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public void updateClient(int cID, String opt, String newValue){
        try {
            if (opt.equals("first name")) {
                PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement(
                        "update customer set first_name = ? where customer_id = ?");
                pstmt.setString(1, newValue);
                pstmt.setInt(2, cID);
                pstmt.executeUpdate();
            } else if (opt.equals("last name")) {
                PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement(
                        "update customer set last_name = ? where customer_id = ?");
                pstmt.setString(1, newValue);
                pstmt.setInt(2, cID);
                pstmt.executeUpdate();
            } else if (opt.equals("email")) {
                PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement(
                        "update customer set email = ? where customer_id = ?");
                pstmt.setString(1, newValue);
                pstmt.setInt(2, cID);
                pstmt.executeUpdate();
            } else if (opt.equals("store id")) {
                PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement(
                        "update customer set store_id = ? where customer_id = ?");
                int newVal = Integer.parseInt(newValue);
                pstmt.setInt(1, newVal);
                pstmt.setInt(2, cID);
                pstmt.executeUpdate();
            }else if (opt.equals("address id")) {
                PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement(
                        "update customer set address_id = ? where customer_id = ?");
                int newVal = Integer.parseInt(newValue);
                pstmt.setInt(1, newVal);
                pstmt.setInt(2, cID);
                pstmt.executeUpdate();
            } else if (opt.equals("active")) {
                PreparedStatement pstmt = (PreparedStatement) conn.prepareStatement(
                        "update customer set active = ? where customer_id = ?");
                int newVal = Integer.parseInt(newValue);
                pstmt.setInt(1, newVal);
                pstmt.setInt(2, cID);
                pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public ResultSet displayreport() {
        ResultSet rs = null;
        try {
//            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3307/sakila", "root", "password");
            Statement s = conn.createStatement();
            String qry = "SELECT i.store_id, c.name AS category_name, COUNT(film_category.film_id) AS num_films " +
                        "FROM inventory i " +
                        "JOIN film ON i.film_id = film.film_id " +
                        "JOIN film_category ON film.film_id = film_category.film_id " +
                        "JOIN category c ON film_category.category_id = c.category_id " +
                        "GROUP BY i.store_id, c.name";
            rs = s.executeQuery(qry);

        } catch (Exception e) {

            e.printStackTrace();

        }
        return rs;
    }
    
    public ResultSet displayforFilter(String name) {
        ResultSet rs = null;
        try {
//            
            Statement s = conn.createStatement();
            String qry = "SELECT * FROM customer WHERE first_name LIKE '"+name+"%'";
            rs = s.executeQuery(qry);

        } catch (Exception e) {

            e.printStackTrace();

        }
        return rs;
    }
    
    public ResultSet displaydrop() {
        ResultSet rs = null;
        try {
//            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3307/sakila", "root", "password");
            Statement s = conn.createStatement();
            String qry = "SELECT * FROM customer WHERE active = 0";
            rs = s.executeQuery(qry);

        } catch (Exception e) {

            e.printStackTrace();

        }
        return rs;
    }
}
