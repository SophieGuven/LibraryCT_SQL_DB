package com.cydeo.utility;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DB_Util {
    private static Connection con ;
    private static Statement stm ;
    private static ResultSet rs ;
    private static ResultSetMetaData rsmd ;

    //Create Connection by jdbc url and username , password provided
    public static void createConnection(String url, String username, String password){

        try{
            con= DriverManager.getConnection(url, username, password);
            System.out.println("Connection Successful");
        }catch (Exception e){
            System.out.println("Connection Has Failed" + e.getMessage());
        }
    }

    //Run the sql query provided and return ResultSet object
    public static ResultSet runQuery(String sql){
        try{
            stm= con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs= stm.executeQuery(sql);
            rsmd= rs.getMetaData();
        }catch (Exception e){
            System.out.println("Error Occured while running query "+ e.getMessage());
        }
        return rs;
    }
    //destroy method to clean up all the resources after being used
    public static void destroy(){

        try{
            if(rs!=null) rs.close();
            if(stm!=null) stm.close();
            if(con!=null) con.close();
        }catch (Exception e){
            System.out.println("Error Occurred while closing resources " + e.getMessage());
        }
    }

    //This method will reset the cursor to before first location
    private static void resetCursor(){
        try{
            rs.beforeFirst();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // find out the row count
    public static int getRowCount(){
        int rowCount=0;
        try{
            rs.last();
            rowCount= rs.getRow();
        }catch (Exception e){
            System.out.println("Error Occurred while getting row count " + e.getMessage());
        }finally {
            resetCursor();
        }
        return rowCount;
    }
    //find out the column count
    public static int getColumnCount(){
        int columnCount = 0;
        try{
            columnCount = rsmd.getColumnCount();
        }catch (Exception e){
            System.out.println("Error Occurred While Getting Column Count " + e.getMessage());
        }
        return columnCount;
    }

    //Get all the Column names into a list object
    public static List<String> getAllColumnNameAsList(){
        List<String> columnNameLst = new ArrayList<>();

        try{
            for(int colIndex= 1; colIndex <= getColumnCount(); colIndex++){
                String columnName = rsmd.getColumnName(colIndex);
                columnNameLst.add(columnName);
            }
        }catch (Exception e){
            System.out.println("Error Occurred While getAllColumnNameAsList "+ e.getMessage());
        }
        return columnNameLst;
    }

    // get entire row of data according to row number
    public static List<String> getRowDataAsList( int rowNum ){

        List<String> rowDataAsLst = new ArrayList<>();
        int colCount =  getColumnCount() ;

        try {
            rs.absolute( rowNum );

            for (int colIndex = 1; colIndex <= colCount ; colIndex++) {

                String cellValue =  rs.getString( colIndex ) ;
                rowDataAsLst.add(   cellValue  ) ;
            }
        } catch (Exception e) {
            System.out.println("ERROR OCCURRED WHILE getRowDataAsList " + e.getMessage() );
        }finally {
            resetCursor();
        }
        return rowDataAsLst ;
    }

    //getting the cell value according to row num and column index
    public static String getCellValue(int rowNum , int columnIndex) {

        String cellValue = "" ;

        try {
            rs.absolute(rowNum) ;
            cellValue = rs.getString(columnIndex ) ;

        } catch (Exception e) {
            System.out.println("ERROR OCCURRED WHILE getCellValue " + e.getMessage() );
        }finally {
            resetCursor();
        }
        return cellValue ;
    }

    //getting the cell value according to row num and column name
    public static String getCellValue(int rowNum ,String columnName){

        String cellValue = "" ;

        try {
            rs.absolute(rowNum) ;
            cellValue = rs.getString( columnName ) ;

        } catch (Exception e) {
            System.out.println("ERROR OCCURRED WHILE getCellValue " + e.getMessage() );
        }finally {
            resetCursor();
        }
        return cellValue ;
    }

    //Get First Cell Value at First row First Column
    public static String getFirstRowFirstColumn(){

        return getCellValue(1,1) ;
    }

    //getting entire column data as list according to column number
    public static List<String> getColumnDataAsList(int columnNum) {

        List<String> columnDataLst = new ArrayList<>();

        try {
            rs.beforeFirst(); // make sure the cursor is at before first location
            while (rs.next()) {

                String cellValue = rs.getString(columnNum);
                columnDataLst.add(cellValue);
            }

        } catch (Exception e) {
            System.out.println("ERROR OCCURRED WHILE getColumnDataAsList " + e.getMessage());
        } finally {
            resetCursor();
        }
        return columnDataLst;
    }

    // getting entire column data as list according to column Name
    public static List<String> getColumnDataAsList(String columnName){

        List<String> columnDataLst = new ArrayList<>();

        try {
            rs.beforeFirst(); // make sure the cursor is at before first location
            while( rs.next() ){

                String cellValue = rs.getString(columnName) ;
                columnDataLst.add(cellValue) ;
            }

        } catch (Exception e) {
            System.out.println("ERROR OCCURRED WHILE getColumnDataAsList " + e.getMessage() );
        }finally {
            resetCursor();
        }
        return columnDataLst ;
    }

    //display all data from the ResultSet Object
    public static void  displayAllData() {

        int columnCount = getColumnCount();
        resetCursor();
        try {

            while (rs.next()) {

                for (int colIndex = 1; colIndex <= columnCount; colIndex++) {

                    //System.out.print( rs.getString(colIndex) + "\t" );
                    System.out.printf("%-25s", rs.getString(colIndex));
                }
                System.out.println();

            }

        } catch (Exception e) {
            System.out.println("ERROR OCCURRED WHILE displayAllData " + e.getMessage());
        } finally {
            resetCursor();
        }
    }

    //Save entire row data as Map<String,String>
    public static Map<String,String> getRowMap(int rowNum){

        Map<String,String> rowMap = new LinkedHashMap<>();
        int columnCount = getColumnCount() ;

        try{

            rs.absolute(rowNum) ;

            for (int colIndex = 1; colIndex <= columnCount ; colIndex++) {
                String columnName = rsmd.getColumnName(colIndex) ;
                String cellValue  = rs.getString(colIndex) ;
                rowMap.put(columnName, cellValue) ;
            }

        }catch(Exception e){
            System.out.println("ERROR OCCURRED WHILE getRowMap " + e.getMessage() );
        }finally {
            resetCursor();
        }
        return rowMap ;
    }

    //Now Store All rows as List of Map object
    public static List<Map<String,String>> getAllRowAsListOfMap(){

        List<Map<String,String>> allRowLstOfMap = new ArrayList<>();
        int rowCount = getRowCount() ;
        // move from first row till last row
        // get each row as map object and add it to the list

        for (int rowIndex = 1; rowIndex <= rowCount ; rowIndex++) {

            Map<String,String> rowMap = getRowMap(rowIndex);
            allRowLstOfMap.add( rowMap ) ;

        }
        resetCursor();

        return allRowLstOfMap ;

    }


}
