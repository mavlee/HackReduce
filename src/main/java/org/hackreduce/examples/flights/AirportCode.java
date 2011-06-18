package org.hackreduce.examples.flights;

import java.io.*;
import java.util.*;

public class AirportCode {

  private static final String airportCodeFile = "GlobalAirportDatabase.txt";
  private static HashMap<String, String> table;

  public static String lookup(String airportCode) {
    getTable();
    return table.get(airportCode);
  }

  public static void getTable() {
    if (table == null) {
      createTable(airportCodeFile);
    }
  }

  public static void createTable(String filename) {
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(filename))));
      String line;
      table = new HashMap<String, String>();
      while ((line = br.readLine()) != null) {
        String[] data = line.split(":");
        String latDeg = convertToDeg(data[5], data[6], data[7], data[8]);
        String longDeg = convertToDeg(data[9], data[10], data[11], data[12]);
        table.put(data[0], latDeg + " " + longDeg);
      }
      br.close();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
  }

  public static String convertToDeg(String deg, String min, String sec, String dir) {
    double degrees = Integer.parseInt(deg) + Integer.parseInt(min) / 60.0 + Integer.parseInt(sec) / 3600.0;
    if (dir.charAt(0) == 'W' || dir.charAt(0) == 'S') {
      degrees *= -1;
    }
    return Double.toString(degrees);
  }
}
