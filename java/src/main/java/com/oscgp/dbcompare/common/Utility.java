/*
 * OSCG-Partners
 * 2022 All rights reserved 
 */
package com.oscgp.dbcompare.common;

import java.sql.Timestamp;

/**
 *
 * @author Muhammad Asif Naeem
 */
public class Utility {
    static public Timestamp getCurrentTime() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return timestamp;
    }

    static public void printLog(String text, boolean verbose) {
        if(verbose) {
            System.out.println(getCurrentTime() + ": " + text);
        }
    }
}





