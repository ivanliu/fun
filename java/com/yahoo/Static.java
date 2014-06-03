package com.yahoo;

public class Static {
    int a;         // initialized to zero
    protected static int b = 10;  // initialized to zero only when class is loaded not for each object created.

    // Constructor incrementing static variable b
    public Static(){
        //b++;
    }

    public void showData(){
        System.out.println("Value of a = "+a);
        System.out.println("Value of b = "+b);
    }

    /*
       public static void increment(){
       a++;
       }
       */
}

