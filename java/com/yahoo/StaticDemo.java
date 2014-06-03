package com.yahoo;

import com.yahoo.Static;

public class StaticDemo {
    public static void main(String args[]){
        Static s1 = new Static();
        s1.showData();
        Static.b = 200;
        Static s2 = new Static();
        s2.showData();
        s1.showData();
    }
}
