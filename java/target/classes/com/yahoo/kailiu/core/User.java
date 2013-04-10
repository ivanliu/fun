package com.yahoo.kailiu.core;

import java.util.ArrayList;
import java.util.List;

public class User {

    // making field private will exclude it in serialization
    public int age = 30;
    public String name = "kailiu";
    public List<String> messages = new ArrayList<String>() {
        {
            add("msg 1");
            add("msg 2");
            add("msg 3");
        }
    };

    // making get/set methods private will exclude it in serialization
    public String getFoo(String foo) {

        return this.name;
        //return "haha";
    }

    /*
    private void setFoo(String name) {

        this.name = name;
    }
    */

    @Override
        public String toString() {
            return "User [age=" + age + ", name=" + name + ", " +
                "messages=" + messages + "]";
        }
}
