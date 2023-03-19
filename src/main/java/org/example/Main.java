package org.example;

import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) {
        //Test1 t= new Test1();
        FileWriter f =  new FileWriter();
        f.prcoessRecords();
        /*try {
            t.testcf();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }*/
        System.out.println("Hello world!");
    }
}