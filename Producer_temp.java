package org.training.spark.loganalysis.analyzer;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Timer;
import java.util.TimerTask;

import org.omg.CORBA.portable.InputStream;

/**
 * Created by hduser on 6/16/19.
 */
public class Producer_temp {
    public static void main(String args[])
    {
  /*      try {
            Runtime.getRuntime().exec("cd /home/hduser/ && echo $(( RANDOM % (10 - 5 + 1 ) + 5 )) | ");
        } catch (IOException e) {
            System.out.println("error"+e.getMessage())
        }*/

        Timer timer = new Timer();
        timer.schedule(new Everysecond_tick(), 0, 1000); // called every 1 second

    }

}

class Everysecond_tick extends TimerTask {
    public void run() {
        try {
            Process proc = Runtime.getRuntime().exec("/home/hduser/random.sh /");
            BufferedReader read = new BufferedReader(new InputStreamReader(
                    proc.getInputStream()));

            try {
                proc.waitFor();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
            while (read.ready()) {
                System.out.println(read.readLine());
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
