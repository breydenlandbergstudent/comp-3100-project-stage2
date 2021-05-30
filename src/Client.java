import java.net.*;
import java.util.*;
import java.io.*;


public class Client {

    // s
    private static Socket s;

    // s args
    private static final String hostname = "localhost";
    private static final int serverPort = 50000;

    // streams
    private static InputStreamReader din;
    private static DataOutputStream dout;
    private static BufferedReader bfr;

    // commands
    private static final String HELO = "HELO";
    private static final String OK = "OK";
    private static final String AUTH = "AUTH";
    private static final String REDY = "REDY";
    private static final String JOBN = "JOBN";
    private static final String JCPL = "JCPL";
    private static final String SCHD = "SCHD";
    private static final String NONE = "NONE";
    private static final String QUIT = "QUIT";
    private static final String GETS_Capable = "GETS Capable";

    // buffer fields
    private static byte[] byteBuffer; // will hold the current message from the server stored as bytes
    private static String stringBuffer; // will hold the current message from the server stored in a string
    private static String[] fieldBuffer; // will hold the current message from the server as an array of strings (created from stringBuffer)

    private static String scheduleString; // string to be scheduled

    // create server/list objects
    private static ArrayList<Server> serverList;


    public static void main(String[] args) throws IOException {
        setup();

        try {
            writeBytes(HELO); // Step 1. Sending HELO...

            readStringBuffer(); // Step 2. Received OK

            writeBytes(AUTH + " " + System.getProperty("user.name") + "\n"); // Step 3. Sending AUTH breyden...

            readStringBuffer(); // Step 4. Received OK

            writeBytes(REDY); // Step 5. Sending REDY...

            while(!(stringBuffer = bfr.readLine()).contains(NONE)) {

                // Step 6. Received JOBN;

                if(stringBuffer.contains(JOBN)) {
                    fieldBuffer = stringBuffer.split(" "); // split String into array of strings (each string being a field of JOBN)

                    Job job = new Job(fieldBuffer); // create new Job object with data from fieldBuffer

                    writeBytes(GETS_Capable + " " + job.core + " " + job.memory + " " + job.disk + "\n"); // Step 7. Sending GETS Capable...

                    writeBytes(OK); // sending OK...

                    readStringBuffer(); // Step 8. Receive DATA command

                    fieldBuffer = stringBuffer.split(" "); // split the DATA command into an array of strings
                    int numServersAvailable = Integer.parseInt(fieldBuffer[1]); // number of available servers

                    writeBytes(OK); // Step 9. Sending OK...

                    parseServerInformation(numServersAvailable); // Step 10. Received and parsing server information...

                    writeBytes(OK); // Step 11. Sending OK after reading server information... going back to Step 10.

                    readStringBuffer(); // Step 10. Received '.' ... going back to Step 7."

                    Server optimalServer = getOptimalServer(serverList, job); // use the custom designed algorithm to find the optimal server

                    // Step 7. Scheduling the job...
                    scheduleString = SCHD + " " + job.id + " " + optimalServer.serverName + " " + optimalServer.id + "\n";
                    writeBytes(scheduleString);

                    readStringBuffer(); // Step 8. Received '.'
                    
                    serverList.clear(); // clear server list for next job to be scheduled

                    writeBytes(REDY); // Step 9. Go back to Step 5.... restart while loop... send REDY
                } 
                else if (stringBuffer.contains(JCPL)) {
                    writeBytes(REDY); // send REDY for the next job
                }
            }
            writeBytes(QUIT); // Step 12.
            // Step 13. Received QUIT
            close(); // Step 14.
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // set up
    public static void setup() throws IOException {
        serverList = new ArrayList<>(); // initialise list of servers

        s = new Socket(hostname, serverPort); // socket withjobCore host IP of 127.0.0.1 (localhost), server port of 50000

        din = new InputStreamReader(s.getInputStream());
        dout = new DataOutputStream(s.getOutputStream());
        bfr = new BufferedReader(din);
    }


    // method to send command bytes to server
    public static void writeBytes(String command) throws IOException {
        byteBuffer = (command + "\n").getBytes();
        dout.write(byteBuffer);
        dout.flush();
    }


    // method to read command bytes from server
    public static void readStringBuffer() throws IOException {
        stringBuffer = bfr.readLine();
    }


    // parse server information into serverList List data structure
    public static void parseServerInformation(int numServersAvailable) throws IOException {
    	for(int i = 0; i < numServersAvailable; i++) {
            readStringBuffer();

            fieldBuffer = stringBuffer.split(" ");

            String serverName = fieldBuffer[0];
            int id = Integer.parseInt(fieldBuffer[1]);
            String state = fieldBuffer[2];
            int startTime = Integer.parseInt(fieldBuffer[3]);
            int core = Integer.parseInt(fieldBuffer[4]);
            int memory = Integer.parseInt(fieldBuffer[5]);
            int disk = Integer.parseInt(fieldBuffer[6]);
            int jobsWaiting = Integer.parseInt(fieldBuffer[7]);
            int jobsRunning = Integer.parseInt(fieldBuffer[8].trim()); // remove whitespace

            Server currentServer = new Server(serverName, id, state, startTime, core, memory, disk, jobsWaiting, jobsRunning);
            serverList.add(currentServer);
        }
    }


    // the algorithm for choosing the optimal server for the job
    // it is a modified BF algorithm
    //
    // BF - choose the server with least fitness value
    // modified to also choose the server with the fewest waiting jobs
    //
    // as opposed to simply the first Active/Booting server that has the lowest fitness value
    public static Server getOptimalServer(ArrayList<Server> servers, Job job) {
        Server optimalServer = servers.get(0);                      // initalise optimal server to be the first element of servers
        int lowestFitnessValue = servers.get(0).core - job.core;    // initialise the lowest fitness value to the fitness value of the first server

        for(Server s : servers) {                                   // iterate through every server
            int fitnessValue = s.core - job.core;                   // find current fitnessValue

            // the optimal server will be the one with
            // the lowest possible fitness value
            // AND lowest possible waiting jobs
             if(lowestFitnessValue < 0 ||
                     (fitnessValue < lowestFitnessValue &&
                    (s.jobsWaiting < optimalServer.jobsWaiting))) {
                        lowestFitnessValue = fitnessValue;
                        optimalServer = s;
            }
        }

        return optimalServer;
    }


    // close
    public static void close() throws IOException {
        din.close();
        dout.close();
        bfr.close();
        s.close();
    }
}