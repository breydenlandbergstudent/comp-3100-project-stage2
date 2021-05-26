import java.net.*;
import java.util.*;
import java.io.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

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
    private static final String HELO = "HELO\n";
    private static final String OK = "OK\n";
    private static final String AUTH = "AUTH";
    private static final String REDY = "REDY\n";
    private static final String JOBN = "JOBN";
    private static final String JCPL = "JCPL";
    private static final String SCHD = "SCHD";
    private static final String NONE = "NONE\n";
    private static final String QUIT = "QUIT\n";
    private static final String GETS_Capable = "GETS Capable ";

    // buffer fields
    private static char[] charBuffer;
    private static byte[] byteBuffer; // will hold the current message from the server stored as bytes

    private static String stringBuffer; /* will hold the current message from the server stored in a string
                                                                       (created from charArray)        */
    private static String[] fieldBuffer; /* will hold the current message from the server as an array of strings
                                                                       (created from stringBuffer)     */

    private static String scheduleString; // string to be scheduled

    private static final int CHAR_BUFFER_LENGTH = 80;

    // create server/list objects
    private static List<Server> serverList;
    private static Server largestServer;

    // create file object
    private static File DSsystemXML;

    
    private static Boolean wasJCPL = false;

    public static void main(String[] args) throws IOException {
        setup();

        try {
            System.out.println("\nStep 1. Sending HELO...");
            writeBytes(HELO); // client sends HELO
            System.out.println("Step 2. Received OK");

            System.out.println("\nStep 3. Sending AUTH " + System.getProperty("user.name") + "...");
            writeBytes(AUTH + " " + System.getProperty("user.name") + "\n");
            System.out.println("Step 4. Received OK");

            System.out.println("\nStep 5. sending REDY...");
            writeBytes(REDY);
            readStringBuffer(); // reset stringBuffer & read job
            System.out.println("Step 6. Received " + stringBuffer);

            while(!(stringBuffer = bfr.readLine()).contains(NONE)) {
                System.out.println("\nprovided command is " + stringBuffer);
                if(wasJCPL == false) {
                    readStringBuffer();
                }
                System.out.println("provided command is " + stringBuffer);
                System.out.println("it is " + stringBuffer.contains(JOBN) + " that the provided command is JOBN");
                System.out.println("it is " + stringBuffer.contains(JCPL) + " that the provided command is JCPL");

                if(stringBuffer.contains(JOBN)) {
                    wasJCPL = false;
                    fieldBuffer = stringBuffer.split(" "); /* split String into array of strings
                                                              (each string being a field of JOBN) */

                    Job job = new Job(fieldBuffer); // create new Job object with data from fieldBuffer

                    /* SCHEDULE JOB */
                    // scheduleString = SCHD + " " + job.id + " " + largestServer.type + " " + largestServer.id;
                    // String GETS_Capable_STRING = GETS_Capable  + job.core.toString() + " " + job.memory.toString() + " " + job.disk.toString();
                    System.out.println("\nStep 7. Sending GETS Capable...");
                    writeBytes(GETS_Capable + " " + job.core + " " + job.memory + " " + job.disk + "\n");
                    // writeBytes(GETS_Capable_STRING);

                    System.out.println("\nsending OK...");
                    writeBytes(OK); // send OK for server to send the DATA command
    
                    readStringBuffer(); // reset stringBuffer & read next job
                    System.out.println("\nStep 8. DATA command received as: " + stringBuffer);

                    fieldBuffer = stringBuffer.split(" "); // split the DATA command into an array of strings
                    int numServersAvailable = Integer.parseInt(fieldBuffer[1]);
                    System.out.println("Therefore the number of available servers is: " + numServersAvailable);

                    System.out.println("\nStep 9. Sending OK...");
                    writeBytes(OK); // send OK

                    // parseServerInformation();
                    System.out.println("\nStep 10. Received and parsing server information...");
                    for(int i = 0; i < numServersAvailable; i++) {
                    	readStringBuffer();
                    	System.out.println(stringBuffer);
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
                    
                    System.out.println("\nStep 11. Sending OK after reading server information... going back to Step 10.");
                    writeBytes(OK); // send OK

                    readStringBuffer();
                    System.out.println("\nStep 10. Received " + stringBuffer + " ... going back to Step 7.");
                    
                    Server mostEfficientServer = getOptimalServer(serverList, job.core, job.memory, job.disk);
                    scheduleString = SCHD + " " + job.id + " " + mostEfficientServer.serverName + " " + mostEfficientServer.id + "\n";
                    System.out.println("\nStep 7. Scheduling the job...");
                    writeBytes(scheduleString);

                    readStringBuffer();
                    System.out.println("\nStep 8. Received " + stringBuffer);
                    
                    serverList.clear(); // clear server list for next job to be scheduled

                    readStringBuffer();
                    System.out.println("\nReceived " + stringBuffer + " ... why do we receive a whitespace");

                    System.out.println("\nStep 9. Go back to Step 5.... restart while loop");
                    writeBytes(REDY); // send REDY
                } 
                else if (stringBuffer.contains(JCPL)) {
                    wasJCPL = true;
                    System.out.println("\nSending REDY...");
                    writeBytes(REDY); // send REDY for the next job
                }
                else {
                	readStringBuffer();
		}
            }

            System.out.println("TERMINATING CONNECTION ...");
            
            writeBytes(QUIT);

            System.out.println("CONNECTION TERMINATED.");

            close();
        } catch (UnknownHostException e) {
            System.out.println("Unknown Host Exception: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("End of File Exception: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO Exception: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage() + " ... stringBuffer value is " + stringBuffer);
        }
    }

    public static void setup() throws IOException {
        serverList = new ArrayList<>(); // initialise list of servers

        s = new Socket(hostname, serverPort); // socket with host IP of 127.0.0.1 (localhost), server port of 50000

        din = new InputStreamReader(s.getInputStream());
        dout = new DataOutputStream(s.getOutputStream());
        bfr = new BufferedReader(din);

        setSystemXML();
    }

    public static void setSystemXML() {
        String dir = System.getProperty("user.dir") + "/ds-system.xml";
        DSsystemXML = new File(dir);
    }

    public static void writeBytes(String command) throws IOException {
        byteBuffer = command.getBytes();
        dout.write(byteBuffer);
        dout.flush();
    }

    public static void readStringBuffer() throws IOException {
        stringBuffer = bfr.readLine();
        
    }
    
    public static void parseServerInformation() {
    	
    }
    
    // the actual algorithm for choosing the optimal server for the job
    // currently just a first fit implementation
    public static Server getOptimalServer(List<Server> serverList, Integer jobCore, Integer jobMemory, Integer jobDisk) {
    	List<Server> tempServerList = new ArrayList<Server>();
    	for(Server server : serverList) {
    		if(server.core >= jobCore && server.memory >= jobMemory && server.disk >= jobDisk) {
    			tempServerList.add(server);
    		}
	}
	for(Server server : tempServerList) {
    		if(server.state.equals("active") || server.state.equals("idle")) {
    			return server;
    		}
    	}
    	return tempServerList.get(0);
    }
    
    public static void close() throws IOException {
        din.close();
        dout.close();
        bfr.close();
        s.close();
    }

}
