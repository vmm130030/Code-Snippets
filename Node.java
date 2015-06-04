
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;

public class Node {

    String hostName;
    String[] hostFileName;
    InputStream input = null;
    Properties prop = new Properties();
    String[] singleIP;
    String listOfIPs;
    Integer numberOfNodes;
    Integer numberOfVotes = 0;
    ArrayList<String> listOfAllNodes = new ArrayList<>();

    
    static NodeProperties acessNumberOfVotes = new NodeProperties();

    public Node() {

		//try to read the configuration from the file
        try {
            hostName = InetAddress.getLocalHost().getHostName();
            hostFileName = hostName.split("\\.");
            input = new FileInputStream("//home/004/k/kv/kvp120030/AOS_Project/Project2/" + hostFileName[0] + "/config.properties");
            
            prop.load(input);
            listOfIPs = prop.getProperty("ips").trim();
            singleIP = listOfIPs.split(",");
            numberOfNodes = singleIP.length;
            numberOfVotes = Integer.parseInt(prop.getProperty("numberofvotes"));
           
            //Set the number of votes for this site and pass this object to receiver thread so that it can also access the
            //votes simultaneously.
            acessNumberOfVotes.setNumberOfVotes(numberOfVotes);
            //set number of replies expected = number of nodes
            acessNumberOfVotes.setnumberOfRepliesExpected(numberOfNodes);
            

            //now store this into an arraylist so that connections can be made
            for (int in = 0; in < numberOfNodes; in++) {
                listOfAllNodes.add(singleIP[in].trim());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        

    }

    public static void main(String args[]) {
        Node nodeObject = new Node();

        
        
        //First try to create receiver so that connections can be accepted
        ReceiveFileObject receiveFileObjects = new ReceiveFileObject(acessNumberOfVotes);
        
        receiveFileObjects.start();

        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Create sender object/ connect to client
        SendFileObject sendFileObject = new SendFileObject(nodeObject.listOfAllNodes);
        
        sendFileObject.start();

    }

}
