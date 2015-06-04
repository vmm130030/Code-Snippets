
import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.BlockingQueue;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

public class SendFileObject extends Thread {

    ArrayList<String> listOfAllNodes;
    SctpChannel[] sctpChannels;
    InetSocketAddress socketAddress;
    SctpChannel sctpChannel;
    ByteBuffer byteBuffer;
    MessageInfo messageInfo;
   // public static volatile Queue<MessageObject> readRequestQ=new LinkedList<MessageObject>();
    public static volatile Queue<MessageObject> RequestQAskingForVotes=new LinkedList<MessageObject>();
    BlockingQueue<MessageObject> proposeMsgQueue;
    
     public SendFileObject(ArrayList<String> listOfAllNodes) {

        this.listOfAllNodes = listOfAllNodes;

        messageInfo = MessageInfo.createOutgoing(null, 0);

        byteBuffer = ByteBuffer.allocate(1024 * 8);

    }

    @Override
    public synchronized void run() {
  	
    	  	
        sctpChannels = multipleConnections();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ex) {
            Logger.getLogger(SendFileObject.class.getName()).log(Level.SEVERE, null, ex);
        }
        
          // a thread for generating read and write requests
          new Thread(new GenerateReadRequests()).start();
        
        
        //now check readRequestQ periodically
        while (true){
        	try {
				Thread.sleep(1000);
			    } catch (InterruptedException e) {
				e.printStackTrace();
			    }
        	
        	
        	MessageObject newmsg= new MessageObject();
        	synchronized (SendFileObject.RequestQAskingForVotes) {
        		
				if(!SendFileObject.RequestQAskingForVotes.isEmpty()){
					
				newmsg=SendFileObject.RequestQAskingForVotes.remove();
				SendFileObject.RequestQAskingForVotes.notify();
				
				//if it is request message asking for votes
				if (newmsg.getMtype().equalsIgnoreCase("RV")){
					idetifyAndSendMessage(sctpChannels, newmsg);
				}
				
				//if it is reply message with votes for the node
				if (newmsg.getMtype().equalsIgnoreCase("SV")){
					idetifyAndSendVotesMessage(sctpChannels, newmsg);
				}
				
				//System.out.println("message removed and function called to send message");
				}
				
			}
        	  
        }
       
        
        

    }

    
    
	////
	//Generate read and write request as per specifications
	//Create a thread for that
	class GenerateReadRequests extends Thread {
		int numberofMsgs = 1; // number of read requests
		int count = 0;
		
		public synchronized void run(){
		    //wait for 5 seconds  
			for(int it = 0; it < numberofMsgs; it ++){
		    	  try {
		    		  Thread.sleep(5000);
		    	  }
		    	  catch (Exception e){
		    		  e.printStackTrace();
		    	  }
		    	      	  
		    	   //generate read request
		    	  
		    	  GenerateReadRequestMsgs();
		    	      	
		      }// end of for
		      
			
		}
	}  
    
    
    
    
    public void idetifyAndSendMessage(SctpChannel[] sctpCahnnels, MessageObject newmsg) {

        for (SctpChannel channel : sctpCahnnels) {
            try {

                Set<SocketAddress> remoteAddresses = channel.getRemoteAddresses();
               // MessageObject msgObj = new MessageObject();

                for (SocketAddress socketAddress : remoteAddresses) {

                    InetSocketAddress ip = (InetSocketAddress) socketAddress;

                    newmsg.setMsource(InetAddress.getLocalHost().getHostName());
                    newmsg.setMdestination(ip.getHostName());
                    
                    

                    try {
                        
                        
                        if (ip.getHostName().equals("net01.utdallas.edu")){
                        	newmsg.setNodeStatus("This is read-request-votes to node 1");
                        }
                        
                        if (ip.getHostName().equals("net02.utdallas.edu")){
                        	newmsg.setNodeStatus("This is read-request-votes to node 2");
                        }
                        
                        if (ip.getHostName().equals("net03.utdallas.edu")){
                        	newmsg.setNodeStatus("This is read-request-votes to node 3");
                        }

                        byteBuffer.put(SerializeDeserialize.SerializeMsg(newmsg));

                        byteBuffer.flip();
                        channel.send(byteBuffer, messageInfo);
                        System.out.println("RV message sent");

                        byteBuffer.clear();
                        

                    } catch (IOException ex) {
                        Logger.getLogger(SendFileObject.class.getName()).log(Level.SEVERE, null, ex);
                    }

                }

            } catch (IOException ex) {
                Logger.getLogger(SendFileObject.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

    }

    public void idetifyAndSendVotesMessage(SctpChannel[] sctpCahnnels, MessageObject newmsg) {
        for (SctpChannel channel : sctpCahnnels) {
            try {

                Set<SocketAddress> remoteAddresses = channel.getRemoteAddresses();
               // MessageObject msgObj = new MessageObject();

                for (SocketAddress socketAddress : remoteAddresses) {

                    InetSocketAddress ip = (InetSocketAddress) socketAddress;

                   // newmsg.setMsource(InetAddress.getLocalHost().getHostName());
                  //  newmsg.setMdestination(ip.getHostName());
                    
                    

                    try {
                        
                        if (ip.getHostName().equalsIgnoreCase((newmsg.getMdestination())) && InetAddress.getLocalHost().getHostName().toString().equalsIgnoreCase(newmsg.getMsource())){
                        	newmsg.setNodeStatus("This is reply with votes");
                        	byteBuffer.put(SerializeDeserialize.SerializeMsg(newmsg));

                            byteBuffer.flip();
                            channel.send(byteBuffer, messageInfo);
                            
                           System.out.println("SV message sent");

                            byteBuffer.clear();
                        }
                        
                        
                        

                    } catch (IOException ex) {
                        Logger.getLogger(SendFileObject.class.getName()).log(Level.SEVERE, null, ex);
                    }

                }

            } catch (IOException ex) {
                Logger.getLogger(SendFileObject.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }
    
    public SctpChannel[] multipleConnections() {

        sctpChannels = new SctpChannel[listOfAllNodes.size()];

        for (int i = 0; i < listOfAllNodes.size(); i++) {
            socketAddress = new InetSocketAddress(listOfAllNodes.get(i), 4567);
            sctpChannels[i] = connection(socketAddress);
        }
        return sctpChannels;

    }

    public synchronized SctpChannel connection(SocketAddress socketAddress) {
        try {
            try {

                sctpChannel = SctpChannel.open();
              
                sctpChannel.connect(socketAddress);

            } catch (IOException ex) {
                Logger.getLogger(SendFileObject.class.getName()).log(Level.SEVERE, null, ex);
            }

            sctpChannel.getRemoteAddresses().iterator().next();
            return sctpChannel;
        } catch (IOException ex) {
            Logger.getLogger(SendFileObject.class.getName()).log(Level.SEVERE, null, ex);
        }
        return sctpChannel;

    }

    
    public void GenerateReadRequestMsgs(){
    	
    	//add the message to the queue
			MessageObject readRequest= new MessageObject();
			//set properties for created object
			readRequest.setMtype("RV");//RV=RequestVotes
			readRequest.setRequestType("R");//R=ReadRequest
			synchronized (SendFileObject.RequestQAskingForVotes){
			//System.out.println("Message added in own request q - " + it);
			SendFileObject.RequestQAskingForVotes.add(readRequest);
			SendFileObject.RequestQAskingForVotes.notify();
			}
    }
    
    
    
}

