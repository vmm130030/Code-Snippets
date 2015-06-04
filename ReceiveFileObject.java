
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import com.sun.nio.sctp.*;

public class ReceiveFileObject extends Thread {
	
	
	NodeProperties acessNumberOfVotes;
	
	//receive the nodeProperties object required to access number of votes
	public ReceiveFileObject(NodeProperties acessNumberOfVotes){
		this.acessNumberOfVotes = acessNumberOfVotes;
	}

    @SuppressWarnings("restriction")
    public void run() {
        try {
            SctpServerChannel sctpServerChannel = SctpServerChannel.open();
            InetSocketAddress serverAddr = new InetSocketAddress(4567);
            sctpServerChannel.bind(serverAddr);

            while (true) {
                SctpChannel sctpChannel = sctpServerChannel.accept();
               // System.out.println("connection accepted from "+);
                new Thread(new ThreadForMultipleMessages(sctpChannel,acessNumberOfVotes)).start();

            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    //Create a separate thread for receiving the messages
    class ThreadForMultipleMessages extends Thread {
    	
    	NodeProperties acessNumberOfVotes = new NodeProperties();
        SctpChannel sctpChannelForMultipleMsg;
        MessageObject receivedMsg = new MessageObject();
        public static final int MESSAGE_SIZE = 1000;
        
        Queue<MessageObject> allRepliesWithVotesForOwnReadRequest=new LinkedList<MessageObject>();

        //constructor to get the sctp channel object
        public ThreadForMultipleMessages(SctpChannel sctpChannelForMultipleMsgs, NodeProperties acessNumberOfVotes) {
            this.sctpChannelForMultipleMsg = sctpChannelForMultipleMsgs;
            this.acessNumberOfVotes = acessNumberOfVotes;
        }

        public void run() {
            
                  
            while (true) {

            	
                try {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(MESSAGE_SIZE);
                    MessageInfo messageInfo = sctpChannelForMultipleMsg.receive(byteBuffer, null, null);

                    if (messageInfo.bytes() != -1) {

                        receivedMsg = (MessageObject) SerializeDeserialize.DeserializeMsg(byteBuffer.array());
                        
                        byteBuffer.clear();

                        System.out.println("source -"+receivedMsg.getMsource()+" destination- "+receivedMsg.getMdestination() + " status is - " + receivedMsg.getNodeStatus() + " and type is - " + receivedMsg.getMtype());
                        
                       
                        
                        
                        if (receivedMsg.getMtype().equalsIgnoreCase("RV")){
                        	//since this is request message for read, respond it with number of votes
                        	MessageObject replyWithVotesMsg = new MessageObject();
                        	
                        	
                        	Integer numberOfOwnVotes = acessNumberOfVotes.getNumberOfVotes();
                        	replyWithVotesMsg.setNumberOfVotesGiven(numberOfOwnVotes);
                        	replyWithVotesMsg.setMtype("SV");
                        	
                        	//Now check whether it is a read or write request and depending on it, set that type of reply
                        	replyWithVotesMsg.setRequestType(receivedMsg.getRequestType());
                        	
                        	//set destination of this message => source of incoming message
                        	replyWithVotesMsg.setMdestination(receivedMsg.getMsource());
                        	//set source of this message => destination of incoming message
                        	replyWithVotesMsg.setMsource(receivedMsg.getMdestination());
                        	
                        	//add this msg to sender queue so that we can reply back to requesting node
                        	synchronized(SendFileObject.RequestQAskingForVotes){
                        		SendFileObject.RequestQAskingForVotes.add(replyWithVotesMsg);
                        		SendFileObject.RequestQAskingForVotes.notify();
                        	}
                        }
                        
                        
                        if (receivedMsg.getMtype().equalsIgnoreCase("SV")){
                        	
                        	
                        	    
                        	System.out.println("This message is with votes that I requested-" + receivedMsg.getNumberOfVotesGiven());
                         //now first check whether it is a read request or write request
                        //as well as we have got replies with votes from other nodes
                        
                        	if (receivedMsg.getRequestType().equalsIgnoreCase("R")){
                        		//as this is read request, add it to queue so that we can calculate number of votes the
                        		//request has received
                        		System.out.println("Initial size - " + allRepliesWithVotesForOwnReadRequest.size());
                        		
                        			allRepliesWithVotesForOwnReadRequest.add(receivedMsg);
                        			
                        		                       		
                        	
                        		System.out.println("SIZE IS - " + allRepliesWithVotesForOwnReadRequest.size());              	
                        		//now check whether we have got replies from all nodes
                        		Integer getNumberOfRepliesExpected = acessNumberOfVotes.getnumberOfRepliesExpected();
                        	     // System.out.println("Replies expected - " + getNumberOfRepliesExpected);
                        		if(getNumberOfRepliesExpected.equals(allRepliesWithVotesForOwnReadRequest.size())){
                        			Integer totalVotesReceived = 0;
                        			
                        			System.out.println("SIZE MATCH ");
                        			//get votes from all replies
                        			while(!allRepliesWithVotesForOwnReadRequest.isEmpty()){
                        				
                        				MessageObject getVotesFromMsg;
                        				getVotesFromMsg = allRepliesWithVotesForOwnReadRequest.poll();
                        				totalVotesReceived = totalVotesReceived + getVotesFromMsg.getNumberOfVotesGiven();
                        			}
                        			
                        			
                        			System.out.println("Total votes received- " +totalVotesReceived);
                        	    	  
                        	      }	//end if = check size of queue with number of nodes
                        	
                        	}//end if=R
                        	
                        	
                        	
                        	
                        	
                        }//end if=SV

                    }//end of If from messageinfo

                } catch (Exception e) {
                    e.printStackTrace();
                }//end of try-catch

            }//end of while(true)

        }//end of run

    }//end of second class

}//end of main class
