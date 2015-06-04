

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

public class MessageObject implements Serializable {

    private long mid;
    private String mtype;
    private String msource;
    private String mdestination;
    private File mcontent;
    private Integer numberOfVotesGiven;
    private String nodeStatus;
    private String ownReadRequest;
    private String requestType;// is either R (Read) OR W (write)

    
    
    
    public String getRequestType() {
        return requestType;
    }

    
    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }
    
    
    
    
    
    
    
    public String getownReadRequest() {
        return ownReadRequest;
    }

    /**
     * @param mid the mid to set
     */
    public void setownReadRequest(String ownReadRequest) {
        this.ownReadRequest = ownReadRequest;
    }
    
    
    
    
    
    
    
    
    public long getMid() {
        return mid;
    }

    /**
     * @param mid the mid to set
     */
    public void setMid(long mid) {
        this.mid = mid;
    }

    /**
     * @return the mtype
     */
    public String getMtype() {
        return mtype;
    }

    /**
     * @param mtype the mtype to set
     */
    public void setMtype(String mtype) {
        this.mtype = mtype;
    }

    /**
     * @return the msource
     */
    public String getMsource() {
        return msource;
    }

    /**
     * @param msource the msource to set
     */
    public void setMsource(String msource) {
        this.msource = msource;
    }

    /**
     * @return the mdestination
     */
    public String getMdestination() {
        return mdestination;
    }

    /**
     * @param mdestination the mdestination to set
     */
    public void setMdestination(String mdestination) {
        this.mdestination = mdestination;
    }

    /**
     * @return the mcontent
     */
    public File getMcontent() {
        return mcontent;
    }

    /**
     * @param mcontent the mcontent to set
     */
    public void setMcontent(File mcontent) {
        this.mcontent = mcontent;
    }

    /**
     * @return the numberOfVotesGiven
     */
    public Integer getNumberOfVotesGiven() {
        return numberOfVotesGiven;
    }

    /**
     * @param numberOfVotesGiven the numberOfVotesGiven to set
     */
    public void setNumberOfVotesGiven(Integer numberOfVotesGiven) {
        this.numberOfVotesGiven = numberOfVotesGiven;
    }

    /**
     * @return the nodeStatus
     */
    public String getNodeStatus() {
        return nodeStatus;
    }

    /**
     * @param nodeStatus the nodeStatus to set
     */
    public void setNodeStatus(String nodeStatus) {
        this.nodeStatus = nodeStatus;
    }

    
}
