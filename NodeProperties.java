
public class NodeProperties {

	Integer numberOfVotes;
	String currentMode;
	Integer numberOfRepliesExpected;

	public Integer getnumberOfRepliesExpected() {
		return numberOfRepliesExpected;
	}
	public void setnumberOfRepliesExpected(Integer numberOfRepliesExpected) {
		this.numberOfRepliesExpected = numberOfRepliesExpected;
	}
	
	public Integer getNumberOfVotes() {
		return numberOfVotes;
	}
	public void setNumberOfVotes(Integer numberOfVotes) {
		this.numberOfVotes = numberOfVotes;
	}
	public String getCurrentMode() {
		return currentMode;
	}
	public void setCurrentMode(String currentMode) {
		this.currentMode = currentMode;
	}
	
}
