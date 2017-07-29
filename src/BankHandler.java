/**
 * @author Balakrishna Lellapalli
 *
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import ThriftService.Branch;
import ThriftService.BranchID;
import ThriftService.LocalSnapshot;
import ThriftService.SystemException;
import ThriftService.TransferMessage;

public class BankHandler implements Branch.Iface {

	private int balance = 0;
	private List<BranchID> branchList;
	private int snapShotIniated = 0;
	private boolean isInitiator = false;
	private List<String> sender = new ArrayList<String>();
	private List<String> receiver = new ArrayList<String>();
	private LocalSnapshot localSnapshot = new LocalSnapshot();
	private Map<String, Integer> transactionHistory = new HashMap<String, Integer>(); 
	
	// Returns branch's current balance
	public int getBalance(){
		return balance;
	}

	// Get list of all branch (Branch details are available in "branch" file)
	public List<BranchID> getBranchList(){
		return branchList;
	}
	
	// Assign snapshot number and current balance to local snapshot
	private void createSnapShot(int snapshot_num){
		localSnapshot.snapshot_num = snapshot_num;
		localSnapshot.balance = this.balance;
		localSnapshot.messages = new ArrayList<Integer>();
	}
	
	// Returns current snapshot number
	public int getSnapShotInitiated(){
		return snapShotIniated;
	}
	
	// Add branch name to sender's list while sending marker message
	public void setSender(String branchName){
		if(!sender.contains(branchName))
			sender.add(branchName);
	}
	
	// Add transaction amount to messages
	public void addMessage(int snapshot_num, int amount){
		if(localSnapshot.snapshot_num == snapshot_num){
			localSnapshot.messages.add(amount);
		}
	}
	
	//Returns true if has already received marker from the parameter "branchName"
	public boolean isReceiverAlreadyExist(String branchName){
		return receiver.contains(branchName);
	}
	
	// Reset values
	private void resetValues(){
		isInitiator = false;
		snapShotIniated = 0;
		sender.clear();
		receiver.clear();
		transactionHistory.clear();
		localSnapshot = new LocalSnapshot();
	}

	// Add balance and snapshot number to local snapshot and if marker was already sent and 
	//now received for the same branch, then record the amount transfered from "transactionHistory"  
	private void createLocalSnapshot(int snapshot_num, String branchName){
		synchronized(this){
			if(localSnapshot.snapshot_num == snapshot_num){
				if(!sender.contains(branchName)){
					receiver.add(branchName);
				}
				if(sender.contains(branchName) && !receiver.contains(branchName)){
					//This is the total amount that the branchA has received from the branchB from the time 
					//it sent marker and received marker from that branchB
					if(transactionHistory.containsKey(branchName))
						localSnapshot.messages.add(transactionHistory.get(branchName));
					else
						localSnapshot.messages.add(0);

					// Add branch to receiver's list once marker is received from that branch
					receiver.add(branchName);
				}
			}
			else{
				//Add balance and snapshot number to local snapshot
				createSnapShot(snapshot_num);
				if(!isInitiator && !sender.contains(branchName))
					receiver.add(branchName);
			}
		}
	}
	
	// Record initial balance before sending marker message
	public void saveInitialState(int snapshot_no){
		if(localSnapshot.snapshot_num == snapshot_no &&
				sender.isEmpty() && receiver.isEmpty()){
			localSnapshot.balance = balance; //Record balance
		}
	}
	
	// Send (deduct from balance) and Receive(add to balance) amount to/from other branch
	public void updateBalance(int amount, String branchID, boolean isDeposit) {
		synchronized (this){	
			if(isDeposit){
				this.balance += amount;
				if(sender.contains(branchID) && !receiver.contains(branchID)){
					int currentAmount = 0;
					// Keep recording amount in "transactionHistory"
					if(transactionHistory.containsKey(branchID))
						currentAmount = transactionHistory.get(branchID); 
					transactionHistory.put(branchID, currentAmount + amount);
				}		
				System.out.println("\nReceived from " + branchID + " : $" + amount + 
						"\nCurrent Balance: $" + this.balance);
				
			}
			else{
				this.balance -= amount;
				System.out.println("\nTransferred to " + branchID + " : $" + amount + 
						"\nCurrent Balance: $" + this.balance);
			}
		}
	}
	
	@Override
	//This method has two input parameters: the initial balance of a branch and a list of all branches in the
	//distributed bank. Upon receiving this method, a branch will set its initial balance and record the list of all branches.	
	public void initBranch(int balance, List<BranchID> all_branches) throws SystemException, TException {
		this.balance = balance;
		this.branchList = all_branches;	
	}

	@Override
	//Given a TransferMessage structure that contains the sending branch’s BranchID as well as the
	//amount of money, the receiving branch updates its balance accordingly
	public void transferMoney(TransferMessage message) throws SystemException, TException {
		updateBalance(message.amount, message.orig_branchId.name, true);
	}

	@Override
	//Upon receiving this call, a branch records its own local state (balance) and sends out “Marker”
	//messages to all other branches by calling the Marker method on them.
	public void initSnapshot(int snapshot_num) throws SystemException, TException {
		isInitiator = true;
		this.snapShotIniated = snapshot_num;
		createLocalSnapshot(snapshot_num, null);
	}

	@Override
	//1. if this is the first Marker message with the snapshot_num, the receiving branch records its own local state
	//(balance), records the state of the incoming channel from the sender to itself as empty, starts recording
	//on other incoming channels, and sends out Marker messages to all of its outgoing channels
	//Else the receiving branch records the state of the incoming channel as the sequence of money
	//transfers that arrived between when it recorded its local state and when it received the Marker.
	public void Marker(BranchID branchId, int snapshot_num)	throws SystemException, TException {
		System.out.println("Marker message received from << << << " + branchId.name);
		this.snapShotIniated = snapshot_num;
		createLocalSnapshot(snapshot_num, branchId.name);
		
	}

	@Override
	//Given the snapshot_num that uniquely identifies a snapshot, a branch retrieves its recorded local
	//and channel states and return them to the caller (i.e., the controller)
	public LocalSnapshot retrieveSnapshot(int snapshot_num)	throws SystemException, TException {
		if(localSnapshot.snapshot_num == snapshot_num){
			LocalSnapshot snapshot = new LocalSnapshot();
			snapshot.balance = localSnapshot.balance;
			snapshot.messages = new ArrayList<Integer>();
			for(int amount: localSnapshot.messages)
				snapshot.messages.add(amount);	
			resetValues(); // reset values
			return snapshot;
		}
		return null;
	}	
}