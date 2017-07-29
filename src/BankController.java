/**
 * @author Balakrishna Lellapalli
 *
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import ThriftService.Branch;
import ThriftService.BranchID;
import ThriftService.LocalSnapshot;
import ThriftService.SystemException;

public class BankController {
	
	private static List<BranchID> branchList = new ArrayList<BranchID>();
	
	// Records list of all branches from file : "branch"
	private static void readFile(String fileName) throws IOException {		
		String line = null;
		BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
		while ((line = bufferedReader.readLine()) != null){
			String [] branchDetails = line.split(" ");
			BranchID branchID = new BranchID(branchDetails[0], branchDetails[1], Integer.valueOf(branchDetails[2]));
			branchList.add(branchID);
		}
		bufferedReader.close();
	}
	
	// Open TTransport and Calls initBranch method of BankHandler class
	private static void initBranch(BranchID branch, int amount) throws SystemException, TException{
		TTransport transport = new TSocket(branch.ip, branch.port);
		transport.open();	
		TProtocol protocol = new TBinaryProtocol(transport);
		Branch.Client client = new Branch.Client(protocol);
		client.initBranch(amount, branchList);
		transport.close();
	}
	
	// Open TTransport and Calls initSnapshot method of BankHandler class
	private static void initSnapshot(BranchID branch, int snapshot_num) throws SystemException, TException{
		System.out.println("\nSnapshot initiated on " + branch.name);
		TTransport transport = new TSocket(branch.ip, branch.port);
		transport.open();	
		TProtocol protocol = new TBinaryProtocol(transport);
		Branch.Client client = new Branch.Client(protocol);
		client.initSnapshot(snapshot_num);
		transport.close();
	}
	
	// Open TTransport and Calls retrieveSnapshot method of BankHandler class (Displays balance and message)
	private static void retrieveSnapshot(BranchID branch, int snapshot_num) throws SystemException, TException{
		StringBuilder channel = new StringBuilder();
		TTransport transport = new TSocket(branch.ip, branch.port);
		transport.open();	
		TProtocol protocol = new TBinaryProtocol(transport);
		Branch.Client client = new Branch.Client(protocol);
		LocalSnapshot snapshot = client.retrieveSnapshot(snapshot_num);
		if(snapshot != null){
			System.out.println("Balance: " + snapshot.balance);
			for(int message : snapshot.messages)
				channel.append("{" + message + "} ");
			System.out.println("Messsage: " + channel.toString());
		}
		transport.close();
	}
	
	public static void main(String[] args) {
		try {
			int snapshot_num = 1;
			readFile(args[1]);
			int amount = Integer.valueOf(args[0]) / branchList.size();
			for(BranchID branch : branchList){			
				initBranch(branch, amount);
			}	
			Thread.sleep(5000);	//Starts sending marker message after 5 seconds
			while(true){
				int index = new Random().nextInt(branchList.size());
				initSnapshot(branchList.get(index), snapshot_num);
				Thread.sleep(branchList.size() * 4000);
				for(BranchID branch : branchList){
					System.out.println("\nSnapshot #" +  snapshot_num + " for " + branch.name + ":");
					retrieveSnapshot(branch, snapshot_num);
				}
				snapshot_num++;
				Thread.sleep(2000);	
			}			
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}