/**
 * @author Balakrishna Lellapalli
 *
 */
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import ThriftService.Branch;
import ThriftService.BranchID;
import ThriftService.SystemException;
import ThriftService.TransferMessage;

public class BankBranch {
	
	private static int port;
	private static BankHandler handler;
	private static Branch.Processor<BankHandler> processor;
	private static int initialBalance = 0;
	private static int snapshot_no = 0;
	private static String branchId;
	
	// Start server thread
	private static void startServer(Branch.Processor<BankHandler> processor) {
		try {
			TServerTransport serverTransport = new TServerSocket(port);
			TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));
			System.out.println("Starting Branch : " + branchId);
			server.serve();
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}
	
	// Set initial balance
	private static void setInitialBalance(){
		if (initialBalance == 0)
			initialBalance = handler.getBalance();
	}
	
	// Amount of money should be drawn randomly between 1% and 5% of the branch’s initial balance
	private static int getTransferAmount(){
		double percent = (double)(new Random().nextInt(5) + 1)/100;
		return (int) (initialBalance * percent);
	}
	
	// Get branch details (name, ip address and port number)
	private static BranchID getBranchID(){
		for (BranchID branch : handler.getBranchList()) {
			if (branch.name.equals(branchId))
				return new BranchID(branch.name, branch.ip, branch.port);
		}
		return null;
	}
	
	// It first decrease its balance, then call the transferMoney method on a remote branch
	private static void transferMoney(int depositAmount) throws SystemException, TException, InterruptedException {
		while (true) {
			int index = new Random().nextInt(handler.getBranchList().size());
			if (!handler.getBranchList().get(index).name.equals(branchId)) {
				String toBranch = handler.getBranchList().get(index).name; 
				handler.updateBalance(depositAmount, toBranch, false);
				TransferMessage message = new TransferMessage();
				message.amount = depositAmount; // Set Deposit
				message.orig_branchId = getBranchID();
				
				TTransport transport = new TSocket(handler.getBranchList().get(index).ip, 
						handler.getBranchList().get(index).port);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				Branch.Client client = new Branch.Client(protocol);
				client.transferMoney(message);
				transport.close();
				break;
			}
		}
	}

	// Sends marker message to all branch from the branch list
	private static void sendMarker() throws SystemException, TException, InterruptedException{
		for(BranchID branch : handler.getBranchList()){
			if(!branch.name.equals(branchId)){
				System.out.println("Marker message sent to >> >> >> " + branch.name);
				if(handler.isReceiverAlreadyExist(branch.name)){
					handler.addMessage(snapshot_no, 0);
				}
				handler.setSender(branch.name); // add sender branch name
			}
		}
		Thread.sleep(1000); // Explicitly adding sending delay for marker message
		for(BranchID branch : handler.getBranchList()){
			if(!branch.name.equals(branchId)){
				TTransport transport = new TSocket(branch.ip, branch.port);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				Branch.Client client = new Branch.Client(protocol);
				client.Marker(getBranchID(), snapshot_no);
				transport.close();		
			}
		}	
	}
	
	// If snapshot initiated then save initial state and send marker message to other branch from the list of branches
	// Else transfer money after random interval of 0 to 5 second
	private static void initiateTransaction(Branch.Processor<BankHandler> processor){		
		try{
			while (true) {
				Thread.sleep(100);
				setInitialBalance();
				if(initialBalance > 0){
					if(handler.getSnapShotInitiated() > snapshot_no){
						snapshot_no = handler.getSnapShotInitiated();
						handler.saveInitialState(snapshot_no);
						sendMarker();
					}
					else{
						int depositAmount = getTransferAmount();
						if(depositAmount > 0 && handler.getBalance() >= depositAmount){	
							transferMoney(depositAmount);	
							Thread.sleep(new Random().nextInt(6) * 1000);
						}
					}
				}
			}
		}catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}
	
	public static void main(String[] args) {
		try {
			handler = new BankHandler();
			processor = new Branch.Processor<BankHandler>(handler);
			branchId = args[0];
			port = Integer.valueOf(args[1]);
			
			// Starts server thread
			Runnable server = new Runnable() {
			public void run() {
				startServer(processor);
				}
			};			
			// Starts another thread to transfer money and send marker message
			Runnable client = new Runnable() {
			public void run() {
				initiateTransaction(processor);
				}
			};
			new Thread(server).start();
			new Thread(client).start();
			
		}catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}
}