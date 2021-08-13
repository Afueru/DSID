import java.util.LinkedList;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

public interface PartRepository extends Remote {

	Part addRep (int cod, String nome, String desc, HashMap <Part, Integer> sub_atual) throws RemoteException;
	String addStub (Part p) throws RemoteException;
	String getRepNome() throws RemoteException;
	void setRepNome(String sn) throws RemoteException;
	String showRep() throws RemoteException;
	String limpa() throws RemoteException;
	Part getPart(int cod) throws RemoteException;
	Part getStub(int cod) throws RemoteException;
}