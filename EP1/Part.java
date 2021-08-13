import java.util.HashMap;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Part extends Remote {
	void setCod(int c) throws RemoteException;
	void setNome(String n) throws RemoteException;
	void setDesc(String d) throws RemoteException;
	void setSublist(HashMap <Part, Integer> sublist) throws RemoteException;
	String getRepNome() throws RemoteException;
	void setRepNome(String sn) throws RemoteException;
	int getCod() throws RemoteException;
	String getNome() throws RemoteException;
	String getDesc() throws RemoteException;
	HashMap <Part, Integer> getSublist() throws RemoteException;
}