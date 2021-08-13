import java.rmi.registry.Registry;
import java.util.LinkedList;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.Remote;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.io.Serializable;

public class Servidor implements Part,PartRepository,Serializable {

	int cod;
	String nome;
	String desc;
	String s_nome;
	HashMap <Part, Integer> sublist = new HashMap<Part, Integer>();
	LinkedList<Part> lista;
	Registry registry;
	int port;


	//PartRepository rep;
	public Servidor() {
		try {
			this.lista = new LinkedList<>();
			this.registry = LocateRegistry.getRegistry();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main (String [] args) {
		Scanner scan = new Scanner(System.in);
		int count = 0;
		while (count <= 0) {
			System.out.print("Digite o numero de servidores que deseja abrir: ");
			count = Integer.parseInt(scan.nextLine());
		}
		String [] servnome = new String [count];
		for (int i = 0; i < count; i++) {
			System.out.print("Digite o nome do servidor " + (i + 1) + ": ");
			servnome [i] = scan.nextLine();
			//System.out.println(servnome[i]);
			construir(servnome[i],i);
		}
		System.out.println("----------------Nome dos servidores----------------");
		for (int i = 0; i < servnome.length; i++) System.out.println("Nome do servidor " + (i + 1) + ": " + servnome[i]);
		System.out.println("---------------------------------------------------");



	}
	public static int construir(String n,int count) {
		try {
			Servidor obj = new Servidor();
			obj.setRepNome(n);
			obj.port = count;
			PartRepository stub = (PartRepository) UnicastRemoteObject.exportObject(obj, count);
			obj.registry.bind(n, stub);
		}
		catch (Exception e) {
			System.out.println("ERRO DE SERVIDOR: ");
			e.printStackTrace();
			return 0;
		}
		return (1);
	}

	public String limpa() {
		try {
			this.lista.clear();
		}
		catch (Exception e) {
			e.printStackTrace();
			return "Erro";
		}
		return "Repositorio limpo com sucesso!";
	}
	public String showRep() throws RemoteException {
		Part p;
		Part subp;
		String pp = "";
		pp = pp + "NOME DO SERVIDOR: " + getRepNome() + " Quantidade de pecas: " + this.lista.size() +" \n";
		if (this.lista.isEmpty()) pp = pp + "SERVIDOR VAZIO \n";
		for (int i = 0; i < this.lista.size(); i++) {
			p = this.lista.get(i);
			pp = pp + "Peca Numero " + (i + 1) + " \n";
			pp = pp +"Codigo: " + p.getCod() + " Nome: " + p.getNome() + " \n" + "Descricao: " +p.getDesc() + " \n";
			if (!p.getSublist().isEmpty()) {
				pp = pp + "-----------SUBCOMPONENTES--------------\n";
				for (Map.Entry<Part,Integer> pair : p.getSublist().entrySet()) {
					subp = pair.getKey();
					pp = pp + "Codigo: " + subp.getCod() + " Nome: " + subp.getNome() +" Quantidade: "+ pair.getValue() + " \n" + "Descricao: " +subp.getDesc() + " \n";

				}
				pp = pp + "---------------------------------------\n";
			}
			else pp = pp + "Esta peca nao tem subcomponentes \n";
		}
		//pp = pp + "\n";
		return pp;
	}

	public Part getPart(int cod) throws RemoteException {
		for (int i = 0; i < this.lista.size(); i++) if (this.lista.get(i).getCod() == cod) return this.lista.get(i);
			return null;
	}

	public Part getStub (int cod) throws RemoteException {
		Part p = getPart(cod);
		if (p == null) return null;
		else return (Part) UnicastRemoteObject.toStub(p);
	} 

	public Part addRep (int cod, String nome, String desc, HashMap <Part, Integer> sub_atual) throws RemoteException {
		/*this.lista.add(p);

		p = this.lista.get(this.lista.indexOf(p));
		System.out.println("Nome: " + p.getNome() +" Codigo: "+ p.getCod() +" \n" +
							"Descricao: " + p.getDesc());*/
		try {
			Part p = new Servidor();
			p.setCod(cod);
			p.setNome(nome);
			p.setDesc(desc);
			p.setRepNome(this.getRepNome());
			p.setSublist(sub_atual);
			this.lista.add(p);
			Part stub = (Part) UnicastRemoteObject.exportObject(this.lista.get(lista.indexOf(p)),this.port);
			return stub;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		//lista.add(p);
	}
	public String addStub (Part p) throws RemoteException {
		lista.add(p);
		return ("Peca: " + p.getNome() + " (CODIGO: " + p.getCod() + ") Adicionada com sucesso ao servidor: " + getRepNome());
	}


	public void setRepNome(String sn) {
		s_nome = sn;
	}
	public String getRepNome() {return s_nome;}




	public void setCod (int c) {
		cod = c;
	}
	public void setNome (String n) {
		nome = n;
	}
	public void setDesc (String d) {
		desc = d;
	}
	public void setSublist (HashMap <Part, Integer> sl) {
		sublist = sl;
	}
	public void setRep(String s) throws RemoteException {

	}

	public int getCod() {return cod;}
	public String getNome() {return nome;}
	public String getDesc() {return desc;}
	public HashMap <Part, Integer> getSublist() {return sublist;}

}