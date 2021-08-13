import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;
import java.lang.Math;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;

public class Cliente {
	public static void main (String [] args) {
		PartRepository stub;
		Part p_atual = new Servidor();
		p_atual = null;
		HashMap <Part, Integer> sub_atual = new HashMap<Part, Integer>();
		try {
			String host = (args.length < 1) ? null : args[0];
			//System.out.println("HOST: " + host);
			Registry registry = LocateRegistry.getRegistry(host);
			Scanner l = new Scanner(System.in);
			System.out.print("Digite o nome do servidor a qual deseja se conectar: ");
			String serv = l.nextLine();
			stub = (PartRepository) registry.lookup(serv);
			System.out.println("Conectado ao servidor " + serv + " com sucesso!");
			//System.out.println("RESPOSTA DO STUB: " + stub.addRep());

			idle(stub,p_atual,sub_atual,registry);
		}
		catch (Exception e) {
			System.out.println("ERRO DE CLIENTE: ");
			e.printStackTrace();
		}
	}
	public static void idle (PartRepository pr_atual, Part p_atual, HashMap <Part, Integer> sub_atual, Registry registry) throws RemoteException{

		Scanner ler = new Scanner(System.in);
		String comando;
		while (true) {
			comando = ler.nextLine();


			switch(comando) {
				case "addpc":
				System.out.println(pr_atual.addStub(p_atual));
				break;

				case "listp":
				System.out.print(pr_atual.showRep());
				break;

				case "swaplist":
				p_atual.setSublist(sub_atual);
				System.out.println("Lista de subcomponentes corrente adicionada no lugar da antiga na peca: " + p_atual.getNome());
				break;

				case "showp":
				mostraAtributos(p_atual);
				break;

				case "clearlist":
				sub_atual.clear();
				System.out.println("Lista de subcomponentes limpa com sucesso!");
				break;

				case "help":
				mostraComandos();
				break;

				case "quit":
				ler.close();
				return;

				default:
				if (comando.startsWith("bind ")) pr_atual = bind(comando.split(" "),registry);
				else if (comando.startsWith("getp ")) p_atual = procura(comando.split(" "),pr_atual,p_atual);
				else if (comando.startsWith("addp ")) p_atual = adiciona(comando.split(" "),p_atual,pr_atual,sub_atual, registry);
				else if (comando.startsWith("addsubpart"))adicionaSub(comando.split(" "),p_atual,pr_atual,sub_atual);
				else System.out.println("Comando invalido. Digite help para ver os comandos");
			}
		}
	}


	public static PartRepository bind (String [] sp, Registry registry) {
		try {
			PartRepository stub;
			String at = "";
			for (int i = 1; i < sp.length; i++) at = at + sp[i] + " ";
			at = at.trim();
			stub = (PartRepository) registry.lookup(at);
			System.out.println("Conectado agora ao servidor: " + at);
			return stub;
		}
		catch (Exception e) {
			System.out.print("ERRO AO TENTAR TROCAR DE SERVIDOR: ");
			e.printStackTrace();
		}
		return null;
	}

	public static Part procura (String [] sp, PartRepository stub, Part p_atual) throws RemoteException{
		if (sp.length <= 1) {
			System.out.println("Especifique o codigo!");
			return p_atual;
		}
		Part p = stub.getStub(Integer.parseInt(sp[1]));
		if (p == null) {
			System.out.println("Peca nao encontrada");
			return p_atual;
		}
		System.out.println("Peca selecionada: " + p.getNome() + " Codigo: " + p.getCod());
		return p;
	}

	public static Part adiciona (String [] sp, Part old, PartRepository stub, HashMap <Part, Integer> sub_atual, Registry registry) throws RemoteException {		

		String aux = "";
		for (int i = 2; i < sp.length; i++) aux = (aux + sp[i] + " ");
		try {
			int cod = (int) (Math.random() * 100000);
			Part p =  stub.addRep(cod,sp[1],aux,sub_atual);
			System.out.println("Peca " + p.getNome() + " incluida com sucesso!");
			return p;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void adicionaSub (String [] sp, Part p, PartRepository stub, HashMap <Part, Integer> sub_atual) throws RemoteException {
		if (sp.length <= 1) {
			System.out.println("Especifique a quantidade!");
			return;
		}
		sub_atual.put(p,Integer.parseInt(sp[1]));
		System.out.println("Peca adicionada a lista de subcomponentes corrente!");
	}

	public static void mostraAtributos (Part p) throws RemoteException {
		if (p == null) {
			System.out.println("Nao ha nenhuma peca selecionada no momento");
			return;
		}
		//Part p = stub.getPart(pzao.getCod());
		Part pzinho;
		System.out.println("Pertencente ao servidor: " + p.getRepNome());
		System.out.println("Nome: " + p.getNome() +" Codigo: "+ p.getCod() +" \n" + "Descricao: " + p.getDesc());
		if (!p.getSublist().isEmpty()) {
			System.out.println("--------LISTA DE SUBCOMPONENTES--------");
			for (Map.Entry<Part,Integer> pair :p.getSublist().entrySet()) {
				pzinho = pair.getKey();
				System.out.println("Codigo: " + pzinho.getCod() + " Nome: " + pzinho.getNome() +" Quantidade: "+ pair.getValue() + " \n" + pzinho.getDesc());
			}
			System.out.println("---------------------------------------");
		}
		else System.out.println("A lista de subcomponentes corrente esta vazia");
	}








	public static void mostraComandos() {
		System.out.println("-bind (nome do servidor): Faz o cliente se conectar a outro servidor e muda o repositorio corrente. Este comando recebe o nome de um repositorio e obtem do servico de nomes uma referencia para esse repositorio, que passa a ser o repositorio corrente.");
		System.out.println("-listp : Lista as pecas do repositorio corrente.");
		System.out.println("-getp (codigo da peca) : Busca uma peca por codigo. A busca e efetuada no repositorio corrente. Se encontrada, a peca passa a ser a nova peca corrente.");
		System.out.println("-showp : Mostra atributos da peca corrente.");
		System.out.println("-clearlist : Esvazia a lista de sub-pecas corrente.");
		System.out.println("-addsubpart (quantidade) : Adiciona a lista de sub-pecas corrente n unidades da peca corrente.");
		System.out.println("-addp (nome da peca) (descricao): Adiciona uma peca ao repositorio corrente. A lista de sub-pecas corrente e usada como lista de subcomponentes diretos da nova peca. (E so para isto que existe a lista de sub-pecas corrente.)");
		System.out.println("-addpc : Adiciona a peca corrente a lista do servidor atual");
		System.out.println("-quit : Encerra a execucao do cliente.");

	}
}