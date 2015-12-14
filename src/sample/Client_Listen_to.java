package sample;

import java.io.*;
import java.net.*;

public class Client_Listen_to implements Runnable{
	
	protected static int C_port = 7090;
	
	public Client_Listen_to(int C_port){
		Client_Listen_to.C_port = C_port;
	}
	
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(C_port);
			while(true){
				System.out.println("");
				Socket s = ss.accept();
				InputStream is = s.getInputStream();
				ObjectInputStream ois = new ObjectInputStream(is);
				String message = (String) ois.readObject();
				if(message.substring(0, 2).equals("ED")) {
					ois.close();
					is.close();
					s.close();
					break;
				}
				else {
					ClientOps.receive(message);
					ois.close();
					is.close();
					s.close();
				}
			}
			ss.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}

