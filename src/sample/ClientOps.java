package sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import net.Address;
import net.IOControl;
import net.Session;
import sample.log.Utils;
import util.FileHelper;
import util.Log;

import com.google.gson.Gson;

public class ClientOps {
	
	private static IOControl control=new IOControl();
	private static final Log log=Log.get();
	private static final long MAX_VALUE = 0xFFFFFFFFL;
	private static String pathname = "/home/groupe/E2_Box/Client/";
	private static String M_ip = "localhost", C_ip = "localhost";
	private static int M_port = 7080, C_port = 7090;
	private static String received_map = null;
	private static String received_ipconfigs = "";
	private static boolean received_ = false;
	private static Gson gson = new Gson();
	private static int firsttime = 1;
	private static Node map = null;
	
	public static void initMap(){
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + "Monitor_Listner_Details.txt"));
			M_ip = br.readLine().trim();
			M_port = Integer.parseInt(br.readLine().trim());
			System.out.println("Monitor ip is " + M_ip + " and Monitor port is " + M_port);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + "Client_Listner_Details.txt"));
			C_ip = br.readLine().trim();
			C_port = Integer.parseInt(br.readLine().trim());
			System.out.println("Client ip is " + C_ip + " and Client port is " + C_port);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Call_Listner();
		System.out.println("will send map request ");
		send_To("MR_" + C_ip + ":" + C_port, M_ip + ":" + M_port);
		System.out.println("Map request sent ");
	}
	
	public static List<Integer> readRequest(String request_on){
		if(firsttime == 1) {
			firsttime = 0;
			initMap();
			while(true) {
				System.out.print("");
				if(received_map != null) {
					map =  gson.fromJson(received_map, Node.class);
					System.out.println("map received ");
					String json = gson.toJson(map);
					System.out.println(json);
					received_map = null;
					send_To("MA", M_ip + ":" + M_port);
					break;
				}
			}
		}
		String res[] = null;
		res = selectInMap(map, request_on);
		List<Integer> num = new ArrayList<Integer>();
		for(String x : res){
			String serverIP = x.split(":")[0];
			num.add(Integer.parseInt(serverIP.substring(serverIP.length() - 3, serverIP.length())));
		}
		String send = "LOOK_" + C_ip + ":" + C_port + "_" + request_on;
		String sendto = res[0].split(":")[0] + ":" + (Integer.parseInt(res[0].split(":")[1]) + 2);
		send_To(send, sendto);
		while(true) {
			System.out.print("");
			if(received_ == true) {
				received_ = false;
				if(received_ipconfigs.length() != 0) {
					res = received_ipconfigs.split(",");
					received_ipconfigs = "";
					System.out.println("Received ipconfig from lookup is " + res[0] + " " + res[1] + " and reading from it ");
					my_read(request_on, res);
				}
				else {
					System.out.println("reading from " + res[0] + " and "+ res[1]);
					my_read(request_on, res);
				}
				break;
			}
		}
		if(received_map != null) {
			map =  gson.fromJson(received_map, Node.class);
			System.out.println("map received ");
			String json = gson.toJson(map);
			System.out.println(json);
			received_map = null;
			send_To("MA", M_ip + ":" + M_port);
		}
		return num;
	}
	
	public static List<Integer> createFile(String file, long size){
		if(firsttime == 1) {
			firsttime = 0;
			initMap();
			while(true) {
				System.out.print("");
				if(received_map != null) {
					map =  gson.fromJson(received_map, Node.class);
					System.out.println("map received ");
					String json = gson.toJson(map);
					System.out.println(json);
					received_map = null;
					send_To("MA", M_ip + ":" + M_port);
					break;
				}
			}
		}
		String filename = "/home/groupe/E2_Box/Client/Input/" + file;
		String res[] = null;
		res = selectInMap(map, file);
		List<Integer> num = new ArrayList<Integer>();
		for(String x : res){
			String serverIP = x.split(":")[0];
			num.add(Integer.parseInt(serverIP.substring(serverIP.length() - 3, serverIP.length())));
		}
		try {
	           RandomAccessFile f = new RandomAccessFile(filename, "rw");
	           if(size < 1024*1024*2)
	        	   f.setLength(size);
	           else
	        	   f.setLength(1024*1024*2);
	           f.close();
	           send_To("WR_" + file, M_ip + ":" + M_port);
	           my_write(file, res);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return num;
	}
	
	public static List<Integer> writeRequest(String request_on){
		if(firsttime == 1) {
			firsttime = 0;
			initMap();
			while(true) {
				System.out.print("");
				if(received_map != null) {
					map =  gson.fromJson(received_map, Node.class);
					System.out.println("map received ");
					String json = gson.toJson(map);
					System.out.println(json);
					received_map = null;
					send_To("MA", M_ip + ":" + M_port);
					break;
				}
			}
		}
		String res[] = null;
		res = selectInMap(map, request_on);
		List<Integer> num = new ArrayList<Integer>();
		for(String x : res){
			String serverIP = x.split(":")[0];
			num.add(Integer.parseInt(serverIP.substring(serverIP.length() - 3, serverIP.length())));
		}
		String send = "LOOK_" + C_ip + ":" + C_port + "_" + request_on;
		String sendto = res[0].split(":")[0] + ":" + (Integer.parseInt(res[0].split(":")[1]) + 2);
		send_To(send, sendto);
		while(true) {
			System.out.print("");
			if(received_ == true) {
				received_ = false;
				if(received_ipconfigs.length() != 0) {
					res = received_ipconfigs.split(",");
					received_ipconfigs = "";
					System.out.println("Received ipconfig from lookup is " + res[0] + " " + res[1] + " and writing to it");
					my_write(request_on, res);
				}
				else {
					System.out.println("writing to " + res[0] + " and "+ res[1]);
					my_write(request_on, res);
				}
				break;
			}
		}
		if(received_map != null) {
			map =  gson.fromJson(received_map, Node.class);
			System.out.println("map received ");
			String json = gson.toJson(map);
			System.out.println(json);
			received_map = null;
			send_To("MA", M_ip + ":" + M_port);
		}
		return num;
	}
	
	public static void receive(String msg) {
	
		String msg_type = msg.substring(0, 2);
		if (msg_type.equals("MA")) {
			String json = msg.substring(3, msg.length());
			received_map = json;
		}
		else if(msg_type.equals("LO")) {
			String ipconfig = msg.substring(5, msg.length());
			received_ = true;
			received_ipconfigs = ipconfig;
		}
	}
	
	private static void my_read(String request_on, String[] res) {
		try{
			Utils.connectToLogServer(log);
			String serverIP = res[0].split(":")[0];
			int serverPort = (Integer.parseInt(res[0].split(":")[1]));
			String path_osd = "/home/groupe/E2_Box/OSD/OSD";
			path_osd = path_osd + serverIP.substring(serverIP.length() - 1, serverIP.length()) + "/Files/";
			long temp = readFile(control, serverIP, serverPort, path_osd + request_on, 0, 0);
			log.i("Read: " + temp);
			System.out.println(temp);
		}
		catch(Exception e){
			try{
				Utils.connectToLogServer(log);
				String serverIP = res[1].split(":")[0];
				int serverPort = (Integer.parseInt(res[1].split(":")[1]));
				String path_osd = "/home/groupe/E2_Box/OSD/OSD";
				path_osd = path_osd + serverIP.substring(serverIP.length() - 1, serverIP.length()) + "/Files/";
				long temp = readFile(control, serverIP, serverPort, path_osd + request_on, 0, 0);
				log.i("Read: " + temp);
				System.out.println("Read " + temp + " bytes");
			}
			catch(Exception ex){
				ex.printStackTrace();
				log.w(ex);
			}
		}
	}
	
	private static void my_write(String request_on, String[] res) {
		
		String path_osd = "/home/groupe/E2_Box/Client/Input/";
		try{
			Utils.connectToLogServer(log);
			for (int i = 0; i < res.length; i++) {
				res[i] = res[i].split(":")[0] + ":" +(Integer.parseInt(res[i].split(":")[1]) + 1);
			}
			ArrayList<Address> addresses = splitAddress(res,0);
			if(upload(control, path_osd + request_on, addresses)) {
				log.i("File upload success.");
				System.out.println("File upload success.");
			}
			else {
				log.i("File upload fails.");
				System.out.println("File upload fails.");
			}
		}catch(Exception e){
			e.printStackTrace();
			log.w(e);
		}
	}
	
	public static void Call_Listner() {
		
		(new Thread() {
			public void run() {
				new Client_Listen_to(C_port).run();
			}
		}).start();
	}
	
	public static void send_To(String msg, String ipconfig) {
		
		try {
			Socket s = new Socket(ipconfig.split(":")[0], Integer.parseInt(ipconfig.split(":")[1]));
			OutputStream os = s.getOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(msg);
			oos.close();
			os.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
		}
	}
	
	static Session downloadFile(IOControl control,String ip,int port,String path,long position,long limit) throws Exception{
		Session session=new Session(FileReadMsgType.READ_FILE);
		session.set("path",path);
		if(position>0)
			session.set("position",position);
		if(limit>0)
			session.set("limit",limit);
		return control.request(session,ip,port);
	}

	static String downloadToTemp(Path tempDir,IOControl control,String ip,int port,String path){
		try{
			Session response=downloadFile(control,ip,port,path,0,0);
			if(response.getType()!=FileReadMsgType.READ_FILE_OK) return null;
			File newFile=new File(tempDir.toFile(),response.getString("name"));
			newFile.createNewFile();
			FileOutputStream fos=new FileOutputStream(newFile);
			FileHelper.download(response.getSocketChannel(),fos.getChannel(),response.getLong("size"));
			fos.close();
			return newFile.getAbsolutePath();
		}catch(Exception e){
			e.printStackTrace();
			log.w(e);
			return null;
		}
	}
	
	static long readFile(IOControl control,String ip,int port,String path,long position,long limit){
		try{
			Session response=downloadFile(control,ip,port,path,position,limit);
			if(response.getType()!=FileReadMsgType.READ_FILE_OK) return 0;
			long size=response.getLong("size");
			SocketChannel src=response.getSocketChannel();
			ByteBuffer buffer=ByteBuffer.allocateDirect(1024*128);
			long read=0;
			while(read<size){
				System.out.print("");
				long read_once=0;
				while(buffer.hasRemaining() && read<size){
					System.out.print("");
					read_once=src.read(buffer);
					if(read_once<0) break;
					read+=read_once;
				}
				if(read_once<0) break;
				if(read<size)
					buffer.clear();
			}
			return read;
		}catch(Exception e){
			e.printStackTrace();
			log.w(e);
			return 0;
		}
	}
	
	static long timeout=60*1000;    //  60 seconds

	static boolean upload(IOControl control,String path,ArrayList<Address> addresses,long position){
		try{
			File file=new File(path);
			FileInputStream fis=new FileInputStream(file);
			FileChannel src=fis.getChannel();
			Session req=new Session(FileWriteMsgType.WRITE_CHUNK);
			String id=file.getName();
			long size=file.length();
			req.set("id",id);
			req.set("size",size);
			req.set("timeout",timeout);
			req.set("address",addresses);
			if(position>0)
				req.set("position",position);
			control.send(req,addresses.get(0));
			SocketChannel dest=req.getSocketChannel();
			FileHelper.upload(src,dest,size);
			fis.close();
			Session result=control.get(req);
			return result.getType()==FileWriteMsgType.WRITE_OK;
		}catch(Exception e){
			e.printStackTrace();
			log.w(e);
			return false;
		}
	}
	
	static boolean upload(IOControl control,String path,ArrayList<Address> addresses){
		return upload(control,path,addresses,0);
	}
	
	static ArrayList<Address> splitAddress(String[] tokens,int start){
		ArrayList<Address> result=new ArrayList<>();
		for(int i=start;i<tokens.length;++i){
			String[] parts=tokens[i].split(":");
			if(parts.length!=2) return null;
			try{
				int port=Integer.parseInt(parts[1]);
				Address address=new Address(parts[0],port);
				result.add(address);
			}catch(NumberFormatException e){e.printStackTrace();return null;}
		}
		return result;
	}
	
	public static String[] selectInMap(Node clustermap, String fileName) {
		
		String FileName = "PlacementRules.txt";
		String str = "";
		int val = 0;
		int rin_row = 0;
		int rin_cabinet = 0;
		int rin_osd = 0;
		try {
			BufferedReader ReadFile = new BufferedReader(new FileReader(pathname + FileName));
			while ((str = ReadFile.readLine()) != null) {
				System.out.print("");
				if (val == 0) {
					rin_row = Character.getNumericValue(str.charAt(0));
					;
					val++;
				} else if (val == 1) {
					rin_cabinet = Character.getNumericValue(str.charAt(0));
					;
					val++;
				} else {
					rin_osd = Character.getNumericValue(str.charAt(0));
					;
				}
			}
			ReadFile.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(ex);
		}
		String ips_ports[] = new String[rin_row * rin_cabinet *rin_osd];
		int sel_row[] = new int[rin_row];
		int sel_cabinet[] = new int[rin_cabinet * rin_row];
		int sel_osd[] = new int[rin_osd * rin_cabinet * rin_row];
		int til_row = rin_row;
		int pos_row = 0;
		for (int i = 0; i < til_row; i++) {
			double h1;
			String uname;
			int res = 1;
			Node traversemap = clustermap;
			while (traversemap.function == 1) {
				System.out.print("");
				uname = traversemap.uname;
				h1 = hashMix(fileName,i,uname);
				if (traversemap.val != 1) {
					if (h1 > traversemap.val) {
						traversemap = traversemap.right;
						res++;
					} else {
						traversemap = traversemap.left;
					}
				}
				else {
					traversemap = traversemap.left;
				}
			}
			int got = 0;
			for (int j = 0; j < pos_row; j++) {
				if (sel_row[j] == res) {
					got = 1;
				}
			}
			if (got == 1) {
				til_row++;
			} else {
				sel_row[pos_row] = res;
				pos_row++;
			}
		}
		int pos_cabinet = 0;
		int checkfromhere1 = 0;
		for (int m = 0; m < rin_row; m++) {
			Node traversemap1 = clustermap;
			for (int n = 1; n < sel_row[m]; n++) {
				traversemap1 = traversemap1.right;
			}
			if (traversemap1.function == 1) {
				traversemap1 = traversemap1.left;
			}
			int til_cabinet = rin_cabinet;
			for (int i = 0; i < til_cabinet; i++) {
				double h2;
				String uname;
				int res = 1;
				Node traversemap2 = traversemap1;
				while (traversemap2.function == 2) {
					System.out.print("");
					uname = traversemap2.uname;
					h2 = hashMix(fileName,i,uname);
					if (traversemap2.val != 1) {
						if (h2 > traversemap2.val) {
							traversemap2 = traversemap2.right;
							res++;
						} else {
							traversemap2 = traversemap2.left;
						}
					}
					else {
						traversemap2 = traversemap2.left;
					}
				}
				int got = 0;
				for (int j = checkfromhere1; j < pos_cabinet; j++) {
					if (sel_cabinet[j] == res) {
						got = 1;
					}
				}
				if (got == 1) {
					til_cabinet++;
				} else {
					sel_cabinet[pos_cabinet] = res;
					pos_cabinet++;
				}
			}
			checkfromhere1 = pos_cabinet;
		}
		int pos_osd = 0;
		int start1 = 0;
		int end1 = rin_cabinet;
		for (int m = 0; m < rin_row; m++) {
			Node traversemap1 = clustermap;
			for (int n = 1; n < sel_row[m]; n++) {
				traversemap1 = traversemap1.right;
			}
			if (traversemap1.function == 1) {
				traversemap1 = traversemap1.left;
			}
			for (int a = start1; a < end1; a++) {
				Node traversemap2 = traversemap1;
				for (int b = 1; b < sel_cabinet[a]; b++) {
					traversemap2 = traversemap2.right;
				}
				if (traversemap2.function == 2) {
					traversemap2 = traversemap2.left;
				}
				int checkfromhere = 0;
				int til_osd = rin_osd;
				for (int i = 0; i < til_osd; i++) {
					double h3;
					String uname;
					int res = 0;
					String ip_port = "";
					String status;
					Node traversemap3 = traversemap2;
					while (true) {
						System.out.print("");
						uname = traversemap3.uname;
						h3 = hashMix(fileName,i,uname);
						if (traversemap3.val != 1) {
							if (h3 > traversemap3.val) {
								traversemap3 = traversemap3.right;
							} else {
								res = traversemap3.name.id;
								String ip1 = traversemap3.name.ip;
								int port1 = traversemap3.name.port;
								status = traversemap3.name.status;
								ip_port = ip1 + ":" + port1;
								break;
							}
						} else {
							res = traversemap3.name.id;
							String ip1 = traversemap3.name.ip;
							int port1 = traversemap3.name.port;
							status = traversemap3.name.status;
							ip_port = ip1 + ":" + port1;
							break;
						}
					}
					if (status.equals("F") || status.equals("O")) {
						til_osd++;
					}
					else {
						int got = 0;
						for (int j = checkfromhere; j < pos_osd; j++) {
							if (sel_osd[j] == res) {
								got = 1;
							}
						}
						if (got == 1) {
							til_osd++;
						} else {
							sel_osd[pos_osd] = res;
							ips_ports[pos_osd] = ip_port;
							pos_osd++;
						}
					}
				}
				checkfromhere = pos_osd;
			}
			start1 = start1 + rin_cabinet;
			end1 = end1 + rin_cabinet;

		}
		return ips_ports;
	}
	private static long subtract(long val, long subtract) {
		
		return (val - subtract) & MAX_VALUE;
	}

	private static long xor(long val, long xor) {
		
		return (val ^ xor) & MAX_VALUE;
	}

	private static long leftShift(long val, int shift) {
		
		return (val << shift) & MAX_VALUE;
	}

	private static double hashMix(String fileName, int r, String uname) {
		
		long a, b, c;
		double hash;
		a = fileName.hashCode();
		b = (long) r;
		c = uname.hashCode();
		a = subtract(a, b);
		a = subtract(a, c);
		a = xor(a, c >> 13);
		b = subtract(b, c);
		b = subtract(b, a);
		b = xor(b, leftShift(a, 8));
		c = subtract(c, a);
		c = subtract(c, b);
		c = xor(c, (b >> 13));
		a = subtract(a, b);
		a = subtract(a, c);
		a = xor(a, (c >> 12));
		b = subtract(b, c);
		b = subtract(b, a);
		b = xor(b, leftShift(a, 16));
		c = subtract(c, a);
		c = subtract(c, b);
		c = xor(c, (b >> 5));
		a = subtract(a, b);
		a = subtract(a, c);
		a = xor(a, (c >> 3));
		b = subtract(b, c);
		b = subtract(b, a);
		b = xor(b, leftShift(a, 10));
		c = subtract(c, a);
		c = subtract(c, b);
		c = xor(c, (b >> 15));
		int res1 = (int)c % 12345;
		hash = res1 / 12345.0;
		if(hash < 0)
			return -hash;
		else
			return hash;
	}		
}
