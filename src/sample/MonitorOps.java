package sample;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;

import net.Address;
import net.IOControl;
import net.Session;
import util.FileHelper;
import util.Log;

public class MonitorOps {
	
	private static int count_transfer = 0;
	private static IOControl control=new IOControl();
	private static int M_port = 7080;
	private static final long MAX_VALUE = 0xFFFFFFFFL;
	private static String F_osd_ipconfig = null;
	private static String O_osd_ipconfig = null;
	private static String pathname = "/home/groupe/E2_Box/Monitor/";
	private static LinkedList<String> AR_msg_queue = new LinkedList<String>();
	private static LinkedList<String> MR_msg_queue = new LinkedList<String>();
	private static LinkedList<String> RC_msg_queue = new LinkedList<String>();
	private static List<String> Clients = new ArrayList<String>();
	private static List<String> Osds_looked = new ArrayList<String>();
	private static Gson gson = new Gson();
	private static int num_msg_back = 0;
	private static final Log log = Log.get();
	
	public static void main(String[] args) {
		
		Node map = null;
		String mapfile = "clustermap.json";
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + mapfile));
			map = gson.fromJson(br, Node.class);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("map created for first time");
		String json_map = gson.toJson(map);
		System.out.println("map is " + json_map);
		try {
			BufferedReader br = new BufferedReader(new FileReader(pathname + "Monitor_Listner_Details.txt"));
			br.readLine();
			M_port = Integer.parseInt(br.readLine().trim());
			System.out.println("monitor port number is " + M_port);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Call_Listner();
		while(true) {
			System.out.print("");
			while(! AR_msg_queue.isEmpty()) {
				String msg = AR_msg_queue.removeFirst();
				String msg_type = msg.substring(0,1);
				if(msg_type.equals("A")) {
					System.out.println("add node is initiated");
					count_transfer = 0;
					String Add_file_name = msg.substring(2, msg.length());
					Node old_map = map;
					Node add_map = null;
					try {
						BufferedReader br = new BufferedReader(new FileReader(pathname + Add_file_name));
						add_map = gson.fromJson(br, Node.class);
						System.out.println("new map received is " + gson.toJson(add_map));
					} catch (IOException e) {
						e.printStackTrace();
					}
					Node new_clustermap = add_map;
					new_clustermap.right = old_map;
					System.out.println("new map formed is " + gson.toJson(new_clustermap));
					try {
						BufferedReader ReadFile = new BufferedReader(new FileReader(pathname + "file_log.txt"));
						String filename;
						System.out.println("File transfer started ");
						while ((filename = ReadFile.readLine()) != null) {
							try {
								Thread.sleep(30);
							}
							catch (Exception e) {
								System.out.println(e);
							}
							System.out.println("checking for the filename " + filename);
							System.out.println("old positions ");
							String result1[] = selectInMap(map, filename);
							for (String x : result1) {
								System.out.println(x);
							}	
							System.out.println("new positions ");
							String result2[] = selectInMap(new_clustermap, filename);
							for (String x : result2) {
								System.out.println(x);
							}	
							List<String> copy_to = new ArrayList<String>();
							List<String> copy_from = new ArrayList<String>();
							for (String check_this : result2) {
								int got = 0;
								for (String check_with : result1) {
									if(check_this.equals(check_with)) {
										got = 1;
									}
								}
								if(got == 0) {
									copy_to.add(check_this);
								}
							}
							copy_from.add(result1[0]);
							if(! copy_to.isEmpty()) { 
								count_transfer = count_transfer + copy_to.size();
								my_download(filename, copy_from);
								my_write(filename, copy_to);
								num_msg_back = 1;
								String result2_str = "";
								for(String res : copy_to) {
									result2_str = result2_str + res + ",";
								}
								result2_str = result2_str.substring(0, result2_str.length() -1);
								String send = "SAVE_" + filename + "_" + result2_str;
								String sendto = result1[0].split(":")[0] + ":" + (Integer.parseInt(result1[0].split(":")[1]) + 2);
								send_To(send, sendto);
								System.out.println("wrote to look up server as " + result2_str);
								while(true) {
									System.out.print("");
									if(num_msg_back == 0) {
										break;
									}
								}
								if (! Osds_looked.contains(copy_from.get(0))) {
									Osds_looked.add(copy_from.get(0));
								}
								File F = new File(pathname + "Tempfolder/");
								deleteFolderContent(F);
								System.out.println("done with file name " + filename);
							}
						}
						ReadFile.close();
						System.out.println(count_transfer + " number of files transfered");
					}
					catch(Exception e) {
						System.out.println(e);
					}
					//wipe must be sent to all osds and not to clients
					String send = "WIPE";
					for (String c : Osds_looked) {
						num_msg_back = 1;
						String sendto = c.split(":")[0] + ":" + (Integer.parseInt(c.split(":")[1]) + 2);
						send_To(send, sendto);
						while(true) {
							System.out.print("");
							if(num_msg_back == 0) {
								break;
							}
						}
						
					}
					Osds_looked.clear();
					num_msg_back = Clients.size();
					for(String ipconfig : Clients) {
						send_To("MA_" + gson.toJson(new_clustermap), ipconfig);
					}
					while(true) {
						System.out.print("");
						if(num_msg_back == 0) {
							break;
						}
					}
					
					map = null;
					map = new_clustermap;
					new_clustermap = null;					
					System.out.println("File transfer done ");;
				}
				else if(msg_type.equals("F")) {
					System.out.println("Fail a node is initiated");
					count_transfer = 0;
					int F_osd_id = Integer.parseInt(msg.substring(2, msg.length()));
					set_osdid_as_failed(map, F_osd_id);
					System.out.println("new map formed is " + gson.toJson(map));
					num_msg_back = Clients.size();
					for(String ipconfig : Clients) {
						send_To("MA_" + gson.toJson(map), ipconfig);
					}
					while(true) {
						System.out.print("");
						if(num_msg_back == 0) {
							break;
						}
					}
					try {
						BufferedReader ReadFile = new BufferedReader(new FileReader(pathname + "file_log.txt"));
						String filename;
						while ((filename = ReadFile.readLine()) != null) {
							try {
								Thread.sleep(30);
							}
							catch (Exception e) {
								System.out.println(e);
							}
							System.out.println("checking for the filename " + filename);
							System.out.println("old positions ");
							String result1[] = selectInMap_oldloc_failed(map, filename, F_osd_id);
							for (String x : result1) {
								System.out.println(x);
							}
							System.out.println("new positions ");
							String result2[] = selectInMap(map, filename);
							for (String x : result2) {
								System.out.println(x);
							}
							List<String> copy_to = new ArrayList<String>();
							List<String> copy_from = new ArrayList<String>();
							for (String check_this : result2) {
								int got = 0;
								for (String check_with : result1) {
									if(check_this.equals(check_with)) {
										got = 1;
									}
								}
								if(got == 0) {
									copy_to.add(check_this);
								}
							}
							if(F_osd_ipconfig.equals(result1[0])) {
								copy_from.add(result1[1]);
							}
							else {
								copy_from.add(result1[0]);
							}
							if(! copy_to.isEmpty()) {
								count_transfer = count_transfer + copy_to.size();
								my_download(filename, copy_from);
								my_write(filename, copy_to);
							}
							File F = new File(pathname + "Tempfolder/");
							deleteFolderContent(F);
							System.out.println("done with file name " + filename);
						}
						ReadFile.close();
						System.out.println(count_transfer + " number of files transfered");
					}
					catch(Exception e) {
						e.printStackTrace();
						System.out.println(e);
					}
				}
				else if(msg_type.equals("O")) {
					System.out.println("Overload a node is initiated");
					count_transfer = 0;
					int O_osd_id = Integer.parseInt(msg.substring(2, msg.length()));
					set_osdid_as_overloaded(map, O_osd_id);
					System.out.println("new map formed is " + gson.toJson(map));
					num_msg_back = Clients.size();
					for(String ipconfig : Clients) {
						send_To("MA_" + gson.toJson(map), ipconfig);
					}
					while(true) {
						System.out.print("");
						if(num_msg_back == 0) {
							break;
						}
					}
					try {
						BufferedReader ReadFile = new BufferedReader(new FileReader(pathname + "file_log.txt"));
						String filename;
						while ((filename = ReadFile.readLine()) != null) {
							try {
								Thread.sleep(30);
							}
							catch (Exception e) {
								System.out.println(e);
							}
							System.out.println("checking for the filename " + filename);
							System.out.println("old positions ");
							String result1[] = selectInMap_oldloc_overloaded(map, filename,O_osd_id);
							for (String x : result1) {
								System.out.println(x);
							}
							System.out.println("new positions ");
							String result2[] = selectInMap(map, filename);
							for (String x : result2) {
								System.out.println(x);
							}
							List<String> copy_to = new ArrayList<String>();
							List<String> copy_from = new ArrayList<String>();
							for (String check_this : result2) {
								int got = 0;
								for (String check_with : result1) {
									if(check_this.equals(check_with)) {
										got = 1;
									}
								}
								if(got == 0) {
									copy_to.add(check_this);
								}
							}
							if(O_osd_ipconfig.equals(result1[0])) {
								copy_from.add(result1[1]);
							}
							else {
								copy_from.add(result1[0]);
							}
							if(! copy_to.isEmpty()) {
								count_transfer = count_transfer + copy_to.size();
								my_download(filename, copy_from);
								my_write(filename, copy_to);
							}
							File F = new File(pathname + "Tempfolder/");
							deleteFolderContent(F);
							System.out.println("done with file name " + filename);
						}
						ReadFile.close();
						System.out.println(count_transfer + " number of files transfered");
					}
					catch(Exception e) {
						System.out.println(e);
					}
				}
			}
			while(! MR_msg_queue.isEmpty()) {
				String ipconfig = MR_msg_queue.removeFirst();
				int got = 0;
				for (String check_this : Clients) {
					if(check_this.equals(ipconfig)) {
						got = 1;
					}
				}
				if(got == 0) {
					Clients.add(ipconfig);
				}
				String str_map = gson.toJson(map);
				num_msg_back = 1;
				send_To("MA_" + str_map, ipconfig);
				System.out.println("map is requested by " + ipconfig + " and sent to it");
				while(true) {
					System.out.print("");
					if(num_msg_back == 0) {
						break;
					}
				}
			}
			while(! RC_msg_queue.isEmpty()) {
				String ipconfig = RC_msg_queue.removeFirst();
				int got = 0;
				for (String check_this : Clients) {
					if(check_this.equals(ipconfig)) {
						got = 1;
					}
				}
				if(got == 1) {
					Clients.remove(ipconfig);
				}
			}
		}
	}
	
	public static void receive(String msg) {
		
		String msg_type = msg.substring(0, 2);
		if (msg_type.equals("MR")) {
			String ipconfig = msg.substring(3, msg.length());
			MR_msg_queue.addLast(ipconfig);
		}
		else if(msg_type.equals("RC")) {
			String ipconfig = msg.substring(3, msg.length());
			RC_msg_queue.addLast(ipconfig);
		}
		else if(msg_type.equals("AR")) {
			String admin_req = msg.substring(3, msg.length());
			AR_msg_queue.addLast(admin_req);
		}
		else if(msg_type.equals("MA")) {
			num_msg_back--;
		}
		else if(msg_type.equals("WR")) {
			String filename = msg.substring(3, msg.length());
			try {
				String filepath =  pathname + "file_log.txt";
				BufferedWriter WriteFile = new BufferedWriter(new FileWriter(filepath, true));
				WriteFile.write(filename);
				WriteFile.newLine();
				WriteFile.close();
			}
			catch (Exception e) {
				System.out.println(e + "in WriteFile");
			}
		}
	}	
	
	public static void Call_Listner() {
		
		(new Thread() {
			public void run() {
				new Monitor_Listen_to(M_port).run();
			}
		}).start();
	}
	
	public static void deleteFolderContent(File folder) {

	    File[] files = folder.listFiles();
	    if(files!=null) { 
	        for(File f: files) {
	            if(f.isDirectory()) {
	                deleteFolderContent(f);
	            } else {
	                f.delete();
	            }
	        }
	    }
	}

	
	static void my_write(String request_on, List<String> res1) {
		
		try{
			String res[] = new String[res1.size()];
			for(int i = 0; i < res1.size(); i++) {
				res[i] = res1.get(i);
			}
			System.out.println("uploading file from " +  pathname + "Tempfolder/ to ");
			for (int i = 0; i < res.length; i++) {
				res[i] = res[i].split(":")[0] + ":" +(Integer.parseInt(res[i].split(":")[1]) + 1);
				System.out.println(res[i]);
			}
			ArrayList<Address> addresses = splitAddress(res,0);
			if(upload(control, pathname + "Tempfolder/" + request_on, addresses)) {
				log.i("File upload success.");
				System.out.println("File upload success.");
			}
			else {
				log.i("File upload fails.");
				System.out.println("File upload fails.");
			}
		}catch(Exception e){
			log.w(e);
		}
	}
	
	static void my_download(String Filename, List<String> copy_from) {
		
		File F1 = new File(pathname + "Tempfolder/");
		Path tempDir = F1.toPath();
		String path_osd = "/home/groupe/E2_Box/OSD/OSD";
		String serverIP = copy_from.get(0).split(":")[0];
		int serverPort = (Integer.parseInt(copy_from.get(0).split(":")[1]));
		path_osd = path_osd + serverIP.substring(serverIP.length() - 1, serverIP.length()) + "/Files/";
		System.out.println("downloading file from " + copy_from.get(0) + " to " +  pathname + "Tempfolder/ from " + path_osd);
		String temp = downloadToTemp(tempDir, control, serverIP, serverPort, path_osd + Filename);
		log.i("Down to: " + temp);
		System.out.println(temp);
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
					//buffer.mark();
				}
				if(read_once<0) break;
				if(read<size)
					buffer.clear();
					//buffer.reset();
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
			System.out.println(e);
		}
	}
	
	public static void set_osdid_as_failed(Node root, int F_osd_id) {
		
		if(root.function == 3) {
			if (root.name.id == F_osd_id) {
				F_osd_ipconfig = root.name.ip + ":" + root.name.port;
				root.name.status = "F";
			}
			if(root.val != 1) {
				set_osdid_as_failed(root.right, F_osd_id);
			}
		}
		else {
			set_osdid_as_failed(root.left, F_osd_id);
			if(root.val != 1.0) {
				set_osdid_as_failed(root.right, F_osd_id);
			}
		}
	}
	
	public static void set_osdid_as_overloaded(Node root, int O_osd_id) {
		
		if(root.function == 3) {
			if (root.name.id == O_osd_id) {
				O_osd_ipconfig = root.name.ip + ":" + root.name.port;
				root.name.status = "O";
			}
			if(root.val != 1) {
				set_osdid_as_overloaded(root.right, O_osd_id);
			}
		}
		else {
			set_osdid_as_overloaded(root.left, O_osd_id);
			if(root.val != 1.0) {
				set_osdid_as_overloaded(root.right, O_osd_id);
			}
		}
	}

	public static String[] selectInMap_oldloc_overloaded(Node clustermap, String fileName, int O_osd_id) {
		
		String FileName = "PlacementRules.txt";
		String str = "";
		int val = 0;
		int rin_row = 0;
		int rin_cabinet = 0;
		int rin_osd = 0;
		try {
			BufferedReader ReadFile = new BufferedReader(new FileReader(pathname + FileName));
			while ((str = ReadFile.readLine()) != null) {
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
					if (status.equals("O") && res != O_osd_id) {
						til_osd++;
					}
					else if (status.equals("F")) {
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
	
	public static String[] selectInMap_oldloc_failed(Node clustermap, String fileName, int F_osd_id) {
		
		String FileName = "PlacementRules.txt";
		String str = "";
		int val = 0;
		int rin_row = 0;
		int rin_cabinet = 0;
		int rin_osd = 0;
		try {
			BufferedReader ReadFile = new BufferedReader(new FileReader(pathname + FileName));
			while ((str = ReadFile.readLine()) != null) {
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
		/*System.out.println("rows selected");
		for (int a = 0; a < rin_row; a++) {
			System.out.print("  " + sel_row[a]);
		}
		System.out.println(" ");*/
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
		/*System.out.println("cabinet selected");
		for (int a = 0; a < rin_cabinet * rin_row; a++) {
			System.out.print("  " + sel_cabinet[a]);
		}
		System.out.println("  ");*/
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
					if (status.equals("F") && res != F_osd_id) {
						til_osd++;
					}
					else if (status.equals("O")) {
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
				if (val == 0) {
					rin_row = Character.getNumericValue(str.charAt(0));
					val++;
				} else if (val == 1) {
					rin_cabinet = Character.getNumericValue(str.charAt(0));
					val++;
				} else {
					rin_osd = Character.getNumericValue(str.charAt(0));
				}
			}
			ReadFile.close();
		} catch (Exception ex) {
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
