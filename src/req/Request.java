package req;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Request implements Serializable{


	static public final double[] ratio=new double[]{0.7,0.2,0.03,0.04,0.03};

	public enum ReqType{
		RANDOM_READ,RANDOM_WRITE,SEQ_READ,SEQ_WRITE,DELETE,RMDIR,APPEND,CREATE_FILE,CREATE_DIR,LS
	}

	public static Set<ReqType> immutable=new HashSet<>(Arrays.asList(ReqType.RANDOM_READ,ReqType.RANDOM_WRITE,
			ReqType.SEQ_READ,ReqType.SEQ_WRITE,ReqType.APPEND,ReqType.LS));
	public static Set<ReqType> mutable=new HashSet<>(Arrays.asList(ReqType.DELETE,ReqType.RMDIR,
			ReqType.CREATE_FILE,ReqType.CREATE_DIR));

	public Request(){
	}

	public Request(ReqType type){
		this.type=type;
	}

	public Request(ReqType type,String path){
		this.type=type;
		this.path=path;
	}

	public Request(ReqType type,String path,long size){
		this.type=type;
		this.path=path;
		this.end=size;
	}

	public Request(String path){
		this.path=path;
	}

	public Request(String path,long size){
		this.path=path;
		this.end=size;
	}

	public ReqType type=null;
	public long start=0;    //start position for read, write
	public long end=-1;     //end position for read, write; append size for append
	public String path=null;
	public Request next=null;

	@Override
	public String toString(){
		return String.format("%s : %s,%d:%d",type,path,start,end);
	}
}
