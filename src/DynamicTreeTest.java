import req.DynamicTree;
import req.Rand.UniformGenerator;
import req.Request;
import req.StaticTree;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DynamicTreeTest{
	public static void dump(StaticTree tree,boolean isFile){
		if(isFile)
			for(int i=0;i<tree.getFileSize();++i) System.out.println(tree.fileInfo(i));
		else
			for(int i=0;i<tree.getNonEmptyDirSize();++i) System.out.println(tree.ls(i));
	}

	static int roll(UniformGenerator uniform,List<Integer> ratio,int sum){
		int dice=uniform.nextInt(sum)+1;
		for(int i=0;i<ratio.size();++i){
			if(dice<=ratio.get(i)) return i;
			dice-=ratio.get(i);
		}
		return -1;
	}

	public static void main(String args[]) throws IOException{
		UniformGenerator uniform=new UniformGenerator();
		StaticTree sTree=StaticTree.getStaticTree("files/test.txt",uniform);
		DynamicTree dTree=DynamicTree.getDynamicTree("files/test2.txt",uniform);
		List<Integer> ratio=Arrays.asList(4,3,2,1);
		List<Request.ReqType> ops=Arrays.asList(Request.ReqType.CREATE_FILE,Request.ReqType.DELETE,Request.ReqType.CREATE_DIR,Request.ReqType.RMDIR);
		int sum=0;
		for(int i=0;i<ratio.size();++i) sum+=ratio.get(i);
		System.out.println("-------original--------");
		dump(dTree,true);
		dump(dTree,false);
		for(int i=0;i<10;++i){
			Request.ReqType type=ops.get(roll(uniform,ratio,sum));
			System.out.println("Run "+i+":"+type);
			boolean isFile=(type==Request.ReqType.CREATE_FILE || type==Request.ReqType.DELETE);
			Request req=null;
			if(type==Request.ReqType.CREATE_FILE)
				req=dTree.createFile(uniform.nextInt(dTree.getEmptyDirSize()+dTree.getNonEmptyDirSize()));
			else if(type==Request.ReqType.DELETE)
				req=dTree.delete(uniform.nextInt(dTree.getFileSize()));
			else if(type==Request.ReqType.CREATE_DIR)
				req=dTree.createDir(uniform.nextInt(dTree.getEmptyDirSize()+dTree.getNonEmptyDirSize()));
			else req=dTree.rmdir(uniform.nextInt(dTree.getEmptyDirSize()));
			req.type=type;
			System.out.println(">"+req);
			dump(dTree,isFile);
		}
	}
}
