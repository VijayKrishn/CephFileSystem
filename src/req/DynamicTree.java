package req;

import req.Rand.RandomGenerator;
import req.Rand.UniformGenerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//  I decide not to make this class thread safe so you need to use external.
public class DynamicTree extends StaticTree{
	protected DynamicTree(){
		super();
	}

	protected DynamicTree(RandomGenerator uniform){
		super(uniform);
	}

	public DynamicTree(RandomGenerator uniform,String sep){
		super(uniform,sep);
	}

	public static DynamicTree getDynamicTree(String filename,UniformGenerator uniform) throws IOException{
		DynamicTree tree=new DynamicTree(uniform);
		new TreeParser<DynamicTree,DynamicRandTreeNode>().parse(tree,filename,true);
		return tree;
	}

	@Override
	protected RandTreeNode emptyNode(){
		return new DynamicRandTreeNode();
	}

	protected class DynamicRandTreeNode extends RandTreeNode{
		Map<String,DynamicRandTreeNode> children=null;

		DynamicRandTreeNode removeUp(){
			if(parent!=null){
				DynamicRandTreeNode p=(DynamicRandTreeNode)parent;
				p.children.remove(this.name);
				if(p.children.size()==0){
					nonEmptyDirs.remove(parent);
					emptyDirs.add(parent);
				}
				return p;
			}
			return null;
		}

		@Override
		protected void setParent(RandTreeNode p){
			super.setParent(p);
			DynamicRandTreeNode parent=(DynamicRandTreeNode)p;
			if(parent.children==null) parent.children=new HashMap<>();
			parent.children.put(name,this);
		}

		DynamicRandTreeNode createFile(int index){
			if(children==null){
				children=new HashMap<>();
			}
			String newName=randName();
			while(children.containsKey(newName)) newName=randName();
			DynamicRandTreeNode child=new DynamicRandTreeNode();
			child.name=newName;
			child.parent=this;
			children.put(newName,child);
			files.add(0,child);
			if(children.size()==1){
				emptyDirs.remove(index);
				nonEmptyDirs.add(this);
			}
			return child;
		}

		DynamicRandTreeNode createDir(int index){
			if(children==null){
				children=new HashMap<>();
			}
			DynamicRandTreeNode child=new DynamicRandTreeNode();
			String newName=randName()+sep;
			while(children.containsKey(newName)) newName=randName()+sep;
			child.name=newName;
			child.parent=this;
			children.put(newName,child);
			emptyDirs.add(0,child);
			if(children.size()==1){
				emptyDirs.remove(index);
				nonEmptyDirs.add(this);
			}
			return child;
		}
	}

	public int getEmptyDirSize(){
		return emptyDirs.size();
	}


	public int getAllDirSize(){
		return emptyDirs.size()+nonEmptyDirs.size();
	}

	public Request rmdir(int index){    //  index in emptyDirs
		if(index<emptyDirs.size()){
			DynamicRandTreeNode result=(DynamicRandTreeNode)emptyDirs.get(index);
			result.removeUp();
			emptyDirs.remove(index);
			Request r=new Request(Request.ReqType.RMDIR,result.toString());
			if(result.parent!=null) r.next=new Request(Request.ReqType.LS,result.parent.toString());
			return r;
		}else return null;
	}

	public Request delete(int index){   //  index in files
		if(index<files.size()){
			DynamicRandTreeNode result=(DynamicRandTreeNode)files.get(index);
			result.removeUp();
			files.remove(index);
			Request r=new Request(Request.ReqType.DELETE,result.toString(),result.size);
			if(result.parent!=null) r.next=new Request(Request.ReqType.LS,result.parent.toString());
			return r;
		}else return null;
	}

	private Request create(int index,boolean isDir){
		DynamicRandTreeNode parent;
		if(index>=emptyDirs.size()){
			index-=emptyDirs.size();
			if(index>=nonEmptyDirs.size()) return null;
			else parent=(DynamicRandTreeNode)nonEmptyDirs.get(index);
		}else parent=(DynamicRandTreeNode)emptyDirs.get(index);
		DynamicRandTreeNode child=isDir ? parent.createDir(index) : parent.createFile(index);
		Request r=new Request(isDir ? Request.ReqType.CREATE_DIR : Request.ReqType.CREATE_FILE,child.toString());
		r.next=new Request(Request.ReqType.LS,parent.toString());
		if(!isDir) r.end=0;
		return r;
	}

	public Request createFile(int index){   //  index in nonEmptyDirs, then emptyDirs
		return create(index,false);
	}

	public Request createDir(int index){
		return create(index,true);
	}
}
