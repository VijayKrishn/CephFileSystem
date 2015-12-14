import req.Rand.UniformGenerator;
import req.StaticTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StaticTreeTest{
	private static final long mb=1024*1024;

	public static void main(String args[]) throws IOException{
		Runtime runtime=Runtime.getRuntime();
		long pre=runtime.freeMemory()/mb;
		StaticTree tree=StaticTree.getStaticTree("files/test.txt",new UniformGenerator());
		System.out.println("Memory: "+(pre-runtime.freeMemory()/mb)+" mb");
		System.out.println("-----------dirs------------");
		System.out.println("NonEmpty dir size: "+tree.getNonEmptyDirSize());
		for(int i=0;i<tree.getNonEmptyDirSize();++i){
			System.out.println(tree.ls(i));
		}
		System.out.println("-----------files------------");
		System.out.println("File size:"+tree.getFileSize());
		for(int i=0;i<tree.getFileSize();++i){
			System.out.println(tree.fileInfo(i));
		}
		tree.shuffleFiles();
		System.out.println("-----------shuffled------------");
		System.out.println("File size:"+tree.getFileSize());
		for(int i=0;i<tree.getFileSize();++i){
			System.out.println(tree.fileInfo(i));
		}
		int t=10;
		List<Integer> ls=new ArrayList<>(t);
		for(int i=1;i<=t;++i) ls.add(i);
//		StaticTree.plainShuffle(ls,new UniformGenerator());
//		System.out.println("-----------shuffled2------------");
//		for(int i=0;i<t;++i){
//			System.out.println(ls.get(i));
//		}
		List<List<Integer>> ls2=new ArrayList<>();
		ls2.add(Arrays.asList(1,2,3));
		ls2.add(Arrays.asList(1));
		ls2.add(Arrays.asList(1));
		ls2.add(Arrays.asList(1));
		ls2.add(Arrays.asList(2));
		ls2.add(Arrays.asList(2));
		ls2.add(Arrays.asList(2));
		ls2.add(Arrays.asList(3));
		ls2.add(Arrays.asList(3));
		ls2.add(Arrays.asList(3));
		StaticTree.unevenShuffle(ls,new UniformGenerator(),ls2);
		System.out.println("-----------shuffled2------------");
		for(int i=0;i<t;++i){
			System.out.println(ls.get(i));
		}
	}
}
