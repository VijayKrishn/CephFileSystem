import req.Rand.ExpGenerator;
import req.Rand.RandomGenerator;
import req.Rand.UniformGenerator;
import req.Rand.ZipfGenerator;

import java.util.*;

public class RandomTest{
	public static class Frequency<T>{
		Map<T, Integer> unsorted=new HashMap<>();
		List<Map.Entry<T, Integer>> sorted=new ArrayList<>();

		class ValueComparator implements Comparator<Map.Entry<T, Integer>>{
			@Override
			public int compare(Map.Entry<T, Integer> o1,Map.Entry<T, Integer> o2){
				int diff=o1.getValue()-o2.getValue();
				if(o1.getKey() instanceof Comparable && o2.getKey() instanceof Comparable)
					return diff==0 ? ((Comparable)(o1.getKey())).compareTo((Comparable)(o2.getKey())) : diff;
				else return diff;
			}
		}

		void add(T t){
			Integer pre=unsorted.get(t);
			unsorted.put(t,pre==null ? 1 : (pre+1));
		}

		void print(){
			sorted.addAll(unsorted.entrySet());
			Collections.sort(sorted,new ValueComparator());
			System.out.println("---------sorted----------");
			for(Map.Entry<T, Integer> entry : sorted){
				System.out.println(entry.getKey().toString()+":"+entry.getValue());
			}
		}
	}

	public static void main(String args[]){
		Map<String, RandomGenerator> gens=new HashMap<>();
		int upper=2048;
		UniformGenerator uniform=new UniformGenerator(upper);
		gens.put("Uniform",uniform);
		gens.put("Exp",new ExpGenerator(100.0,upper,uniform));
		gens.put("Zipf",new ZipfGenerator(0.9,upper,uniform));
		for(Map.Entry<String, RandomGenerator> entry : gens.entrySet()){
			System.out.println("\n----------"+entry.getKey()+"----------");
			RandomGenerator gen=entry.getValue();
			Frequency<Integer> f=new Frequency<>();
			for(int i=1;i<=upper;++i){
				int rand=gen.nextInt();
				f.add(rand);
			}
			f.print();
		}
	}
}
