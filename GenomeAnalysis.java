import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import java.util.List;
import java.util.regex.Pattern;
import java.util.ArrayList;
import org.apache.spark.broadcast.Broadcast;

public final class GenomeAnalysis {
  private static final Pattern TAB = Pattern.compile("\t");
  
  // the implementation of mapping fragments to genes
  static class Mapper implements PairFlatMapFunction<String, String, Integer> {
    private final Broadcast<int[]> broadcastGeneIDs;
    private final Broadcast<int[]> broadcastChromosomes;
    private final Broadcast<String[]> broadcastStrands;
    private final Broadcast<int[]> broadcastGeneStarts;
    private final Broadcast<int[]> broadcastGeneEnds;

    public Mapper(Broadcast<int[]> ids, Broadcast<int[]> chrs, Broadcast<String[]> strands, Broadcast<int[]> starts, Broadcast<int[]> ends) {
      this.broadcastGeneIDs = ids;
      this.broadcastChromosomes = chrs;
      this.broadcastStrands = strands;
      this.broadcastGeneStarts = starts;
      this.broadcastGeneEnds = ends;    
    }

    private int map(String chr, String start, String end, int[] gene_ids, int[] chromosomes, String[] strands, int[] gene_starts, int[] gene_ends) {
      int gene_id = -1;
      int distance = Integer.MAX_VALUE;
      int frag_chr = Integer.parseInt(chr);
      int frag_start = Integer.parseInt(start);
      int frag_end = Integer.parseInt(end);
      int frag_mid = (frag_start + frag_end) / 2;

      int gene_num = gene_ids.length;
      for (int i = 0; i < gene_num; i++) {
        if (frag_chr == chromosomes[i] && !(frag_end <= gene_starts[i] || frag_start >= gene_ends[i])) {
          int tss;
          if (strands[i].equals("+")) {
            tss = gene_starts[i];
          } else {
            tss = gene_ends[i];
          }
          int tmp = Math.abs(tss - frag_mid);
          if (tmp < distance) {
            distance = tmp;
            gene_id = gene_ids[i];
          }
        }
        
        if(frag_chr < chromosomes[i]) {
          break;
        }
      }
      return gene_id;
    }

    @Override
    public Iterable<Tuple2<String, Integer>> call(String s) {
      List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
      s = s.trim();
      String[] parts = TAB.split(s);

      int[] ids = broadcastGeneIDs.value();
      int[] chromosomes = broadcastChromosomes.value();
      String[] strands = broadcastStrands.value();
      int[] starts = broadcastGeneStarts.value();
      int[] ends = broadcastGeneEnds.value(); 
      int gene1 = map(parts[0], parts[1], parts[2], ids, chromosomes, strands, starts, ends);
      int gene2 = map(parts[3], parts[4], parts[5], ids, chromosomes, strands, starts, ends);
      
      if (-1 != gene1 && -1 != gene2) {
        String key = gene1 + ":" + gene2;
        list.add(new Tuple2<String, Integer>(key, Integer.parseInt(parts[6])));
      }
      return list;
    }

  } 

  public static void main(String[] args) throws Exception {

    if (args.length != 3) {
      System.err.println("Usage: GenomeAnalysis <input file> <gene file> <output file> <spark task number>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("GenomeAnalysis");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    int task_num = Integer.parseInt(args[3]);
    JavaRDD<String> input_lines = ctx.textFile(args[0], task_num);
    JavaRDD<String> gene_lines = ctx.textFile(args[1], task_num);
    List<String> genes = gene_lines.collect();
    
    // extract gene fields
    int gene_num = genes.size();
    int[] gene_ids = new int[gene_num];
    int[] chromosomes = new int[gene_num];
    String[] strands = new String[gene_num];
    int[] gene_starts = new int[gene_num];
    int[] gene_ends = new int[gene_num];
    int index = 0;
    for (String gene : genes) {
      gene = gene.trim();
      String[] parts = TAB.split(gene);
      gene_ids[index] = Integer.parseInt(parts[0]);
      chromosomes[index] = Integer.parseInt(parts[1]);
      strands[index] = parts[2];
      gene_starts[index] = Integer.parseInt(parts[3]);
      gene_ends[index] = Integer.parseInt(parts[4]);
      index = index + 1;
    }

    // broadcast gene fields because they are shared by all tasks and read only
    Broadcast<int[]> broadcastGeneIDs = ctx.broadcast(gene_ids);
    Broadcast<int[]> broadcastChromosomes = ctx.broadcast(chromosomes);
    Broadcast<String[]> broadcastStrands = ctx.broadcast(strands);
    Broadcast<int[]> broadcastGeneStarts = ctx.broadcast(gene_starts);
    Broadcast<int[]> broadcastGeneEnds = ctx.broadcast(gene_ends);    

    // map fragments to genes, generate (gene_id:gene_id, count) pair
    JavaPairRDD<String, Integer> ones = input_lines.flatMapToPair(new Mapper(broadcastGeneIDs, broadcastChromosomes, broadcastStrands, broadcastGeneStarts, broadcastGeneEnds));
    // sum counts according to keys
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

    counts.saveAsTextFile(args[2]);
    ctx.stop();
  }
}
