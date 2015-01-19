# Spark_Genome_Analysis
This is a Spark version genome analysis tool on HiC data with the purpose of mapping HiC intra interactions to genes, then obtaining a gene-gene interaction network and enabling further study. 

0. How to compile and run at the directory containing all Java source code
Spark is required to run the tool. In the following example, I have uploaded the two input files (gene and input) to a hdfs, which is welcome to change to meet your own environment. 

  mkdir GenomeAnalysis

  javac -classpath /home/group5/spark-1.1.1-bin-hadoop1/lib/spark-assembly-1.1.1-hadoop1.0.4.jar:/home/group5/scala-2.11.4/lib/scala-library.jar -d ./GenomeAnalysis/ *.java

  jar -cvf GenomeAnalysis.jar -C ./GenomeAnalysis/ .

  /home/group5/spark-1.1.1-bin-hadoop1/bin/spark-submit --class "GenomeAnalysis" --master spark://group5node29:7077 ./GenomeAnalysis.jar hdfs://group5node29:54310/user/group5/project/input hdfs://group5node29:54310/user/group5/project/gene hdfs://group5node29:54310/user/group5/spark/output4

1. Introduction
As the rapid development of sequencing technology in genome research, scientists are generating a large amount of data at an unprecedented rate through high throughput experiments. One kind of these experiments is HiC experiment, which is a derivative of Chromosome Conformation Capture [1]. The experiment captures one type of mysterious phenomena in cell, chromosomes binding to themselves or each other through proteins and forming functional interactions, which has a tremendous impact on long range gene expression regulation. Therefore an analysis tool used to understanding the data at gene level is required and important to the genome research.
Meanwhile, one sample of the HiC experiment generates several gigabytes of data, while a study generally has multiple samples, so the tool should be able to meet a large memory demand and execute in parallel. Thereby we propose a cloud based HiC chromosome interaction genome analysis tool because cloud computing inherently has an advantage to process big data economical and conveniently. 

2. The Data
There are two types of data used in this study. One is HiC chromosome interaction data, which describes two locations of chromosomes binding together. Its format defines like
location1_chromosome 	location2_chromosome 	location1_start 		location1_end	location2_start		location2_end		count
An example is given as bellow
  2       3      181910803       181910852       181404359       181404408       1
It means the fragment at chromosome 2 starting from 181910803 and ending at 181910852 binds with the fragment at chromosome 3 starting from 18144359 and ending at 181404408. There is only 1 interaction. 
The other data are whole human genes, whose format define like
gene_id		chromosome		strand		end1		end2	
Examples are given as bellow
  1		1			-		14362   		29370
  2		1			+ 		69090		70008
There are two genes. The first one is at the reverse strand (DNA has double strands) so the gene start is 29370 and the end is 14362. While the other is at the forward strand, therefore the gene start is 69090 and the end is 70008. The start of a gene is important to its expression. 

3. The Goals
The purpose of this tool is to map HiC interactions’ locations to genes and calculate the number of interactions between genes according to the following criteria.
  (1).	mapping a location to a gene if they are on the same chromosome and overlap with each other
  (2).	if a location is mapped to multiple genes, pick the one whose start is closest to the middle point of the location
  (3).	Only interactions whose locations are mapped to genes are counted, otherwise it is ignored
The result of tool can be visualized by Cytoscape to show us the interactions between genes, which can be used to continue further analysis by scientists. 


[1] http://en.wikipedia.org/wiki/Chromosome_conformation_capture
