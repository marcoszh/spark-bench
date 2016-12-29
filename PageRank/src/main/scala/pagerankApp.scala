
/*
 * (C) Copyright IBM Corp. 2015 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 *  http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package src.main.scala
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}

import scala.collection.parallel._

object pagerankApp extends Logging {
  var masterurl = "ec2-52-207-241-50.compute-1.amazonaws.com"
  var hdfs = "hdfs://" + masterurl + ":9000/SparkBench/"

  def main(args: Array[String]) {
    if (args.length < 0) {
      println("usage: <input> <output> <minEdge> <maxIterations> <tolerance> <resetProb> <StorageLevel>")
      System.exit(0)
    }
	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
	
    val conf = new SparkConf
    conf.setAppName("Spark PageRank Application").set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://" + masterurl + ":9000/logs").set("spark.scheduler.mode","Fair")
      .set("spark.memory.useLegacyMode", "true").set("spark.storage.memoryFraction", "0.01")
    val sc = new SparkContext(conf)
	//conf.registerKryoClasses(Array(classOf[pagerankApp] ))
    val start = System.currentTimeMillis

    //var pc = mutable.ParArray(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
    var pc = mutable.ParArray(0, 1, 2, 3, 4, 5, 6, 7)
    pc.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(8))
    pc map {i => func(sc, i, start)}

    sc.stop();


  }

  def func(sc: SparkContext, i: Int, start0:Long): Unit = {
    sc.setLocalProperty("job.threadId", i.toString)
    sc.setLocalProperty("job.priority","1")

    while (System.currentTimeMillis < start0 + 500 * i){}
    println("######### Start execution - ForegroundApp")

    if (i % 2 == 0) {
      println("######### Start execution - PregelOperation")
      pregelOperation(sc)
    }
    if (i % 2 == 1) {
      println("######### Start execution - ConnectedComponent")
      connectedComponentApp(sc)
    }

  }


  def pagerank_usingGenedData(sc: SparkContext): Unit = {
    val input = hdfs + "PageRank/Input"
    val output = hdfs + "PageRank/Output"
    val minEdge = 20
    val maxIterations = 24
    val tolerance = 0.001
    val resetProb = 0.15
    val storageLevel=StorageLevel.MEMORY_AND_DISK

    var sl:StorageLevel=StorageLevel.MEMORY_ONLY;
    if(storageLevel=="MEMORY_AND_DISK_SER")
      sl=StorageLevel.MEMORY_AND_DISK_SER
    else if(storageLevel=="MEMORY_AND_DISK")
      sl=StorageLevel.MEMORY_AND_DISK

    val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, sl, sl)

    val staticRanks = graph.staticPageRank(maxIterations, resetProb).vertices
    //staticRanks.saveAsTextFile(output);
  }

  def connectedComponentApp(sc: SparkContext): Unit = {
    val input = hdfs + "ConnectedComponent/Input"
    val output = hdfs + "ConnectedComponent/Output"
    val minEdge= 20
    val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
    val res = graph.connectedComponents().vertices
    //res.saveAsTextFile(output);
  }

  def pregelOperation(sc: SparkContext): Unit = {
    val input = hdfs + "PregelOperation/Input"
    val output = hdfs + "PregelOperation/Output"
    val minEdge = 20
    val loadedgraph = GraphLoader.edgeListFile(sc, input, true, minEdge, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)
    val graph: Graph[Int, Double] =loadedgraph.mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b) // Merge Message
    )
    //println(sssp.vertices.collect.mkString("\n"))
    println("vertices count: "+sssp.vertices.count())
    //res.saveAsTextFile(output);
  }

  def svdPlusPlus(sc: SparkContext): Unit = {
    val input = hdfs + "SVDPlusPlus/Input"
    val output = hdfs + "SVDPlusPlus/Output"
    val minEdge= 20
    val numIter = 3
    val rank=50
    val minVal=0.0
    val maxVal=5.0
    val gamma1=0.007
    val gamma2=0.007
    val gamma6=0.005
    val gamma7=0.015

    val storageLevel=StorageLevel.MEMORY_AND_DISK

    var sl:StorageLevel=StorageLevel.MEMORY_ONLY;
    if(storageLevel=="MEMORY_AND_DISK_SER")
      sl=StorageLevel.MEMORY_AND_DISK_SER
    else if(storageLevel=="MEMORY_AND_DISK")
      sl=StorageLevel.MEMORY_AND_DISK

    var conf = new SVDPlusPlus.Conf(rank, numIter, minVal, maxVal, gamma1, gamma2, gamma6, gamma7)
    var edges:  org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Double]]= null
    val dataset="small";

    if(dataset=="small"){
      val graph = GraphLoader.edgeListFile(sc, input, true, minEdge,sl, sl).partitionBy(PartitionStrategy.RandomVertexCut)
      edges=graph.edges.map{ e => {
        var attr=0.0
        if(e.dstId %2 ==1) attr=5.0 else attr=1.0
        Edge(e.srcId,e.dstId,e.attr.toDouble)
      }
      }
      edges.persist()
    }  else if(dataset=="large"){
      edges = sc.textFile(input).map { line =>
        val fields = line.split("::")
        Edge(fields(0).toLong , fields(1).toLong, fields(2).toDouble)

      }
      edges.persist()

    }else{
      sc.stop()
      System.exit(1)

    }

    var (newgraph, u) = SVDPlusPlus.run(edges, conf)
    newgraph.persist()

    var tri_size=newgraph.triplets.count() //collect().size

    var err = newgraph.vertices.collect().map{ case (vid, vd) =>
      if (vid % 2 == 1) vd._4 else 0.0
    }.reduce(_ + _) / tri_size

    println("the err is %.2f".format(err))
  }
}
