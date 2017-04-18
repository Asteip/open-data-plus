package com.alma.opendata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

import scala.collection.immutable.TreeSet

/**
  * Web Data Commons Analyze built with Apache Spark
  */
  object NQuadsSearch {
  /**
    * Parse a NQuad in String format and get it's context
    * @param nquad The string to parse
    * @return
    */
    def getContext(nquad : String) : String = {
      val begin = nquad.lastIndexOf("<")
      val end = nquad.lastIndexOf(">")
      printl("nquad : "+ nquad)
      try{
      	nquad.substring(begin + 1, end) 
      }
      catch{
	case e:IndexOutOfBoundsException => {
	    println("line ignore. : " + nquad)
	    e.toString()
	}
      }
    }

    def getNode(nquad : String) : String = {
      /* Deprecated
      * val begin = nquad.firstIndexOf(":")
      * val end = nquad.fisrtIndexOf("<")
      * val test = nquad.split("<")*/
      nquad.split(" <")(0)
    }

  /**
    * Run stage one
    * @param files
    * @param sc
    */
    def runStageOne(files: String, sc : SparkContext) : Unit = {

      val dataFile : RDD[String] = sc.textFile(files)
      dataFile.filter(nquad => nquad.contains("Nantes") | nquad.contains("postal-code> \"44000") | nquad.contains("postal-code> \"44100") | nquad.contains("postal-code> \"44200") | nquad.contains("postal-code> \"44300") | nquad.contains("postal-code> \"44301"))
      .map(getContext)
      .distinct()
      .saveAsTextFile("resultsPhase1")
    }

  /**
    * Run stage two
    * @param files
    * @param stageOneFiles
    * @param sc
    */

    def runStageTwo(files: String, stageOneFiles: String, sc : SparkContext) : Unit = {


      val dataFile : RDD[String] = sc.textFile(files)
      val stageOne  = sc.broadcast(TreeSet[String]() ++ sc.textFile(stageOneFiles).collect().toSet)

      dataFile.filter(nquad => {
        stageOne.value.contains(getContext(nquad))
        }).saveAsTextFile("resultsPhase2")
    }


  /**
    * Run concat
    * @bref Concatener tous les n-quads ayant le meme contexte
    * @param files
    * @param stageOneFiles
    * @param sc
    */

    //var init = "";
    var tat = "";
    var total =""; //Pour chaque contexte
    //var total2 = ""; //tous les resultats
    val total3 = ArrayBuffer[String]();

    def concat(files: String, stageOneFiles: String, sc : SparkContext) : Unit = {
      val dataFile : RDD[String] = sc.textFile(files);
      val stageOne  = sc.broadcast(TreeSet[String]() ++ sc.textFile(stageOneFiles).collect().toSet);
      

      dataFile.filter(nquad => {
        stageOne.value.contains(getContext(nquad))
        }).foreach(a => {
          if(getContext(a) == tat){
            total = total + a;
            //total2 = total2 +a;
            } else{
              //total.saveAsTextFile("resultsPhase2")
              total3 += total;
             // println("--- Nouvelle entrée ---");
             // println(total);
              tat = getContext(a);
              total = "";
              //total2 = total2 + "\n";
              total = a;
              //total2 = total2 + a;
            }
            
          }          
          )
        //println("--- total ---");
        //println(total);
        total3 += total;

        val distData = sc.parallelize(total3);
        distData.saveAsTextFile("resultsPhase2");

        //total2.saveAsTextFile("resultsPhase2");
        //val dataFile2 : RDD[String] = sc.textFile(total2);
        //dataFile2.saveAsTextFile("resultsPhase2");
      }

      def decisionTree(stageOneFiles: String, sc : SparkContext) : Unit = {

        //val dataFile : RDD[String] = sc.textFile(files)
        //val stageOne  = sc.broadcast(TreeSet[String]() ++ sc.textFile(stageOneFiles).collect().toSet)
        val stageOne : RDD[String] = sc.textFile(stageOneFiles)

/*
Ville and CodePostal and Country and Region     //sur 100%

(Ville and CodePostal and Country) or         //sur 99%
  (Ville and CodePostal and Region) or
  (Ville and Country and Region) or
  (CodePostal and Country and Region)

(Ville and CodePostal) or             //sur 80%
  (Ville and Region) or             //sur 80%
  (Ville and Country) or              //sur 100%
  (CodePostal and Country) or           //sur 100%
  (CodePostal and Region)             //sur 80%

Ville or CodePostal or Region or Country      //sur 30%
*/

        stageOne.foreach(nquad => {
            if(nquad.contains("Nantes") & nquad.contains("France") & (nquad.contains("postal-code> \"44000") | nquad.contains("postal-code> \"44100") | nquad.contains("postal-code> \"44200") | nquad.contains("postal-code> \"44300") | nquad.contains("postal-code> \"44301"))){
              //println("--- Contiens nantes + 44000 + france ---");
              //println(nquad);
              total3 += nquad;
            }
            else if(nquad.contains("Nantes") & nquad.contains("France")){
              //println("--- Contiens nantes + france ---");
              //println(nquad);
              total3 += nquad;
            }
            else if(nquad.contains("Nantes") & (nquad.contains("postal-code> \"44000") | nquad.contains("postal-code> \"44100") | nquad.contains("postal-code> \"44200") | nquad.contains("postal-code> \"44300") | nquad.contains("postal-code> \"44301"))){
              //println("--- Contiens nantes + 44XXX ---");
              //println(nquad);
              total3 += nquad;
            }
            else if(nquad.contains("France") & (nquad.contains("postal-code> \"44000") | nquad.contains("postal-code> \"44100") | nquad.contains("postal-code> \"44200") | nquad.contains("postal-code> \"44300") | nquad.contains("postal-code> \"44301"))){
              //println("--- Contiens france + 44XXX ---");
              //println(nquad);
              total3 += nquad;
            }
            
            else if(nquad.contains("Nantes") & nquad.contains("Pays de la loire")){
              //println("--- Contiens nantes + loire ---");
              //println(nquad);
              total3 += nquad;
            }

          })
        val distData = sc.parallelize(total3);
        distData.saveAsTextFile("resultsArbre");
      }

  /**
    * Execute the main program
    * @param args
    */
    def main(args: Array[String]) : Unit = {
      val conf: SparkConf = new SparkConf()
      .setAppName("NQuads Search")
      val sc = new SparkContext(conf)

      args(0) match {
        case "1" => runStageOne(args(1), sc)
        case "2" => {
          runStageTwo(args(1), args(2), sc)
          //concat(args(1), args(2), sc)
        }
        case "3" => {
          decisionTree(args(1), sc)
        }
        //  2 ./../dl ./resultsPhase1/* */
        case default => println("Pas de phase : " + args(0))
      }
    }

  }
