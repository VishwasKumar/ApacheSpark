package com.sparkdevelopment.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class DataGenerator {

    public static void main(String[] args) {

        System.out.println("*********************************************");
        System.out.println("* The part 1 of assignment#1 in B649 class  *");
        System.out.println("* PageRank input Data generator             *");
        System.out.println("*********************************************");

        int numUrls = Integer.parseInt("5000");
        int numGroups = Integer.parseInt("1");
        String fileNameBase = "resources/pageRankInput";
        try{
            for (int index=0; index<numGroups; index++){

                String fileName = fileNameBase + "."+index;
                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
                String strLine = null;
                int i, j;

			/*
			 * simulate the power law distributions.
			 */

                StringBuffer strBuf = null;
                for (i = 0; i < numUrls; i++) {
                    strBuf = new StringBuffer();
                    for (j = 0; j < numUrls; j++) {
                        if ((-3*i*i + 7 + i) % (j + 3) == 0)
                            strBuf.append(i).append(" ").append(j).append("\n");
                    }// for j;
                    writer.write(strBuf.toString());
                    System.out.print(strBuf.toString());
                }//for i
                writer.flush();
                writer.close();
            }//for index
        } catch (IOException e) {
            e.printStackTrace();
        }
    }//main
}
