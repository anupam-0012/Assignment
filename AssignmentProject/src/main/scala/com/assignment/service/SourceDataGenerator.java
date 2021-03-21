package com.assignment.service;

/**
 * @author Anupam.Patel
 *
 */
public class SourceDataGenerator {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//For Transaction Data
		/*
		 *int rows = 10000000;
		  int intNum =1;

		for (int i = 1; i <= 20; i++) {
			CSVWriter writer = new CSVWriter(new FileWriter("C:\\temp\\transaction"+i+".csv"));
			String headers[] = {"tx_id", "vendor_id", "amount"};
			writer.writeNext(headers);
			rows=10000000*i;
			for(int j=intNum;j<=rows;j++) {
				String line1[] = {j+"", ""+rand.nextInt(10), ""+j};
				 writer.writeNext(line1);
			}
			intNum=intNum+10000000;
			writer.flush();
			writer.close();
		}*/

		/*
		 *int rows = 10000000;
		  int intNum =1;

		for (int i = 1; i <= 20; i++) {
			CSVWriter writer = new CSVWriter(new FileWriter("C:\\temp\\transaction"+i+".csv"));
			String headers[] = {"tx_id", "vendor_id", "amount"};
			writer.writeNext(headers);
			rows=10000000*i;
			for(int j=intNum;j<=rows;j++) {
				String line1[] = {j+"", ""+rand.nextInt(1000000), ""+j};
				 writer.writeNext(line1);
			}
			intNum=intNum+10000000;
			writer.flush();
			writer.close();
		}*/

		//geneator for vendor data
		/*
		 *int rows = 100000;
		  int intNum =1;

		for (int i = 1; i <= 10; i++) {
			CSVWriter writer = new CSVWriter(new FileWriter("C:\\temp\\transaction"+i+".csv"));
			String headers[] = {"vendor_id", "vendor_name", "vendor_type_code"};
			writer.writeNext(headers);
			rows=100000*i;
			for(int j=intNum;j<=rows;j++) {
				String line1[] = {j+"", "name_"+j, ""+rand.nextInt(1000)};
				 writer.writeNext(line1);
			}
			intNum=intNum+100000;
			writer.flush();
			writer.close();
		}*/

		//geneator for vendor type
		/*
		 *
        CSVWriter writer = new CSVWriter(new FileWriter("C:\\temp\\transaction"+i+".csv"));
		String headers[] = {"vendor_id", "vendor_name", "vendor_type_code"};
	        for (int i = 1; i <= 1000; i++) {
						String line1[] = {j+"", "vendor_name_"+j, "description"+j};
						 writer.writeNext(line1);
					}
		*/


	}

}
