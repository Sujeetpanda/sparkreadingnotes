package com.v2maestros.spark.bda.train;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.v2maestros.spark.bda.common.SparkConnection;

import scala.collection.Seq;

public class SparkSQLDemo3 {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkSession spSession = SparkConnection.getSession();
		JavaSparkContext spContext = SparkConnection.getContext();
		
		
		
		//Create Data Frames from an CSV
		Dataset<Row> personDf = spSession.read()
								.option("header","true")
								.csv("data/Person.txt");
		personDf.show(6);
		
		Dataset<Row> productDf = spSession.read().option("header", "true").csv("data/Product.txt");
		//productDf.show(5);

		Dataset<Row> ordersDf = spSession.read().option("header", "true").csv("data/Orders.txt");
		//ordersDf.show(5);
		
		//1.	Display count of number of persons by gender and city. And display the average, min and max age.
		
		personDf.groupBy(col("Person_gender"),col("Person_City"))
		.count()
		.show();
		System.out.println("----------------------------AGE---------------------------");
		personDf.groupBy(col("Person_gender"),col("Person_City"))
		.agg(avg(personDf.col("Person_age")),max(personDf.col("Person_age")),min(personDf.col("Person_age")))
		.show();
		
		//2.	List products bought by persons with age group 34 to 48 who do not live in city "Chicago" and who live in "New York".
		//And consider only those persons who never bought any product of category "Snacks". 
		//Display products, number of transactions, total number of units and the total amount in $.
		
		
		Dataset<Row> joinDf = personDf.join(ordersDf, personDf.col("Person_id").equalTo(ordersDf.col("Person_id")));
		joinDf.show();
		
		Dataset<Row> finalJoinDf = joinDf.join(productDf, joinDf.col("Product_id").equalTo(productDf.col("Product_id")));
		finalJoinDf.show();
		
		// List products bought by persons with age group 34 to 48 who do not live in city "Chicago" and who live in "New York".
		//And consider only those persons who never bought any product of category "Snacks". 
		Dataset<Row> ProductDetailsDf = finalJoinDf.filter( col("Person_age").between(34, 48).and(col("Person_City").equalTo("New York").notEqual("Chicago").and(col("Product_Category").notEqual("Snacks"))));
		ProductDetailsDf.show();
		System.out.println("----------products--------------");
		ProductDetailsDf.select(col("Product_name")).show();

		System.out.println("Number of transactions:"+ProductDetailsDf.count());
		
		System.out.println("Total number of units:"+ProductDetailsDf.agg(sum(col("Units_Bought"))));
		
		List<Row> mylist = ProductDetailsDf.select(col("Units_Bought")).collectAsList();
		System.out.println(mylist.get(0));
		//System.out.println("The first eelemt is " + new Double(mylist.get(0).get(0).toString()));
		System.out.println(mylist);
		System.out.println(mylist.get(0).getClass());
		//mylist.stream().reduce((x,y) => x+y);
		//System.out.println("Total number of units:"+ProductDetailsDf.agg(sum(col("Units_Bought"))));
		//System.out.println("Total number of units:"+ProductDetailsDf.groupBy(col("Units_Bought")).sum((Seq<String>) col("Units_Bought")));
		//personDf.groupBy(col("Person_age")).sum(colNames)
		
		// 5.	Sort all persons by age. Provide potential challenges and possible solutions.
		personDf.orderBy(col("Person_age")).show();
		
		//System.out.println("Total number of units:"+ProductDetailsDf.groupBy(col("Units_Bought")).sum((new Seq<String>(col("Units_Bought"))));

		
		
		
		//System.out.println("Total number of units:"+ProductDetailsDf.groupby(col("Units_Bought")));
		//df.groupby('steps').sum().show()
		 
		//empDf.groupBy(col("gender")).count().show();
		//Dataset<Row> summaryData = personDf.groupBy(col("deptid"))
			//	.agg(avg(empDf.col("salary")),max(empDf.col("age")));
		
		
		
		
		
		
		
		
		/*
		
		-------------------------------------------------------------------
		 * Working with Data Frames
		 -------------------------------------------------------------------
		//Load a JSON file into a data frame.
		Dataset<Row> empDf = spSession.read().json("data/customerData.json");
		empDf.show();
		empDf.printSchema();
		
		//Do data frame queries
		System.out.println("SELECT Demo :");
		empDf.select(col("name"),col("salary")).show();
		
		System.out.println("FILTER for Age == 40 :");
		empDf.filter(col("age").equalTo(40)).show();
		
		System.out.println("GROUP BY gender and count :");
		empDf.groupBy(col("gender")).count().show();
		
		System.out.println("GROUP BY deptId and find average of salary and max of age :");
		Dataset<Row> summaryData = empDf.groupBy(col("deptid"))
				.agg(avg(empDf.col("salary")),max(empDf.col("age")));
		summaryData.show();
		
		//Create Dataframe from a list of objects
		Department dp1 = new Department("100","Sales");
		Department dp2 = new Department("200", "Engineering");
		
		List<Department> deptList = new ArrayList<Department>();
		deptList.add(dp1);
		deptList.add(dp2);

		Dataset<Row> deptDf = spSession.
				createDataFrame(deptList, Department.class);
		System.out.println("Contents of Department DF : ");
		deptDf.show();
		
		System.out.println("JOIN example :");
		Dataset<Row> joinDf = empDf.join(deptDf, col("deptid").equalTo(col("id")));
		
		System.out.println("Cascading operations example : ");
		empDf.filter( col("age").gt(30) )

		//Create Data Frames from an CSV
		Dataset<Row> autoDf = spSession.read()
						.option("header","true")
						.csv("data/auto-data.csv");
		autoDf.show(5);
		
		//Create dataframe from Row objects and RDD
		Row iRow = RowFactory.create(1,"USA");
		Row iRow2 = RowFactory.create(2,"India");
		
		List<Row> rList = new ArrayList<Row>();
		rList.add(iRow);
		rList.add(iRow2);
		
		JavaRDD<Row> rowRDD = spContext.parallelize(rList);
		
		StructType schema = DataTypes
			.createStructType(new StructField[] {
				DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("name", DataTypes.StringType, false) });
		
		Dataset<Row> tempDf = spSession.createDataFrame(rowRDD, schema);
		tempDf.show();
		
		-------------------------------------------------------------------
		 * Working with Temp tables
		 -------------------------------------------------------------------
		autoDf.createOrReplaceTempView("autos");
		System.out.println("Temp tables Demo : ");
		spSession.sql("select * from autos where hp > 200").show();
		spSession.sql("select make, max(rpm) from autos group by make order by 2").show();
		
		//Convert DataFrame to JavaRDD
		JavaRDD<Row> autoRDD = autoDf.rdd().toJavaRDD();
		
		//Working with Databases
		Map<String,String> jdbcOptions = new HashMap<String,String>();
		jdbcOptions.put("url", "jdbc:mysql://localhost:3306/demo");
		//put the mysql jdbc driver into spark/lib folder so that it recognises it 
		jdbcOptions.put("driver", "com.mysql.jdbc.Driver");
		jdbcOptions.put("dbtable", "demotable");
		jdbcOptions.put("user", "root");
		jdbcOptions.put("password", "");
		
		System.out.println("Create Dataframe from a DB Table");
		Dataset<Row> demoDf = spSession.read().format("jdbc")
				.options(jdbcOptions).load();
		demoDf.show();
		
		ExerciseUtils.hold();
*/
	}

}



Collapse 
Message Input


Message Arun Rajshekhar
