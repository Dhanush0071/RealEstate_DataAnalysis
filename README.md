                                                                             Real Estate Data Analysis and Visualization


This project involves comprehensive data analysis and visualization of real estate data using various technologies and tools.

1. OVERVIEW :
===============================
The dataset used in this project is called "Makaan Dataset," which comprises four primary tables:

makaan_estate.tsv
builder.csv
Sub_urban.csv
locality.csv

2. SETUP
================================
-->Environment and Software Used
	Software Development Kit: Amazon Corretto 11
	Integrated Development Environment: IntelliJ IDEA
	Database: MySQL
	Other Tools: Apache Spark SQL, Python

-->Steps to Set Up

	1.MySQL Setup:
	------------
	Install MySQL locally and configure it using MySQL Workbench.
	Create a new database and four tables (makaan_estate, builder_details, suburban_details, locality_details) in your local host with a USERNAME and PASSWORD

	2.IntelliJ Configuration:
	-------------------------
	Configure IntelliJ to use the Amazon Corretto 11 SDK.
	Connect MySQL in IntelliJ using JDBC to establish a connection between the database and the Spark SQL pipeline use the USERNAME AND PASSWORD that you used to create your database
	example:
		spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/<your_database_name>").option("driver", "com.mysql.jdbc.Driver")
      		.option("dbtable", <table_name>).option("user", <your_user_name>).option("password", <your_password>).load()
		

	3.Dependencies:
	---------------
	Add the following dependencies to your IntelliJ project:
		scala:
		Copy the below lines into your build.sbt 
		libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
		libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.0"
		libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
		libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"

		Python Setup:
		Install required Python dependencies (streamlit, sqlalchemy, matplotlib, pandas).
		pip install streamlit
		pip install sqlalchemy
		pip install pandas
		pip intsall matplotlib

3. Analysis and Visualization
================================
----: Utilized Apache Spark SQL for data analysis, with the results stored in the MySQL database.
----: Employed Python and its libraries (matplotlib, pandas, streamlit) for visualizing the analyzed data.
----: To run the visualization, execute the command:
	streamlit run visualization.py    //type this in terminal

4. Project Structure
================================
-->	src/: Contains the source code files used for data analysis in IntelliJ using Apache Spark SQL.
	visualization.py: Python script for visualizing the analyzed data using Streamlit and other libraries.
5. Usage
================================
--> Ensure all dependencies are installed.
--> Run the analysis in IntelliJ to populate the MySQL database.
--> Execute streamlit run visualization.py to visualize the analyzed data through the Streamlit interface.

5. CONTRIBUTORS
================================
Dhanush Sanapala       [CB.EN.U4AIE21109]
Chinta Subha Srikar    [CB.EN.U4AIE21107]
Anand Raj Gali         [CB.EN.U4AIE21111]
Sudiksha Kumari        [CB.EN.U4AIE21168]




