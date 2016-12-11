salaries = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/root/assgn5/dept_salary.txt")
mapped_salaries = salaries.map(lambda line: line.split(" "))
grouped_salaries = mapped_salaries.map(lambda fields: (fields[0], int(fields[1])))
aggregated_salaries = grouped_salaries.reduceByKey(lambda x, y: x + y)