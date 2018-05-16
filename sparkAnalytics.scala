val customerData=sc.textFile("customer_orders_data")
customerData.count();
res0: Long = 68883

val order_customer=customerData.map(c=>c.split(","))

val perCustomerCount=order_customer.map(c=> (c(1).toInt, 1)).reduceByKey(_+_)

perCustomerCount.count()
res16: Long = 12405


val top10Cust=perCustomerCount.map(c=>(c._2,c._1)).sortByKey(ascending=false).top(10).map(c=>(c._2,c._1)) 
//top 10 customers with max orders, next: do on order price

val customer_info=sc.textFile("customers")
val customerinfo=customer_info.map(c=>c.split(","))
val top10CustIds=top10Cust.map(c=>(c._1))
top10CustIds: Array[Int] = Array(12431, 6316, 5897, 569, 12284, 5654, 5624, 5283, 4320, 221)
val top10CustInfo=customerinfo.filter(c=>( top10CustIds.toList.contains(c(0).toInt)))
top10CustInfo.count()
res9: Long = 10
 
//get cities and states of top 10
val top10CityState=top10CustInfo.map(c=>(c(6),c(7)))
top10CityState.collect().foreach(println)
(Columbus,OH)
(Meridian,ID)
(Caguas,PR)
(Escondido,CA)
(Caguas,PR)
(Caguas,PR)
(Newburgh,NY)
(Caguas,PR)
(New Haven,CT)
(Kaneohe,HI)

//get most bought items of top 10 customers

//store orders which were made by the most frequent 10
val order_info=sc.textFile("orders")
val ordersByTop10=order_info.map(o=>o.split(",")).filter(o=>(top10CustIds.toList.contains(o(2).toInt)))
ordersByTop10.count
res25: Long = 154 
//154 orders total by top 10 customers in dataset

//find items , prices total and products














