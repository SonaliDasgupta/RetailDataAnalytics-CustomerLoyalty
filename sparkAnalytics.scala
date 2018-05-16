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

//find status of orders of top 10 : how responsive is store
 val ordersByStatus=ordersByTop10.map(o=>(o(3),1)).reduceByKey(_+_).map(o=>(o._2,o._1)).sortByKey(ascending=false).map(o=>(o._2,o._1))

ordersByStatus.collect().foreach(println)
(COMPLETE,44)
(PENDING_PAYMENT,34)
(PENDING,22)
(CLOSED,19)
(PROCESSING,17)
(ON_HOLD,9)
(SUSPECTED_FRAUD,7)
(CANCELED,2)

//oldest order yet pending payment for loyalty of frequent customers
val oldestOrderPending=ordersByTop10.filter(o=>(o(3)=="PENDING_PAYMENT")).map(o=>(o(1),o)).sortByKey(ascending=true).first()
oldestOrderPending: (String, Array[String]) = (2013-08-17 00:00:00.0,Array(3870, 2013-08-17 00:00:00.0, 12431, PENDING_PAYMENT))

val orderIdsList=ordersByTop10.map(o=>o(0).toInt).collect().toList
val orderItems=sc.textFile("order_items")
val orderItemsInTop10=orderItems.map(oi=>oi.split(",")).filter(oi=>orderIdsList.contains(oi(1).toInt))
//order items in orders of top 10 most frequent buyers, which means they liked these
//later, find most frrequently bought items and do comparitive analysis
orderItemsInTop10.count()
res12: Long = 372 

val orderItemsTotalCost=orderItemsInTop10.map(o=>o(4).toDouble).sum
orderItemsTotalCost: Double = 74264.38999999998 

//most frequent 10 customers account for this much

//tot ammount for all customers
val allOrderItemsCost=orderItems.map(oi=>oi.split(",")).map(oi=>oi(4).toDouble).sum
allOrderItemsCost: Double = 3.4322619930019915E7 

val percentForTop10=orderItemsTotalCost/allOrderItemsCost
percentForTop10: Double = 0.002163715653158675
//top 10 account for very small percentage in large store

//now get product information of most frequently ordered items
val mostFreqOrderItems=orderItems.map(oi=>oi.split(",")).map(oi=>(oi(2).toInt,1)).reduceByKey(_+_).map(oi=>(oi._2,oi._1)).sortByKey(ascending=false).top(10).map(oi=>(oi._2,oi._1))
mostFreqOrderItems: Array[(Int, Int)] = Array((365,24515), (403,22246), (502,21035), (1014,19298), (1004,17325), (1073,15500), (957,13729), (191,12169), (627,10617), (116,328))
//most frequent product IDs bought
val mostFreqProducts=mostFreqOrderItems.map(oi=>oi._1).toList
mostFreqProducts: List[Int] = List(365, 403, 502, 1014, 1004, 1073, 957, 191, 627, 116)

val products=sc.textFile("products")

val top10Products=products.map(p=>p.split(",")).filter(p=>mostFreqProducts.contains(p(0).toInt)).map(p=>(p(0).toInt,p(1).toInt,p(2),p(3)))
 top10Products.collect().foreach(println)
(116,6,Nike Men's Comfort 2 Slide,)
(191,9,Nike Men's Free 5.0+ Running Shoe,)
(365,17,Perfect Fitness Perfect Rip Deck,)
(403,18,Nike Men's CJ Elite 2 TD Football Cleat,)
(502,24,Nike Men's Dri-FIT Victory Golf Polo,)
(627,29,Under Armour Girls' Toddler Spine Surge Runni,)
(957,43,Diamondback Women's Serene Classic Comfort Bi,)
(1004,45,Field & Stream Sportsman 16 Gun Fire Safe,)
(1014,46,O'Brien Men's Neoprene Life Vest,)
(1073,48,Pelican Sunstream 100 Kayak,)

//how many of these top 10 products were ordered by top 10 frequent customers, is it 100% ?
val prodIdsForTop10Cust=orderItemsInTop10.map(oi=>oi(2).toInt)

val prodFreqList= prodIdsForTop10Cust.collect().toList
val top10ProdsInFreqCust=top10Products.filter(t=>prodFreqList.contains(t._1.toInt))
top10ProdsInFreqCust.count
res17: Long = 10 
//100% top 10 products are preferred by all top 10 customers in their orders so highly correlated

//now find most popular categories shopped
val categories=sc.textFile("categories")
categories.count
res19: Long = 58

val category_info=categories.map(c=>(c(0).toInt,c(1).toInt,c(2).toString))

val top10ProdCategories=top10Products.map(p=>p._2)
val top10Cat=top10ProdCategories.collect().toList
top10Cat: List[Int] = List(6, 9, 17, 18, 24, 29, 43, 45, 46, 48)

val top10CatInfo=category_info.filter(c=>top10Cat.contains(c._1))
//0??












 














