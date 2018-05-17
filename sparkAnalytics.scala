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

val category_info=categories.map(c=>(c.split(","))).map(c=>(c(0).toInt,c(1).toInt,c(2).toString))

val top10ProdCategories=top10Products.map(p=>p._2)
val top10Cat=top10ProdCategories.collect().toList
top10Cat: List[Int] = List(6, 9, 17, 18, 24, 29, 43, 45, 46, 48)

val top10CatInfo=category_info.filter(c=>top10Cat.contains(c._1))
top10CatInfo.collect().foreach(println)
(6,2,Tennis & Racquet)                                                          
(9,3,Cardio Equipment)
(17,4,Cleats)
(18,4,Men's Footwear)
(24,5,Women's Apparel)
(29,5,Shop By Sport)
(43,7,Camping & Hiking)
(45,7,Fishing)
(46,7,Indoor/Outdoor Games)
(48,7,Water Sports)
//most preferred categories

val departments=sc.textFile("departments")
val dept=departments.map(d=>d.split(",")).map(d=>(d(0).toInt,d(1).toString))
val top10ProdsDeptId=top10CatInfo.map(c=>c._2).collect().distinct.toList
top10ProdsDeptId: List[Int] = List(2, 3, 4, 5, 7)
//most popular departments
val topDepts=dept.filter(d=>top10ProdsDeptId.contains(d._1))
topDepts.collect().foreach(println)
(2,Fitness)                                                                     
(3,Footwear)
(4,Apparel)
(5,Golf)
(7,Fan Shop)
//most popular departments though not necessarily in that order

//calculate total sales for each category and department
//PERFORM JOIN ON order_items vs products vc categories vs departments
//ONLY FOR ALREADY COMPLETED AND CLOSED ORDERS 
val orderInfo=order_info.map(o=>o.split(",")).map(o=>(o(0).toInt,o(3).toString))
val ordersDone=orderInfo.filter(o=>(List("COMPLETE","CLOSED").contains(o._2)))
 ordersDone.count
res43: Long = 30455  
//processing required for these many orders
 val ordersDoneIds=ordersDone.map(o=>o._1).collect().toList
 val orderItemsDone=orderItems.map(oi=>oi.split(",")).map(oi=>(oi(0).toInt,oi(1).toInt,oi(2).toInt,oi(3).toInt,oi(4).toDouble, oi(5).toDouble))
 val orderItems_done=orderItemsDone.filter(oi=>ordersDoneIds.contains(oi._2))

 val productsInfo=products.map(p=>p.split(",")).map(p=>(p(0).toInt,p))
 
 val orderItemsDone=orderItems_done.map(oi=>(oi._3,oi))

 val productsDone=productsInfo.join(orderItemsDone).map(t=>(t._2._1(0),t._2._1(1),t._2._1(2),t._2._2._5)).distinct

productsDone.take(5).foreach(println)
(295,38,Fitbit The One Wireless Activity & Sleep Trac,199.9)                    
(273,13,Under Armour Kids' Mercenary Slide,139.95)
(565,26,adidas Youth Germany Black/Red Away Match Soc,210.0)
(835,37,Bridgestone e6 Straight Distance NFL Carolina,95.97)
(775,35,Clicgear 8.0 Shoe Brush,29.97)

//top 5 products with maximum price
val top5=productsDone.map(p=>(p._4,p)).distinct.sortByKey(ascending=false).top(5).map(p=>p._2)

top5.foreach(println)
(208,10,SOLE E35 Elliptical,1999.99)
(60,4,SOLE E25 Elliptical,999.99)
(860,38,Bushnell Pro X7 Jolt Slope Rangefinder,599.99)
(226,11,Bowflex SelectTech 1090 Dumbbells,599.99)
(724,33,LIJA Women's Mid-Length Panel Golf Shorts,500.0)

//join productsDone with categories
val prodCat=productsDone.map(p=>(p._2.toInt,p))
val cat=category_info.map(c=>(c._1,c))
val prod_Cat=cat.join(prodCat)
val prodCatInfo=prod_Cat.map(p=>(p._2._2._3,p._1,p._2._1(2),p._2._2._4))
//productName, categoryId,categoryName,amountSpent

//categories on which maximum amount is spent

val catSpent=prodCatInfo.map(p=>(p._4,p)).sortByKey(ascending=false).map(p=>(p._2._3,(p._1,p._2._1)))

val catAgg=catSpent.reduceByKey((c1,c2)=>(c1._1+c2._1, ""))
val catAgg1=catAgg.map(c=>(c._2._1,c._1)).sortByKey(ascending=false)
catAgg1.top(5).foreach(println)
(6493.35,Electronics)                                                           
(4658.01,Kids' Golf Clubs)
(3783.0,Golf Shoes)
(3599.2499999999995,Boxing & MMA)
(3054.37,Golf Gloves)
//category aggregates done

//now rank the departments as per sales
val deptInfo=dept.map(d=>(d._1.toInt,d))
val catAgg2=catAgg1.map(c=>(c._2,c._1))
val categories=sc.textFile("categories")
val cat=categories.map(c=>c.split(","))
val cat1=cat.map(c=>(c(2),c(1)))
val joinCat=cat1.join(catAgg2)
val deptCat=joinCat.map(t=>(t._2._1.toInt,t._2._2))
val dept_info=deptInfo.map(d=>(d._1.toInt,d._2._2))
val deptSales=dept_info.join(deptCat)

//order the sales
val orderedSales=deptSales.reduceByKey((t1,t2)=>(t1._1,(t1._2+t2._2))).map(t=>(t._2._2,t._2._1)).sortByKey(ascending=false)

orderedSales.collect.foreach(println)
(28120.949999999997,Outdoors)
(17256.120000000003,Footwear)
(8094.040000000001,Fitness)
(6723.790000000001,Golf)
(4084.48,Fan Shop)
(3578.9300000000003,Apparel)

//while finding total revenue don't forget to reduceByKey !!






















 














