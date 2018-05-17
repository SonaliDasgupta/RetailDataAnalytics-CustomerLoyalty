sqoop eval --connect jdbc:mysql://quickstart.cloudera --username root --password cloudera --query "show databases";

sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password cloudera  --target-dir customer_orders_data --query 'select order_id,customer_id from orders join customers ON (orders.order_customer_id=customers.customer_id) WHERE $CONDITIONS' --split-by customer_id;

sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password cloudera --table customers --target-dir customers

sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password cloudera --table orders --target-dir orders

sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password cloudera --table order_items --target-dir order_items

sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password cloudera --table products --target-dir products

sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password cloudera --table categories --target-dir categories

sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password cloudera --table departments --target-dir departments
